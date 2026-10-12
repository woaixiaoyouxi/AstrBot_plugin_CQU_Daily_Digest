"""指令实现。分派在 main.py 的 digest_entry 里。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from cqu_digest.fetch.sources import format_source_lines, unconfigured_keys
from cqu_digest.digest.entries import is_valid, split_entries

QUEUE_LIMIT = 3
"""排队的忍耐上限。超过就劝退——不然连点会把队列堆起来。"""

QUERY_RECENT_DAYS = 3
"""公开查询只看最近几天的通知（没有已读基线，不卡窗口会倒出整站历史）。"""


class CommandsMixin:
    async def _cmd_query(self, event: AstrMessageEvent):
        """公开查询：现算一份给当前会话。

        只查【增量】：跟 seen_ids 比，有新通知才现算一份；
        一条新的都没有就直接回「新增 0 条」，连模型都不调。

        走 dry_run——不推送、不写 seen_ids、不消费群缓冲，所以外人刷它不会
        动到他的增量基线。按 umo 记冷却，冷却期内直接回上一次算好的那份，
        避免有人连点把 API 额度烧穿。
        """
        if self._deny(event):
            return

        key = str(getattr(event, "unified_msg_origin", "") or "")
        cooldown = self._cfg_int("query_cooldown_seconds", 120)
        cached = self._query_cache.get(key)
        if cached and cooldown > 0:
            waited = time.monotonic() - cached[0]
            if waited < cooldown:
                note = (f"距上次生成才 {int(waited)} 秒，先看这一份"
                        f"（{cooldown} 秒后可再算）：")
                async for r in self._emit_digest(event, cached[1], note=note):
                    yield r
                return

        note, go = self._queue_notice()
        if note:
            yield event.plain_result(note)
        if not go:
            return

        days = self._cfg_int("query_recent_days", 30)
        result = await self._run_once(
            dry_run=True,
            recent_days=days,
            with_group_messages=True,
            only_new=True,
            keep_entries=True,
        )
        if result["skipped"]:
            yield event.plain_result("刚好在生成简报，稍等十几秒再来。")
            return

        # 增量：一条新的都没有就直接说一声，连生成都省了
        if result["new"] <= 0 or not result["text"]:
            yield event.plain_result("本次没有新通知（新增 0 条）。")
            return

        text = result["text"]

        # 群友这次查到了新通知 -> 也给管理员发一份（提示 + 同一份卡片）
        if result["new"] > 0:
            ok_admin = await self._notify_admin_new(
                event, text, result.get("blocks"))
            if ok_admin:
                # 确实送到他手上了 -> 记进 seen_ids，免得定时推送再推一遍
                await self._store.add_seen_ids(result.get("new_ids") or [])

        if cooldown > 0:
            self._query_cache[key] = (time.monotonic(), text)
        async for r in self._emit_digest(event, text, blocks=result.get("blocks")):
            yield r

    def _queue_notice(self):
        """返回 (要发给用户的提示, 还能不能继续跑)。没提示就是 ("", True)。"""
        ahead = self._queue_ahead()
        if ahead <= 0:
            return "", True
        if ahead > QUEUE_LIMIT:
            return ("排队的人太多了（前面 " + str(ahead) + " 个），过一会儿再来吧。", False)
        return ("已收到请求，目前排队在第 " + str(ahead + 1) + " 位（前面还有 "
                + str(ahead) + " 个在跑），请稍后…", True)

    async def _cmd_help(self, event: AstrMessageEvent):
        """查看使用说明。"""
        if self._deny(event):
            return
        yield event.plain_result(self._help_text())

    async def _cmd_now(self, event: AstrMessageEvent):
        """立即跑一次完整流程并推送。"""
        if self._deny(event, admin_only=True):
            return
        note, go = self._queue_notice()
        if note:
            yield event.plain_result(note)
        if not go:
            return

        result = await self._run_once()
        if result["skipped"]:
            yield event.plain_result("已经有一次运行在进行中，稍等片刻再试。")
            return
        if result["seeded"]:
            yield event.plain_result(
                f"首次运行：已把当前 {result['new']} 条记为基线，本次不推送"
                "（避免一次性刷屏）。\n再触发一次即可看到真正的增量。"
            )
            return
        if not result["pushed"]:
            note = "简报已生成，但没有推送目标——去插件配置里填 push_targets。"
            async for r in self._emit_digest(event, result["text"], note=note, blocks=result.get("blocks")):
                yield r
            return
        yield event.plain_result(f"已推送，本次新增 {result['new']} 条。")

    async def _cmd_test(self, event: AstrMessageEvent):
        """完整重走一遍：抓网页 + 调模型。全程不过库。

        不推送、不写 seen_ids、不写条目库、不带到期提醒——
        它就是"如果现在从头跑一遍会是什么样"的探针。
        """
        if self._deny(event, admin_only=True):
            return
        note, go = self._queue_notice()
        if note:
            yield event.plain_result(note)
        if not go:
            return

        # 全程不过库：不看条目库、不写条目库、也不插【即将截止】
        result = await self._run_once(dry_run=True, with_deadline_alert=False)
        if result["skipped"]:
            yield event.plain_result("已经有一次运行在进行中，稍等片刻再试。")
            return
        if not result["text"]:
            yield event.plain_result("（没有抓到任何内容，先检查源配置）")
            return
        async for r in self._emit_digest(event, result["text"], blocks=result.get("blocks")):
            yield r

    async def _cmd_query_all(self, event: AstrMessageEvent):
        """把库里所有没过去的都发一遍，顺便看看有没有新抓到的。"""
        if self._deny(event):
            return
        today = self._today()
        entries = await self._store.get_entries()
        limit = self._cfg_int("no_due_expire_days", 30)
        valid = [r for r in entries.values() if is_valid(r, today, limit)]

        note, go = self._queue_notice()
        if note:
            yield event.plain_result(note)
        if not go:
            return

        # 顺便检测有没有新信息。走 only_new，且不插提醒（提醒下面自己插一次）
        result = await self._run_once(
            dry_run=True,
            only_new=True,
            with_group_messages=True,
            with_deadline_alert=False,
            keep_entries=True,
        )
        if result["skipped"]:
            yield event.plain_result("刚好在生成简报，稍等十几秒再来。")
            return

        merged = list(valid)
        if result["new"] > 0 and result["text"]:
            # 新抓的那份解析成记录后并进来，避免出现两个同名分类
            parsed = split_entries(result["text"])
            if parsed:
                merged = merged + self._to_records(parsed, [], today)

        body = self._digester.render_from_entries(merged)
        if not body:
            yield event.plain_result("条目库还是空的 —— 等下一次推送之后再来。")
            return

        head = self._digester.header(today, await self._weather_text())
        due = await self._due_soon_items(today)
        if due:
            head = head + chr(10) * 2 + self._digester.render_due_soon(due)
        text = head + chr(10) * 2 + body
        async for r in self._emit_digest(event, text):
            yield r

    async def _cmd_status(self, event: AstrMessageEvent):
        """看一眼当前状态。只读，什么都不改。"""
        if self._deny(event, admin_only=True):
            return
        today = self._today()
        entries = await self._store.get_entries()
        limit = self._cfg_int("no_due_expire_days", 30)
        valid = [r for r in entries.values() if is_valid(r, today, limit)]
        due = await self._due_soon_items(today)
        seen = await self._store.get_seen_ids()
        buffer = await self._store.get_group_buffer()
        last_run = await self._store.get_last_run_date()
        hour, minute = self._push_time()
        targets = self._push_targets()
        lines = [
            "每日简报 · 当前状态",
            "",
            "条目库：共 " + str(len(entries)) + " 条，其中未过期 " + str(len(valid)) + " 条",
            "即将截止（" + str(self._cfg_int("deadline_alert_days", 7)) + " 天内）："
            + str(len(due)) + " 条",
            "无截止条目的保质期：" + str(limit) + " 天",
            "去重基线：已记 " + str(len(seen)) + " 条",
            "群消息缓冲：" + str(len(buffer)) + " 条（窗口 "
            + str(self._cfg_int("group_capture_hours", 24)) + " 小时）",
            "上次推送日：" + (last_run or "（还没推过）"),
            "下次推送：每天 " + str(hour).zfill(2) + ":" + str(minute).zfill(2),
            "推送目标：" + (str(len(targets)) + " 个" if targets else "（没配！去填 push_targets）"),
            "机器人号：" + (self._self_id() or "（没拿到，建议在配置里填 bot_uin）"),
            "合并转发：" + (
                "开（上限 " + str(self._cfg_int("max_forward_nodes", 60)) + " 节点）"
                if self._cfg_bool("use_forward_msg", True) else "关"
            ),
            "到期提醒：" + (
                "开（提前 " + str(self._cfg_int("deadline_alert_days", 7)) + " 天）"
                if self._cfg_bool("deadline_alert_enabled", True) else "关"
            ),
            "群消息转发：" + (
                "开" if self._cfg_bool("enable_group_forward", True) else "关"
            ),
        ]
        yield event.plain_result(chr(10).join(lines))

    async def _cmd_reset(self, event: AstrMessageEvent):
        """清数据。三个动作各自独立，都要带上词才执行。"""
        if self._deny(event, admin_only=True):
            return
        words = [w for w in str(getattr(event, "message_str", "") or "").split()]
        word = ""
        if "reset" in words:
            index = words.index("reset")
            if len(words) > index + 1:
                word = words[index + 1].lower()
        if word == "seen":
            await self._store.clear_seen_ids()
            yield event.plain_result(
                "已清空【去重基线】。" + chr(10)
                + "下次推送会把窗口内的通知都当成新的，一次性全推一遍 —— "
                "这是正常的，不是坏了。"
            )
            return
        if word == "store":
            count = len(await self._store.get_entries())
            await self._store.clear_entries()
            yield event.plain_result(
                "已清空【条目库】（清掉 " + str(count) + " 条）。" + chr(10)
                + "它会随着后续每次推送慢慢重新积累；去重基线不受影响。"
            )
            return
        if word == "group":
            count = len(await self._store.get_group_buffer())
            await self._store.clear_group_buffer()
            yield event.plain_result(
                "已清空【群消息缓冲】（清掉 " + str(count) + " 条）。" + chr(10)
                + "只影响「还没推出去的那批」，已经推过的简报不受影响。"
            )
            return
        yield event.plain_result(self._reset_menu())

    @staticmethod
    def _reset_menu() -> str:
        return chr(10).join([
            "清数据 —— 三个动作各自独立，请带上要清的那个词：",
            "",
            "/digest reset seen    清【去重基线】",
            "     那是什么：一张「已经推送给你的通知 id」清单，推送时靠它判断哪些是新的。",
            "               它错了 = 推送错了。",
            "     什么时候用：你手动重建过、或发现某条明明没推过却被当成旧的。",
            "     ★ 清完之后第一次推送会比较长（窗口内的会一次性全推）——正常。",
            "",
            "/digest reset store   清【条目库】",
            "     那是什么：消息 + 发布时间 + 截止时间 + 分类 + 内容的那张表。",
            "               /digest all 和到期提醒都读它。",
            "     什么时候用：库里灌进了脏数据（分类错乱、截止日解析得很怪）。",
            "     ★ 清完它会随后续推送慢慢重新积累；不影响「哪些推过了」。",
            "",
            "/digest reset group   清【群消息缓冲】",
            "     那是什么：群消息采到、但还没被某次推送消费掉的那批。",
            "     什么时候用：通知群里刷了一堆无关消息，你不想让它进下一次简报。",
            "     ★ 只影响还没推出去的，已推过的简报不受影响。",
        ])

    def _help_text(self) -> str:
        return chr(10).join(
            [
                "每日简报——使用说明",
                "",
                "任何人可用（限已开放的群）：",
                "- /digest：只查增量——有新通知才生成，没有就回一句新增 0 条",
                "- /digest all：把库里所有没过去的都发一遍（不再调模型）",
                "- /digest help：查看本说明",
                "",
                "仅管理员可用：",
                "- /digest now：立即跑一次完整流程并推送",
                "- /digest test：完整重走一遍（抓网页+调模型），不推送、不写库",
                "- /digest status：看当前状态（库里多少条、下次推送几点、机器人号…）",
                "- /digest reset：清数据（见 /digest reset 单独列出的说明）",
                "",
                "查询有冷却时间，同一个会话短时间内只会现算一次，",
                "期内再来直接返回上一次的结果。简报只发到 push_targets 里列出的",
                "会话，不会自己往群里冒泡。",
                "",
                "推送时间与内容开关都在插件配置里改。简报只摘与在校学生",
                "相关的条目（活动竞赛、评奖选课、宿舍食堂等），行政公文由",
                "模型自行略去。",
            ]
        )

