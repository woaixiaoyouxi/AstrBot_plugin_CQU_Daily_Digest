"""指令实现。分派在 main.py 的 digest_entry 里。"""
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from cqu_digest.fetch.sources import format_source_lines, unconfigured_keys
from cqu_digest.digest.entries import is_valid, render_from_entries
from cqu_digest.push import ZWSP


class QueryGate(Enum):
    """公开查询的前半段跑完之后，只可能是这三种去向。"""

    STOP = "stop"        # 别继续了（没权限，或队列满被劝退）——该发的提示已经发过
    CACHED = "cached"    # 冷却期内命中缓存，把上次那份发出去就行
    RUN = "run"          # 可以真跑了


@dataclass
class QueryGateResult:
    """_public_query_gate 的返回值。

    用具名 dataclass 而不是元组或者魔法字符串：
    调用方读 gate.text / gate.key 一眼就知道是什么，以后加字段也不会打乱顺序。
    """

    gate: QueryGate
    key: str = ""
    cooldown: int = 0
    note: str = ""
    text: str = ""
    blocks: list | None = None


class CommandsMixin:
    async def _public_query_gate(self, event: AstrMessageEvent,
                                 key_suffix: str = "") -> QueryGateResult:
        """公开查询的前半段：权限 -> 冷却 -> 排队。多个入口共用这一份。

        抽出来的理由：/digest 和 /digest all 这段原来逐字相同、各写各的，
        上次给 all 补冷却就是因为漏改了一处。
        """
        self._prune_query_cache()

        if self._deny(event):
            return QueryGateResult(QueryGate.STOP)

        key = str(getattr(event, "unified_msg_origin", "") or "") + key_suffix
        cooldown = self._cfg_int("query_cooldown_seconds", 120)
        cached = self._query_cache.get(key)
        if cached and cooldown > 0:
            waited = time.monotonic() - cached[0]
            if waited < cooldown:
                note = (f"距上次生成才 {int(waited)} 秒，先看这一份"
                        f"（{cooldown} 秒后可再算）：")
                # ★ 缓存必须连 blocks 一起存：正文里「群消息转发」用的是
                #   {{BLOCK:n}} 占位符，没有 blocks 就展不开 —— 整段会静默消失。
                return QueryGateResult(
                    QueryGate.CACHED, note=note, text=cached[1],
                    blocks=(cached[2] if len(cached) > 2 else None),
                )

        if not await self._ack_and_check_queue(event):
            return QueryGateResult(QueryGate.STOP)
        return QueryGateResult(QueryGate.RUN, key=key, cooldown=cooldown)

    QUERY_CACHE_MAX = 200

    def _prune_query_cache(self) -> None:
        """查询缓存满了就丢掉最老的一半。

        不清理的话它会按会话一直攒 —— 会话数有限所以影响很小，
        但顺手清一下更省心（缓存里是三元的 (时刻, 文本, blocks)）。
        """
        if len(self._query_cache) <= self.QUERY_CACHE_MAX:
            return
        oldest = sorted(self._query_cache.items(), key=lambda kv: kv[1][0])
        for key, _value in oldest[:len(oldest) // 2]:
            self._query_cache.pop(key, None)

    async def _cmd_query(self, event: AstrMessageEvent):
        """公开查询（增量）：有新通知才现算一份，没有就直接回「新增 0 条」。

        走 dry_run——不推送、不写 seen_ids、不消费群缓冲，所以外人刷它不会
        动到他的增量基线。权限、冷却、排队都在 _public_query_gate 里。

        唯一会写 seen_ids 的例外：通知管理员成功之后补写那批 new_ids ——
        因为那条确实送到他手上了，不写的话定时推送会再推一遍。
        """
        gate = await self._public_query_gate(event)
        if gate.gate is QueryGate.STOP:
            return
        if gate.gate is QueryGate.CACHED:
            async for r in self._emit_digest(event, gate.text, note=gate.note,
                                             blocks=gate.blocks):
                yield r
            return

        days = self._cfg_int("query_recent_days", 30)
        result = await self._run_once(
            dry_run=True,
            recent_days=days,
            with_group_messages=True,
            only_new=True,
            keep_entries=True,
        )

        # 增量：一条新的都没有就直接说一声，连生成都省了
        if result["new"] <= 0 or not result["text"]:
            yield event.plain_result("本次没有新通知（新增 0 条）。")
            return

        text = result["text"]
        # 群友这次查到了新通知 -> 也给管理员发一份（提示 + 同一份卡片）
        if await self._notify_admin_new(event, text, result.get("blocks")):
            await self._store.add_seen_ids(result.get("new_ids") or [])

        if gate.cooldown > 0:
            self._query_cache[gate.key] = (time.monotonic(), text, result.get("blocks"))
        async for r in self._emit_digest(event, text, blocks=result.get("blocks")):
            yield r

    async def _ack_and_check_queue(self, event: AstrMessageEvent) -> bool:
        """先回一句「已收到」，顺便报排队位置。返回还能不能继续跑。

        ★ 用 send_message 直接发，不靠 yield —— 不确定 AstrBot 是不是逐条
          发 generator 的 yield；靠 yield 的话提示可能和简报一起出现，那就白做了。
        """
        ahead = self._queue_ahead()
        limit = max(1, self._cfg_int("queue_limit", 3))
        if ahead > limit:
            text = "排队的人太多了（前面 " + str(ahead) + " 个），过一会儿再来吧。"
            go = False
        elif ahead > 0:
            text = ("已收到请求，目前排队在第 " + str(ahead + 1)
                    + " 位（前面还有 " + str(ahead) + " 个在跑），请稍后…")
            go = True
        else:
            text = "已收到请求，正在生成…"
            go = True

        umo = str(getattr(event, "unified_msg_origin", "") or "")
        if umo:
            try:
                await self.context.send_message(
                    umo, MessageChain().message(ZWSP + text + ZWSP)
                )
            except Exception as exc:                        # noqa: BLE001
                logger.warning(f"[简报] 发「已收到」提示失败：{exc}")
        return go

    async def _cmd_help(self, event: AstrMessageEvent):
        """查看使用说明。"""
        if self._deny(event):
            return
        yield event.plain_result(self._help_text())

    async def _cmd_now(self, event: AstrMessageEvent):
        """立即跑一次完整流程并推送。"""
        if self._deny(event, admin_only=True):
            return
        if not await self._ack_and_check_queue(event):
            return

        result = await self._run_once()
        if result["seeded"]:
            yield event.plain_result(
                f"首次运行：已把当前 {result['new']} 条记为基线，本次不推送"
                "（避免一次性刷屏）。\n再触发一次即可看到真正的增量。"
            )
            return
        if not result["pushed"]:
            # ★ pushed=False 有三种成因，原来一律说「没配推送目标」——
            #   后两种情况下用户会去查一个没问题的地方（日志文件默认还是关的）。
            if not self._push_targets():
                note = "简报已生成，但一个推送目标都没配 —— 去插件配置里填 push_targets。"
            elif not self._cfg_bool("push_enabled", True):
                note = "简报已生成，但定时推送的总开关（push_enabled）关着。"
            else:
                why = (getattr(self, "_forward_fail_reason", "")
                       or "没拿到原因，看控制台日志")
                note = ("简报已生成，但发送失败了：" + why
                        + chr(10) + "（这次不记 seen_ids，下一轮会自动重试）")
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
        if not await self._ack_and_check_queue(event):
            return

        # 全程不过库：不看条目库、不写条目库、也不插【即将截止】
        result = await self._run_once(dry_run=True, with_deadline_alert=False)
        if not result["text"]:
            yield event.plain_result("（没有抓到任何内容，先检查源配置）")
            return
        async for r in self._emit_digest(event, result["text"], blocks=result.get("blocks")):
            yield r

    async def _cmd_query_all(self, event: AstrMessageEvent):
        """把库里所有没过去的都发一遍，顺便看看有没有新抓到的。

        缓存键带 :all 后缀 —— 它和 /digest 共用同一套冷却参数、但各存各的那份，
        否则两边会互相覆盖。
        """
        gate = await self._public_query_gate(event, key_suffix=":all")
        if gate.gate is QueryGate.STOP:
            return
        if gate.gate is QueryGate.CACHED:
            async for r in self._emit_digest(event, gate.text, note=gate.note,
                                             blocks=gate.blocks):
                yield r
            return

        today = self._today()
        limit = self._cfg_int("no_due_expire_days", 30)

        # 顺便检测有没有新信息。走 only_new，且不插提醒（提醒下面自己插一次）
        result = await self._run_once(
            dry_run=True,
            only_new=True,
            with_group_messages=True,
            with_deadline_alert=False,
            keep_entries=True,
        )

        # ★ 从库里现读一份，不要自己把 result["text"] 拼回来：
        #   那条路会给新抓的条目配【合成 id】，和库里的【真实 notice id】不是同一个 ——
        #   同一条通知会在输出里出现两遍（实测第 2 次 /digest all 必然如此）。
        #   刚抓到的那些已经 upsert 进去了（keep_entries=True）。
        entries = await self._store.get_entries()
        # 倒序：upsert 是往后追加，所以新的在后面；截断时该保新的而不是旧的。
        merged = [r for r in reversed(list(entries.values())) if is_valid(r, today, limit)]

        # 库大了不能一次全倒出来（节点数会超上限、退回纯文本）
        cap = self._cfg_int("query_all_max_items", 40)
        dropped = 0
        if cap > 0 and len(merged) > cap:
            dropped = len(merged) - cap
            merged = merged[:cap]

        body = render_from_entries(merged)
        if not body:
            yield event.plain_result("条目库还是空的 —— 等下一次推送之后再来。")
            return

        head = self._digester.header(today, await self._weather_text())
        due = await self._due_soon_items(today)
        if due:
            head = head + chr(10) * 2 + self._digester.render_due_soon(due)
        text = head + chr(10) * 2 + body
        if dropped:
            text += (chr(10) * 2 + "（还有 " + str(dropped)
                     + " 条没列出来；调大面板里的 query_all_max_items 可以多看一些）")
        if gate.cooldown > 0:
            self._query_cache[gate.key] = (time.monotonic(), text, result.get("blocks"))
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
        # 全部转小写再找：main.py 的分派是 sub.lower()，所以 /digest RESET 能进来，
        # 但这里原来是区分大小写的，于是落到菜单而不是执行。
        words = [w.lower() for w in str(getattr(event, "message_str", "") or "").split()]
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
                "发指令后先回一句「已收到请求」，跑完再出简报；前面有人在跑就排队。",
                "查询有冷却时间，同一个会话短时间内只会现算一次，期内再来直接",
                "返回上一次的结果。",
                "群友查到新通知时，管理员会同时收到一份（配置项 query_notify_admin）。",
                "简报只发到 push_targets 里列出的会话，不会自己往群里冒泡。",
                "",
                "推送时间与内容开关都在插件配置里改。简报只摘与在校学生",
                "相关的条目（活动竞赛、评奖选课、宿舍食堂等），行政公文由",
                "模型自行略去。",
            ]
        )

