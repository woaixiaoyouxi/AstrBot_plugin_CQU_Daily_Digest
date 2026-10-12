"""指令实现。分派在 main.py 的 digest_entry 里。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from cqu_digest.fetch.sources import format_source_lines, unconfigured_keys

QUERY_RECENT_DAYS = 3
"""公开查询只看最近几天的通知（没有已读基线，不卡窗口会倒出整站历史）。"""


class CommandsMixin:
    async def _cmd_query(self, event: AstrMessageEvent):
        """公开查询：现算一份给当前会话。

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

        result = await self._run_once(
            dry_run=True,
            recent_days=QUERY_RECENT_DAYS,
            with_group_messages=False,
        )
        if result["skipped"]:
            yield event.plain_result("刚好在生成简报，稍等十几秒再来。")
            return

        if not result["text"]:
            yield event.plain_result("（什么都没抓到，可能是源还没配好）")
            return
        text = result["text"]
        if cooldown > 0:
            self._query_cache[key] = (time.monotonic(), text)
        async for r in self._emit_digest(event, text):
            yield r

    async def _cmd_help(self, event: AstrMessageEvent):
        """查看使用说明。"""
        if self._deny(event):
            return
        yield event.plain_result(self._help_text())

    async def _cmd_now(self, event: AstrMessageEvent):
        """立即跑一次完整流程并推送。"""
        if self._deny(event, admin_only=True):
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
            async for r in self._emit_digest(event, result["text"], note=note):
                yield r
            return
        yield event.plain_result(f"已推送，本次新增 {result['new']} 条。")

    async def _cmd_test(self, event: AstrMessageEvent):
        """只看简报长什么样：抓取+渲染，但不推送、不写去重记录。"""
        if self._deny(event, admin_only=True):
            return
        result = await self._run_once(dry_run=True)
        if result["skipped"]:
            yield event.plain_result("已经有一次运行在进行中，稍等片刻再试。")
            return
        if not result["text"]:
            yield event.plain_result("（没有抓到任何内容，先检查源配置）")
            return
        async for r in self._emit_digest(event, result["text"]):
            yield r

    async def _cmd_last(self, event: AstrMessageEvent):
        """查看最近一次生成的简报原文。"""
        if self._deny(event, admin_only=True):
            return
        text = await self._store.get_last_digest()
        if not text:
            yield event.plain_result("还没有生成过简报。")
            return
        async for r in self._emit_digest(event, text):
            yield r

    async def _cmd_sources(self, event: AstrMessageEvent):
        """列出所有信息源及其配置状态。"""
        if self._deny(event, admin_only=True):
            return
        lines = ["信息源：", *format_source_lines()]
        missing = unconfigured_keys()
        if missing:
            lines += ["", f"⚠️ 还没配 URL/selector：{', '.join(missing)}"]
        yield event.plain_result("\n".join(lines))

    # ── 配置读写 ────────────────────────────────────────────────

    def _help_text(self) -> str:
        return "\n".join(
            [
                "每日简报——使用说明",
                "",
                "任何人可用（限已开放的群）：",
                "- /digest：现算一份发到本群（只含校内通知与竞赛）",
                "- /digest query：同上",
                "- /digest help：查看本说明",
                "",
                "仅管理员可用：",
                "- /digest now：立即跑一次完整流程并推送",
                "- /digest test：看完整简报（含群消息）长什么样，不推送、不写去重",
                "- /digest last：查看最近一次推送的原文",
                "- /digest sources：查看信息源配置状态",
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
