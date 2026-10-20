"""定时调度与启动补推。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from cqu_digest.base import CHINA_TZ

class ScheduleMixin:
    async def _should_catch_up(self) -> bool:
        """今天已过推送点、但今天还没跑过 → 需要补一次（asyncio 重启即断的兜底）。"""
        if not self._cfg_bool("catch_up_on_start", True):
            return False
        now = datetime.now(CHINA_TZ)
        if now < self._due_time(now):
            return False
        return await self._store.get_last_run_date() != self._today(now)

    async def _catch_up(self):
        try:
            await self._run_once()
        except Exception as exc:
            logger.error(f"[简报] 启动补偿失败：{exc}")

    async def _scheduler_loop(self):
        while not self._stop_event.is_set():
            delay = self._seconds_until_next_push()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                return  # 收到停止信号
            except asyncio.TimeoutError:
                pass
            try:
                await self._run_once()
            except Exception as exc:
                logger.error(f"[简报] 定时任务失败：{exc}")

    def _due_time(self, now: datetime) -> datetime:
        hour, minute = self._push_time()
        return now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def _seconds_until_next_push(self) -> float:
        now = datetime.now(CHINA_TZ)
        target = self._due_time(now)
        if target <= now:
            target += timedelta(days=1)
        return max(1.0, (target - now).total_seconds())

    # ── 采集提醒 ────────────────────────────────────────────────

    def _reminder_time(self) -> tuple[int, int]:
        raw = str(self.config.get("collect_reminder_time", "08:30") or "08:30").strip()
        raw = raw.replace("：", ":").replace("　", "").replace("．", ".")
        head, _, tail = raw.partition(":")
        try:
            hour, minute = int(head), int(tail)
        except ValueError:
            return 8, 30
        if 0 <= hour < 24 and 0 <= minute < 60:
            return hour, minute
        return 8, 30

    def _seconds_until_reminder(self) -> float:
        now = datetime.now(CHINA_TZ)
        hour, minute = self._reminder_time()
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return max(1.0, (target - now).total_seconds())

    async def _reminder_loop(self):
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._seconds_until_reminder()
                )
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self._send_collect_reminder()
            except Exception as exc:                        # noqa: BLE001
                logger.error(f"[简报] 采集提醒发送失败：{exc}")

    async def _send_collect_reminder(self):
        """到点了给管理员发一句「该去采集了」。"""
        if not self._cfg_bool("collect_reminder_enabled", False):
            return
        targets = self._cfg_list("collect_reminder_targets", [])
        if not targets:
            # 没单独配就退到 push_targets 里的私聊会话
            targets = [t for t in self._push_targets() if ":FriendMessage:" in t]
        if not targets:
            logger.warning(
                "[简报] 采集提醒没地方发：collect_reminder_targets 空，"
                "push_targets 里也没有私聊会话"
            )
            return
        text = ("该去采集今天的消息了。\n"
                "把通知转发给我（文字或截图），我录进简报并分发到指定的群。")
        for umo in targets:
            try:
                await self.context.send_message(umo, MessageChain().message(text))
                logger.info(f"[简报] 已发采集提醒到 {umo}")
            except Exception as exc:                        # noqa: BLE001
                logger.warning(f"[简报] 采集提醒发到 {umo} 失败：{exc}")

    # ── 主流水线 ────────────────────────────────────────────────
