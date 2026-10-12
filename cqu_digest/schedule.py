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

    # ── 主流水线 ────────────────────────────────────────────────
