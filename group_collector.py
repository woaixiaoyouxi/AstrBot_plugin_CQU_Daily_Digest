"""群消息采集。

监听器本体在 main.py（装饰器只能挂在 Star 子类的方法上），这里只负责
「收下一条消息 → 写进 KV 环形缓冲」的策略：

    条目   {"gid", "sender", "sender_name", "text", "ts", "mid"}
    去重   按 mid（同一条消息重复回调不重复入队）
    截断   text 超 300 字直接砍（缓冲是给 LLM 看的，不求完整）
    裁剪   先丢 ts 早于 now-hours 的，若仍超 N 条再留最新的 N 条

两层过滤，都可留空：
    group_allow_from  群白名单——留空 = 机器人所在的群全抓，填了只收名单里的群
    group_keywords    内容关键词——留空 = 全要，填了只留含任一关键词的消息
"""

import time
from typing import Any

from astrbot.api import logger

from storage import DigestStore

TEXT_LIMIT = 300


class GroupCollector:
    def __init__(self, store: DigestStore, config: dict[str, Any] | None = None):
        self._store = store
        self.config = config or {}

    # ── 写入（由 main.py 的监听器调用）──────────────────────────

    async def record(self, event: Any) -> None:
        if not self._enabled():
            return

        message_obj = getattr(event, "message_obj", None)
        text = str(getattr(event, "message_str", "") or "").strip()
        if not message_obj or not text:
            return

        gid = str(getattr(message_obj, "group_id", "") or "")
        if not gid:
            return

        if not self._passes_group(gid):
            return

        if not self._passes_keywords(text):
            return

        mid = str(getattr(message_obj, "message_id", "") or "")
        buffer = await self._store.get_group_buffer()

        if mid and any(item.get("mid") == mid for item in buffer):
            return

        buffer.append(
            {
                "gid": gid,
                "sender": str(self._sender_id(message_obj)),
                "sender_name": str(self._sender_name(message_obj)),
                "text": text[:TEXT_LIMIT],
                "ts": int(time.time()),
                "mid": mid,
            }
        )
        await self._store.set_group_buffer(self._trim(buffer))

    # ── 读取（供简报组装）──────────────────────────────────────

    async def peek_recent(self) -> list[dict[str, Any]]:
        """取窗口内的群消息，顺便清掉过期项，但不消费。调试用。"""
        buffer = await self._store.get_group_buffer()
        if not buffer:
            return []

        cutoff = int(time.time()) - self._hours() * 3600
        recent = [item for item in buffer if int(item.get("ts", 0)) >= cutoff]

        if len(recent) != len(buffer):
            await self._store.set_group_buffer(recent)
            logger.info(f"[简报] 群缓冲清理 {len(buffer) - len(recent)} 条过期消息")
        return recent

    async def consume_recent(self) -> list[dict[str, Any]]:
        """取窗口内的群消息并从缓冲里删掉，避免下一天重复喂给 LLM。

        只删这次真正读到的那批——抓取期间新到的消息不受影响。
        """
        snapshot = int(time.time())
        recent = await self.peek_recent()
        if not recent:
            return []

        buffer = await self._store.get_group_buffer()
        await self._store.set_group_buffer(
            [item for item in buffer if int(item.get("ts", 0)) > snapshot]
        )
        return recent

    # ── 内部 ────────────────────────────────────────────────────

    def _trim(self, buffer: list[dict[str, Any]]) -> list[dict[str, Any]]:
        cutoff = int(time.time()) - self._hours() * 3600
        buffer = [item for item in buffer if int(item.get("ts", 0)) >= cutoff]
        return buffer[-self._max_buffer():]

    def _enabled(self) -> bool:
        return bool(self.config.get("group_capture_enabled", True))

    def _passes_group(self, gid: str) -> bool:
        """群白名单。留空 = 全抓（跟旧行为一致），填了则只收名单里的群。"""
        raw = self.config.get("group_allow_from", [])
        if not isinstance(raw, list):
            return True
        allowed = [str(item).strip() for item in raw if str(item).strip()]
        if not allowed:
            return True
        return gid in allowed

    def _passes_keywords(self, text: str) -> bool:
        raw = self.config.get("group_keywords", [])
        if not isinstance(raw, list):
            return True
        keywords = [str(item).strip() for item in raw if str(item).strip()]
        if not keywords:
            return True
        return any(keyword in text for keyword in keywords)

    @staticmethod
    def _sender_id(message_obj: Any) -> str:
        sender = getattr(message_obj, "sender", None)
        return str(getattr(sender, "user_id", "") or "")

    @staticmethod
    def _sender_name(message_obj: Any) -> str:
        sender = getattr(message_obj, "sender", None)
        return str(getattr(sender, "nickname", "") or "")

    def _hours(self) -> int:
        try:
            return max(1, int(self.config.get("group_capture_hours", 24)))
        except (ValueError, TypeError):
            return 24

    def _max_buffer(self) -> int:
        try:
            return max(1, int(self.config.get("group_max_buffer", 500)))
        except (ValueError, TypeError):
            return 500
