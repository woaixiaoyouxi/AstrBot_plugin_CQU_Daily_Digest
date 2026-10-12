"""群消息采集。

监听器本体在 main.py（装饰器只能挂在 Star 子类的方法上），这里只负责
「收下一条消息 → 写进 KV 环形缓冲」的策略：

    条目   {"gid", "sender", "sender_name", "text", "images", "ts", "mid",
            "reply_to", "is_end"}
            reply_to 是这条引用/回复的消息 id（可能为空）
            is_end  是这条看起来像"结"这类收尾词
            images 是图片 URL 列表（可能为空）
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

from cqu_digest.digest import blocks as block_utils
from cqu_digest.digest.storage import DigestStore

TEXT_LIMIT = 300

DEFAULT_END_EXACT = ("结", "已结", "截止", "已截止", "结束", "已结束",
                     "截止了", "已截止了")
"""没引用别人时，只有【精确等于】这些才算"结"。

可在面板 group_end_keywords 里改 —— 通知群实际怎么写还没实测过，很可能要加词。
刻意收严：正文里出现"截止"太常见了（"报名截止"就四个字），
按包含判定会把整条通知误判成收尾。"""

DEFAULT_END_CONTAINS = ("截止", "结")
"""引用回复时按【包含】判定用的字。可在面板 group_end_contains 里改。"""

DEFAULT_AGGREGATE_MINUTES = 10


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
        if not message_obj:
            return

        gid = str(getattr(message_obj, "group_id", "") or "")
        if not gid:
            return

        images = self._image_urls(message_obj)
        reply_to = self._reply_to(message_obj)
        if not text and not images:
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
                "images": images,
                "reply_to": reply_to,
                "is_end": self._is_end(text, reply_to),
                "ts": int(time.time()),
                "mid": mid,
            }
        )
        await self._store.set_group_buffer(self._trim(buffer))

    @staticmethod
    def _image_urls(message_obj: Any, limit: int = 3) -> list[str]:
        """把消息里的图片 URL 抠出来（只留 http(s) 的）。

        QQ 的图片链接会过期，所以这里【只记链接、不下图】——
        对"每日简报"够用，而且不会把 KV 撑爆。
        """
        out: list[str] = []
        for seg in getattr(message_obj, "message", None) or []:
            if type(seg).__name__ != "Image":
                continue
            for attr in ("url", "file", "path"):
                val = str(getattr(seg, attr, "") or "").strip()
                if val.startswith("http"):
                    out.append(val)
                    break
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _reply_to(message_obj: Any) -> str:
        """这条消息引用/回复的是哪条（QQ 消息 id）。没有就返回空串。"""
        for seg in getattr(message_obj, "message", None) or []:
            if type(seg).__name__ != "Reply":
                continue
            for attr in ("id", "message_id", "target"):
                val = str(getattr(seg, attr, "") or "").strip()
                if val:
                    return val
        return ""

    def _end_exact(self) -> tuple:
        words = self._keywords_of("group_end_keywords")
        return tuple(words) if words else DEFAULT_END_EXACT

    def _end_contains(self) -> tuple:
        words = self._keywords_of("group_end_contains")
        return tuple(words) if words else DEFAULT_END_CONTAINS

    def _keywords_of(self, key: str) -> list:
        raw = self.config.get(key, [])
        if not isinstance(raw, list):
            return []
        return [str(x).strip() for x in raw if str(x).strip()]

    def _is_end(self, text: str, reply_to: str) -> bool:
        """这条看起来像不像"结"（收尾词）。

        两种形态，判据不一样：
          ① 引用回复了别人 + 很短 + 含"截止"/"结" -> 是
             （引用本身是人为动作，而且很短，基本就是收尾）
          ② 没引用 -> 只认精确等于白名单
             （"报名截止"这种片段不会被误杀成收尾）

        词表都从面板读（group_end_keywords / group_end_contains）。
        """
        s = (text or "").strip()
        if not s:
            return False
        if s in self._end_exact():
            return True
        if reply_to and len(s) <= 10:
            return any(word in s for word in self._end_contains())
        return False

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

    async def blocks(self) -> list[dict[str, Any]]:
        """把缓冲里的扁平消息聚成「通知块」，并处理「结」。

        这里只干两件 IO 的事：读群缓冲、读配置。
        分块和「结」的算法在 blocks.py —— 那边是纯函数，能脱离 AstrBot 单独测。

        closed 的块照样返回（调用方自己决定滤不滤）。
        返回 [{"gid","ts","last_ts","items","closed"}]
        """
        buffer = await self.peek_recent()
        if not buffer:
            return []
        gap = max(1, self._aggregate_minutes()) * 60
        grouped = block_utils.group_into_blocks(buffer, gap, self._block_same_sender())
        return block_utils.mark_closed(grouped)

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

    async def consume_blocks(self) -> list[dict[str, Any]]:
        """取走块并从缓冲里删掉（推送用）。和 consume_recent 同构。"""
        snapshot = int(time.time())
        result = await self.blocks()
        buffer = await self._store.get_group_buffer()
        await self._store.set_group_buffer(
            [item for item in buffer if int(item.get("ts", 0)) > snapshot]
        )
        return result

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

    def _block_same_sender(self) -> bool:
        """分块时要不要要求"同一个人发的"。面板：group_block_same_sender。"""
        return bool(self.config.get("group_block_same_sender", True))

    def _aggregate_minutes(self) -> int:
        try:
            return max(1, int(self.config.get("group_aggregate_minutes",
                                              DEFAULT_AGGREGATE_MINUTES)))
        except (ValueError, TypeError):
            return DEFAULT_AGGREGATE_MINUTES

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
