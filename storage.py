"""KV 存储封装。

AstrBot 的 get_kv_data/put_kv_data 是插件作用域的，落 SQLite，
这里只做键名统一、类型兜底和容量裁剪。
"""

from collections.abc import Awaitable, Callable
from typing import Any

GetKV = Callable[[str, object], Awaitable[object]]
PutKV = Callable[[str, object], Awaitable[None]]

KEY_SEEN_IDS = "seen_ids"
KEY_LAST_RUN_DATE = "last_run_date"
KEY_LAST_DIGEST = "digest_last"
KEY_GROUP_BUFFER = "group_buffer"

SEEN_IDS_CAP = 800


class DigestStore:
    def __init__(self, get_kv_data: GetKV, put_kv_data: PutKV):
        self._get = get_kv_data
        self._put = put_kv_data

    # ── 已推条目去重 ────────────────────────────────────────────

    async def get_seen_ids(self) -> set[str]:
        raw = await self._get(KEY_SEEN_IDS, [])
        if not isinstance(raw, list):
            return set()
        return {str(item) for item in raw}

    async def add_seen_ids(self, ids: list[str], cap: int = SEEN_IDS_CAP) -> None:
        merged = list(dict.fromkeys([*await self._as_list(KEY_SEEN_IDS), *ids]))
        await self._put(KEY_SEEN_IDS, merged[-cap:])

    # ── 上次运行日期（启动补偿用）──────────────────────────────

    async def get_last_run_date(self) -> str:
        value = await self._get(KEY_LAST_RUN_DATE, "")
        return str(value) if value else ""

    async def set_last_run_date(self, date_str: str) -> None:
        await self._put(KEY_LAST_RUN_DATE, date_str)

    # ── 最近一次简报（调试用）──────────────────────────────────

    async def get_last_digest(self) -> str:
        value = await self._get(KEY_LAST_DIGEST, "")
        return str(value) if value else ""

    async def set_last_digest(self, text: str) -> None:
        await self._put(KEY_LAST_DIGEST, text)

    # ── 群消息缓冲 ──────────────────────────────────────────────

    async def get_group_buffer(self) -> list[dict[str, Any]]:
        raw = await self._get(KEY_GROUP_BUFFER, [])
        if not isinstance(raw, list):
            return []
        return [item for item in raw if isinstance(item, dict)]

    async def set_group_buffer(self, buffer: list[dict[str, Any]]) -> None:
        await self._put(KEY_GROUP_BUFFER, buffer)

    # ── 内部 ────────────────────────────────────────────────────

    async def _as_list(self, key: str) -> list[str]:
        raw = await self._get(key, [])
        if not isinstance(raw, list):
            return []
        return [str(item) for item in raw]
