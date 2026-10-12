"""KV 存储封装。

AstrBot 的 get_kv_data/put_kv_data 是插件作用域的，落 SQLite，
这里只做键名统一、类型兜底和容量裁剪。
"""

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

GetKV = Callable[[str, object], Awaitable[object]]
PutKV = Callable[[str, object], Awaitable[None]]

KEY_SEEN_IDS = "seen_ids"
KEY_LAST_RUN_DATE = "last_run_date"
KEY_LAST_DIGEST = "digest_last"
KEY_GROUP_BUFFER = "group_buffer"
KEY_LAST_PUBLIC = "digest_last_public"
"""公开口径的最近一份简报（不含群消息）。"""

KEY_ENTRIES = "entries"
"""条目库：{id: {cat, title, fields, times, link, published, due, first_seen, last_seen}}

存的是简报解析出来的结构化条目。

★ published（发布时间）在【第一次入库时定死】，之后不再更新 ——
  网页那边的 published_at 在解析不出日期时会退化成"抓取时刻"，
  每抓一次刷新一次，直接拿它做"30 天无截止销毁"会永远销毁不掉。
"""

SEEN_IDS_CAP = 800
ENTRIES_CAP = 400


def shift_date(date_str: str, days: int) -> str:
    """「2026-10-12」+ days 天 -> 「2026-10-05」（days 为负就是往前挪）。

    认不出来就原样返回（调用方按"不过期"处理，宁可多留）。
    """
    try:
        y, m, d = (int(x) for x in str(date_str).split("-"))
        return (datetime(y, m, d) + timedelta(days=days)).strftime("%Y-%m-%d")
    except Exception:                                       # noqa: BLE001
        return str(date_str)


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

    async def clear_seen_ids(self) -> None:
        await self._put(KEY_SEEN_IDS, [])

    # ── 上次运行日期（启动补偿用）──────────────────────────────

    async def get_last_run_date(self) -> str:
        value = await self._get(KEY_LAST_RUN_DATE, "")
        return str(value) if value else ""

    async def set_last_run_date(self, date_str: str) -> None:
        await self._put(KEY_LAST_RUN_DATE, date_str)

    # ── 最近一次简报（调试用）──────────────────────────────────

    async def get_last_public_digest(self) -> str:
        """公开口径的最近一份（不含群消息）。"""
        value = await self._get(KEY_LAST_PUBLIC, "")
        return str(value) if value else ""

    async def set_last_public_digest(self, text: str) -> None:
        await self._put(KEY_LAST_PUBLIC, text)

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

    async def clear_group_buffer(self) -> None:
        await self._put(KEY_GROUP_BUFFER, [])

    # ── 条目库 ──────────────────────────────────────────────────

    async def get_entries(self) -> dict[str, dict[str, Any]]:
        raw = await self._get(KEY_ENTRIES, {})
        if not isinstance(raw, dict):
            return {}
        return {str(k): v for k, v in raw.items() if isinstance(v, dict)}

    async def set_entries(self, entries: dict[str, dict[str, Any]]) -> None:
        """写库，顺带按入库时间裁到上限（丢最早的）。"""
        items = sorted(entries.items(), key=lambda kv: str(kv[1].get("first_seen", "")))
        if len(items) > ENTRIES_CAP:
            items = items[-ENTRIES_CAP:]
        await self._put(KEY_ENTRIES, dict(items))

    async def upsert_entries(self, records: list[dict[str, Any]], today: str) -> int:
        """把解析出来的条目写进库，返回【新入库】的条数。

        published 只在新入库时写，已存在的一律保留旧值（见 KEY_ENTRIES 的说明）。
        """
        entries = await self.get_entries()
        added = 0
        for rec in records:
            if not isinstance(rec, dict):
                continue
            rid = str(rec.get("id", "") or "").strip()
            if not rid:
                continue
            merged = dict(rec)
            old = entries.get(rid)
            if old:
                merged["published"] = old.get("published") or rec.get("published") or today
                merged["first_seen"] = old.get("first_seen") or today
            else:
                merged["published"] = rec.get("published") or today
                merged["first_seen"] = today
                added += 1
            merged["last_seen"] = today
            entries[rid] = merged
        if records:
            await self.set_entries(entries)
        return added

    async def prune_entries(self, today: str, no_due_expire_days: int = 30,
                            grace_days: int = 7) -> int:
        """清掉过期太久的条目，返回清掉的条数。

        两条线：
          · 有截止日的：截止日过了 grace_days 天以上 -> 清
          · 没截止日的：入库超过 no_due_expire_days 天 -> 清（无截止销毁）
        """
        entries = await self.get_entries()
        due_floor = shift_date(today, -grace_days)
        no_due_floor = shift_date(today, -no_due_expire_days)
        kept: dict[str, dict[str, Any]] = {}
        for rid, rec in entries.items():
            due = str(rec.get("due", "") or "")
            if due:
                if due >= due_floor:
                    kept[rid] = rec
                continue
            seen = str(rec.get("first_seen", "") or "")
            if not seen or seen >= no_due_floor:
                kept[rid] = rec
        removed = len(entries) - len(kept)
        if removed:
            await self.set_entries(kept)
        return removed

    async def clear_entries(self) -> None:
        await self._put(KEY_ENTRIES, {})

    # ── 内部 ────────────────────────────────────────────────────

    async def _as_list(self, key: str) -> list[str]:
        raw = await self._get(key, [])
        if not isinstance(raw, list):
            return []
        return [str(item) for item in raw]
