"""主流水线：抓取 -> 去重 -> 补正文 -> 群消息 -> 交给渲染 -> 记账。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from cqu_digest.base import CHINA_TZ, Notice
from cqu_digest.digest import blocks as block_utils
from cqu_digest.fetch.sources import SOURCES_BY_KEY
from cqu_digest.digest.entries import days_between, split_entries, to_records

BODY_FETCH_CONCURRENCY = 5


class PipelineMixin:
    @staticmethod
    def _result(
        *,
        text: str = "",
        new: int = 0,
        pushed: bool = False,
        seeded: bool = False,
        skipped: bool = False,
        blocks: list | None = None,
        new_ids: list | None = None,
    ) -> dict[str, Any]:
        """统一返回值形状——调用方固定读这几个键，别各写各的 dict。"""
        return {"text": text, "new": new, "pushed": pushed, "seeded": seeded,
                "skipped": skipped, "blocks": blocks or [],
                "new_ids": new_ids or []}

    def _queue_ahead(self) -> int:
        """我前面还有几个在跑/在等（0 = 轮子空着，直接能跑）。"""
        return max(0, int(getattr(self, "_waiting", 0)))

    async def _run_once(
        self,
        *,
        dry_run: bool = False,
        recent_days: int | None = None,
        with_group_messages: bool = True,
        only_new: bool = False,
        with_deadline_alert: bool = True,
        keep_entries: bool = False,
    ) -> dict[str, Any]:
        """跑一次完整流程。前面有一次在跑就【排队等】，不直接拒绝。

        （以前是直接跳过：两条流水线并发会读到同一份 seen_ids、算出同一批
          「新增」然后推两遍。现在改成串行排队，靠 _run_lock 保证只跑一个；
          前面有几个人用 _waiting 报给调用方。）
        """
        self._waiting += 1
        try:
            async with self._run_lock:
                return await self._run_pipeline(
                    dry_run=dry_run,
                    recent_days=recent_days,
                    with_group_messages=with_group_messages,
                    only_new=only_new,
                    with_deadline_alert=with_deadline_alert,
                    keep_entries=keep_entries,
                )
        finally:
            self._waiting -= 1

    async def _run_pipeline(
        self,
        *,
        dry_run: bool = False,
        recent_days: int | None = None,
        with_group_messages: bool = True,
        only_new: bool = False,
        with_deadline_alert: bool = True,
        keep_entries: bool = False,
    ) -> dict[str, Any]:
        """跑一次完整流程。dry_run 不推送、不写 seen_ids（调试源 / 公开查询用）。

        only_new：只取不在 seen_ids 里的（增量）。查询走这条——
        有新才值得生成，没有的话调用方直接回一句「新增 0 条」就行。

        with_deadline_alert：要不要插【即将截止】那一段。
        test 传 False（它要全程不过库，而提醒是读库来的）。

        keep_entries：dry_run 也【写条目库】。查询（/digest、/digest all）走这条 ——
        它确实「见过」这些通知，存下来 /digest all 才更全。
        ★ 这条路径【本身】不写 seen_ids：群友查一次就记成「已推送」的话，
          以后真推送会把那条当旧的跳过 —— 你反而收不到它。
          例外：/digest 通知管理员成功之后，调用方会自己补写一次
          （那条确实送到他手上了，不写的话定时推送会再推一遍）。
        test 不传这个（它要全程不过库）。
        """
        categories = self._enabled_categories()
        notices = await self._fetcher.fetch_notices(categories=categories) if categories else []

        seen = await self._store.get_seen_ids()
        # 这一批是「检测到的新通知」，跟 fresh 不同：fresh 在全量模式下是全部
        new_ids = [item["id"] for item in notices if item["id"] not in seen]

        # 首次运行：不把整站历史一次性推给用户，只记基线
        seeded = not dry_run and not seen and not await self._store.get_last_run_date()

        if only_new:
            fresh = [item for item in notices if item["id"] not in seen]
        elif dry_run or seeded:
            fresh = list(notices)
        else:
            fresh = [item for item in notices if item["id"] not in seen]

        if recent_days:
            fresh = self._within_days(fresh, recent_days)

        await self._fill_bodies(fresh)
        if not with_group_messages or not self._cfg_bool("enable_group_forward", True):
            group_messages: list[dict[str, Any]] = []
        else:
            raw_blocks = (
                await self._collector.blocks()
                if dry_run
                else await self._collector.consume_blocks()
            )
            # 被"结"关掉的块不进简报
            group_messages = [b for b in raw_blocks if not b.get("closed")]

        text = await self._digester.build(
            self.context,
            date_str=self._today(),
            weather=await self._weather_text(),
            notices_by_category=self._group_by_category(fresh),
            group_messages=group_messages,
        )

        # 解析入库。只有【真跑】才写库 —— 库的语义是"已推送给他的账本"，
        # 所以 /digest 和 test（都是 dry_run）一个字都不写。
        if (not dry_run or keep_entries) and text:
            parsed = split_entries(text)
            if parsed:
                added = await self._store.upsert_entries(
                    to_records(parsed, notices, self._today()), self._today()
                )
                if added:
                    logger.info(f"[简报] 条目库新增 {added} 条")
            removed = await self._store.prune_entries(
                self._today(), self._cfg_int("no_due_expire_days", 30)
            )
            if removed:
                logger.info(f"[简报] 条目库清理 {removed} 条过期记录")

        # 群消息的「块号：N」换成占位符。放在入库之后 —— 入库时要靠那行配对。
        if group_messages:
            text = block_utils.stamp_block_refs(text, group_messages)

        # 到期提醒：从库里挑"已推送过 + 快到期"的，插在头部之后、第一个分类之前。
        # 放在入库之后 —— 它读的就是刚写进去的那张表。
        due_items = (
            await self._due_soon_items(self._today())
            if with_deadline_alert else []
        )
        if due_items:
            text = self._digester.insert_section(
                text, self._digester.render_due_soon(due_items)
            )
            logger.info(f"[简报] 即将截止 {len(due_items)} 条")

        pushed = False
        if not dry_run:
            await self._store.set_last_digest(text)
            if seeded:
                logger.info(f"[简报] 首次运行：记录 {len(notices)} 条为基线，本次不推送")
            else:
                pushed = await self._push(text, group_messages)
            await self._store.add_seen_ids([item["id"] for item in notices])
            await self._store.set_last_run_date(self._today())

        return self._result(text=text, new=len(fresh), pushed=pushed, seeded=seeded,
                              blocks=group_messages,
                              new_ids=new_ids)

    async def _fill_bodies(self, notices: list[Notice]) -> None:
        """按开关给需要正文的条目补抓详情页；单条失败降级为仅标题。"""
        targets = [item for item in notices if self._wants_body(item["category"])]
        if not targets:
            return

        semaphore = asyncio.Semaphore(BODY_FETCH_CONCURRENCY)

        async with self._fetcher.article_client() as client:

            async def fill(item: Notice):
                async with semaphore:
                    source = SOURCES_BY_KEY.get(item["source_key"], {})
                    text = await self._fetcher.fetch_article_text(
                        item["link"],
                        client=client,
                        content_selector=source.get("article_selector"),
                    )
                    # 抓空了不要覆盖列表页自带的摘要
                    if text:
                        item["body"] = text

            await asyncio.gather(*(fill(item) for item in targets), return_exceptions=True)

    def _wants_body(self, category: str) -> bool:
        if category == "campus":
            return self._cfg_bool("fetch_body_campus", True)
        if category == "competition":
            return self._cfg_bool("fetch_body_competition", False)
        return False

    @staticmethod
    def _within_days(notices: list[Notice], days: int) -> list[Notice]:
        """只留最近 N 天的条目。

        日期没解析出来的条目 published_at 是「抓取时刻」，天然落在窗口内，
        不会因为站点没写日期就被误杀。
        """
        cutoff = datetime.now(CHINA_TZ) - timedelta(days=days)
        return [
            item
            for item in notices
            if item.get("published_at") is None or item["published_at"] >= cutoff
        ]

    @staticmethod
    def _group_by_category(notices: list[Notice]) -> dict[str, list[Notice]]:
        grouped: dict[str, list[Notice]] = {}
        for item in notices:
            grouped.setdefault(item["category"], []).append(item)
        return grouped

    async def _due_soon_items(self, today: str) -> list:
        """从条目库里挑出【即将截止】的条目。

        条件：① 已经推送过（id 在 seen_ids 里）
              ② 0 <= (截止日 - 今天) <= deadline_alert_days（默认 7）
        只认截止日，活动日不算 —— 提醒的是"别错过截止"。
        按剩余天数升序（最急的在前）。
        """
        if not self._cfg_bool("deadline_alert_enabled", True):
            return []
        limit = self._cfg_int("deadline_alert_days", 7)
        seen = await self._store.get_seen_ids()
        entries = await self._store.get_entries()
        items = []
        for rid, rec in entries.items():
            if rid not in seen:
                continue
            due = str(rec.get("due", "") or "")
            if not due:
                continue
            left = days_between(today, due)
            if left is None or left < 0 or left > limit:
                continue
            note = ""
            for slot in rec.get("times") or []:
                if str(slot.get("date", "")) == due:
                    note = str(slot.get("note", "") or "")
                    break
            items.append({
                "title": str(rec.get("title", "") or ""),
                "due": due,
                "note": note,
                "link": str(rec.get("link", "") or ""),
                "days_left": left,
            })
        items.sort(key=lambda x: x["days_left"])
        return items

    async def _weather_text(self) -> str:
        if not self._cfg_bool("weather_enabled", True):
            return ""
        city = str(self.config.get("weather_city", "Chongqing") or "Chongqing")
        return await self._weather.today(city)

    # ── 推送 ────────────────────────────────────────────────────
