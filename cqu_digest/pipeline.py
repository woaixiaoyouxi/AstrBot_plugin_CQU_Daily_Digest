"""主流水线：抓取 -> 去重 -> 补正文 -> 群消息 -> 交给渲染 -> 记账。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from cqu_digest.fetch.engine import CHINA_TZ, Notice
from cqu_digest.fetch.sources import SOURCES_BY_KEY

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
    ) -> dict[str, Any]:
        """统一返回值形状——调用方固定读这五个键，别各写各的 dict。"""
        return {"text": text, "new": new, "pushed": pushed, "seeded": seeded, "skipped": skipped}

    async def _run_once(
        self,
        *,
        dry_run: bool = False,
        recent_days: int | None = None,
        with_group_messages: bool = True,
    ) -> dict[str, Any]:
        """跑一次完整流程。

        已有一次在跑就直接跳过——两条流水线并发会读到同一份 seen_ids，
        算出同一批「新增」，然后推两遍。
        """
        if self._run_lock.locked():
            logger.info("[简报] 已有一次运行在进行中，本次跳过")
            return self._result(skipped=True)
        async with self._run_lock:
            return await self._run_pipeline(
                dry_run=dry_run,
                recent_days=recent_days,
                with_group_messages=with_group_messages,
            )

    async def _run_pipeline(
        self,
        *,
        dry_run: bool = False,
        recent_days: int | None = None,
        with_group_messages: bool = True,
    ) -> dict[str, Any]:
        """跑一次完整流程。dry_run 不推送、不写 seen_ids（调试源 / 公开查询用）。"""
        categories = self._enabled_categories()
        notices = await self._fetcher.fetch_notices(categories=categories) if categories else []

        seen = await self._store.get_seen_ids()
        # 首次运行：不把整站历史一次性推给用户，只记基线
        seeded = not dry_run and not seen and not await self._store.get_last_run_date()

        if dry_run or seeded:
            fresh = list(notices)
        else:
            fresh = [item for item in notices if item["id"] not in seen]

        if recent_days:
            fresh = self._within_days(fresh, recent_days)

        await self._fill_bodies(fresh)
        if not with_group_messages:
            group_messages: list[dict[str, Any]] = []
        else:
            group_messages = (
                await self._collector.peek_recent()
                if dry_run
                else await self._collector.consume_recent()
            )

        text = await self._digester.build(
            self.context,
            date_str=self._today(),
            weather=await self._weather_text(),
            notices_by_category=self._group_by_category(fresh),
            group_messages=group_messages,
        )

        pushed = False
        if not dry_run:
            await self._store.set_last_digest(text)
            if seeded:
                logger.info(f"[简报] 首次运行：记录 {len(notices)} 条为基线，本次不推送")
            else:
                pushed = await self._push(text)
            await self._store.add_seen_ids([item["id"] for item in notices])
            await self._store.set_last_run_date(self._today())

        return self._result(text=text, new=len(fresh), pushed=pushed, seeded=seeded)

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

    async def _weather_text(self) -> str:
        if not self._cfg_bool("weather_enabled", True):
            return ""
        city = str(self.config.get("weather_city", "Chongqing") or "Chongqing")
        return await self._weather.today(city)

    # ── 推送 ────────────────────────────────────────────────────
