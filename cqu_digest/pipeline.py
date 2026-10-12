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
        sent: int = 0,
        blocks: list | None = None,
        new_ids: list | None = None,
    ) -> dict[str, Any]:
        """统一返回值形状——调用方固定读这几个键，别各写各的 dict。"""
        return {"text": text, "new": new, "pushed": pushed, "seeded": seeded,
                "skipped": skipped, "sent": sent, "blocks": blocks or [],
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

        # ★ 先挑出「这次真正会进正文的」，再补正文（少抓几个详情页）。
        #   llm_max_items_per_cat 是喂给模型的单次上限，挑剩下的不会出现在简报里 ——
        #   所以它们也【不能】记进 seen_ids，否则就是「没推给你、又永远不会再出现」。
        model_feed, dropped = self._select_for_model(fresh)
        # new_ids 的语义：这次真正呈现出去的新通知（seeded / 记账 / 通知管理员都用它）
        new_ids = [item["id"] for item in model_feed if item["id"] not in seen]

        await self._fill_bodies(model_feed)
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
            notices_by_category=self._group_by_category(model_feed),
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

        # 被单次上限挤掉的那些，明确说一声：它们在下一轮还会重新进 prompt、不会丢，
        # 但用户有权知道今天看到的不全（query_all_max_items 那条路就是这么做的）。
        if dropped and text:
            text += (chr(10) * 2 + "（另有 " + str(dropped)
                     + " 条这次没排上，下一轮会继续；想一次多看几条就调大面板里的"
                     " llm_max_items_per_cat）")

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
            # ★ 插完再截一次：build() 内部那次截断发生在插入【即将截止】之前，
            #   不管的话最终正文可能超过 max_digest_chars。
            text = self._digester.truncate(text)

        pushed = False
        sent = 0
        if not dry_run:
            # ★ digest_last 是「上次真推出去的原文」——推出去的形态是
            #   占位符已经展开的，所以存的也该是展开后的。不然接上
            #   「看上次简报」会把 {{BLOCK:n}} 直接漏给用户。
            await self._store.set_last_digest(
                block_utils.expand_block_text(text, group_messages)
            )
            if seeded:
                logger.info(f"[简报] 首次运行：记录 {len(notices)} 条为基线，本次不推送")
                # 首次要记【全部】：只记进正文的那部分的话，下次会把剩下的
                # 当成新的、整站历史一次性刷给他。
                await self._store.add_seen_ids([item["id"] for item in notices])
            else:
                sent_count = await self._push(text, group_messages)
                sent = sent_count
                pushed = sent_count > 0
                if pushed:
                    # ★ 只记「真正呈现出去的那批」。
                    #   以前记的是抓到的全量，超上限的那些既没推给他、
                    #   又永远不会再出现在简报里 —— 静默永久丢失。
                    await self._store.add_seen_ids(new_ids)
                else:
                    # ★ 一条都没发出去（没配 push_targets / 开关关着 / 发送失败）
                    #   就不记账，下一轮重试。以前照样记，于是持续静默丢弃。
                    logger.warning(
                        "[简报] 这次一条都没发出去 —— 不记 seen_ids，下一轮重试"
                    )
            await self._store.set_last_run_date(self._today())

        return self._result(text=text, new=len(model_feed), pushed=pushed, seeded=seeded,
                              sent=sent,
                              blocks=group_messages,
                              new_ids=new_ids)

    def _select_for_model(self, notices: list[Notice]) -> tuple[list[Notice], int]:
        """挑出这次要喂给模型的条目，返回 (选中的, 被挤掉的条数)。

        ★ 为什么「挑」必须在这里做、而不是在渲染层做：
          挑剩下的那些【不会进正文】，因此也就不能记进 seen_ids ——
          否则它们既没推给用户、又永远不会再出现在任何一份简报里（静默永久丢失）。
          所以挑选必须发生在调用方能看到结果的地方。

        被挤掉的那些下一轮仍然在「新的」里，会重新进 prompt，不会丢。
        llm_max_items_per_cat 填 0 或负数表示不限。
        """
        limit = self._cfg_int("llm_max_items_per_cat", 8)
        if limit <= 0:
            return list(notices), 0
        per_cat: dict[str, int] = {}
        selected: list[Notice] = []
        dropped = 0
        for item in notices:
            cat = str(item.get("category", "") or "")
            if per_cat.get(cat, 0) < limit:
                per_cat[cat] = per_cat.get(cat, 0) + 1
                selected.append(item)
            else:
                dropped += 1
        return selected, dropped

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

            results = await asyncio.gather(*(fill(item) for item in targets),
                                           return_exceptions=True)
            # ★ return_exceptions=True 会把异常当成【返回值】交回来，不看就丢了。
            #   fetch_article_text 自己的失败它内部记过日志，这里是外层的兜底。
            for item, outcome in zip(targets, results):
                if isinstance(outcome, BaseException):
                    logger.warning(
                        f"[简报] 补正文时出意外（{item.get('link', '')}）："
                        f"{type(outcome).__name__}: {outcome}"
                    )

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
