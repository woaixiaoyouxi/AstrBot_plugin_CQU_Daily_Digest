"""通知抓取引擎。

源自 DUT_Notices 的 rss_service.py：保留抓取、日期解析、去重、排序、id 生成，
去掉 RSS 输出，并新增正文抓取 fetch_article_text()。
"""

import asyncio
import re
from collections.abc import Iterable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from typing import Any, TypedDict
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup, Tag

from astrbot.api import logger

from sources import SOURCES, SourceConfig

CHINA_TZ = timezone(timedelta(hours=8))

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

DATE_PATTERN = re.compile(
    r"(?P<year>\d{4})\s*(?:年|[-/.])\s*(?P<month>\d{1,2})\s*(?:月|[-/.])\s*(?P<day>\d{1,2})\s*日?"
)

# 正文里要剔除的噪声节点
_NOISE_SELECTORS = ("script", "style", "nav", "header", "footer", "noscript", "iframe")

# 列表页摘要的截断长度
EXCERPT_LIMIT = 500


class Notice(TypedDict):
    id: str
    title: str
    link: str
    source: str
    source_key: str
    category: str
    date: str
    published_at: datetime
    body: str


class NoticeFetcher:
    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}

    # ── 列表抓取 ────────────────────────────────────────────────

    async def fetch_notices(
        self,
        source_keys: set[str] | None = None,
        categories: set[str] | None = None,
    ) -> list[Notice]:
        timeout_sec = self._cfg_int("request_timeout_seconds", 20)
        max_items = self._cfg_int("max_items", 80)

        selected = [
            source
            for source in SOURCES
            if (source_keys is None or source.get("key") in source_keys)
            and (categories is None or source.get("category") in categories)
            and source.get("url")
            and source.get("selector")
        ]
        if not selected:
            logger.info("[简报] 没有可抓取的源（URL/selector 未配置或全被过滤）")
            return []

        async with httpx.AsyncClient(
            timeout=timeout_sec,
            follow_redirects=True,
            headers=DEFAULT_HEADERS,
        ) as client:
            results = await asyncio.gather(
                *(self._fetch_source_notices(client, source) for source in selected),
                return_exceptions=True,
            )

        notices: list[Notice] = []
        for source, result in zip(selected, results):
            if isinstance(result, Exception):
                logger.warning(
                    f"[简报] 抓取来源失败 {source.get('key')} {source.get('url')}: {result}"
                )
                continue
            notices.extend(result)

        deduped: dict[str, Notice] = {item["link"]: item for item in notices}
        ordered = sorted(
            deduped.values(),
            key=lambda item: (item["published_at"], item["source"]),
            reverse=True,
        )
        return ordered[:max_items]

    async def _fetch_source_notices(
        self, client: httpx.AsyncClient, source: SourceConfig
    ) -> list[Notice]:
        page_urls = [str(source["url"]), *source.get("extra_urls", [])]
        notices: list[Notice] = []
        seen_links: set[str] = set()

        for page_url in page_urls:
            response = await client.get(page_url, headers=self._request_headers(source, page_url))
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            tags = soup.select(str(source["selector"]))
            if not tags:
                logger.info(
                    f"[简报] 选择器未命中 {source.get('key')} {page_url} "
                    f"selector={source.get('selector')}"
                )
                continue

            for tag in tags:
                if not isinstance(tag, Tag):
                    continue
                href = (tag.get("href") or "").strip()
                if not href:
                    continue

                title = source["parser"](tag).strip()
                if not title:
                    continue

                base_url = source.get("base_url") or page_url
                full_url = urljoin(base_url, href)
                if full_url in seen_links:
                    continue

                published_at = self._extract_published_at(tag)
                notices.append(
                    {
                        "id": self._make_notice_id(str(source["key"]), full_url),
                        "title": title,
                        "link": full_url,
                        "source": str(source.get("name", "")),
                        "source_key": str(source["key"]),
                        "category": str(source.get("category", "")),
                        "date": published_at.strftime("%Y-%m-%d"),
                        "published_at": published_at,
                        "body": self._extract_excerpt(tag, source),
                    }
                )
                seen_links.add(full_url)

        if not notices:
            logger.info(f"[简报] 来源无有效条目 {source.get('key')} urls={page_urls}")
        return notices

    def _extract_excerpt(self, tag: Tag, source: SourceConfig) -> str:
        """列表页条目自带的摘要，白送的正文——不抓详情页也能有内容。

        详情页抓成功时会被覆盖掉（见 main.py 的 _fill_bodies）。

        摘要在条目里的位置不固定：有的在链接自己肚子里，有的是链接的同级兄弟
        （如 <a class="title">…</a><p class="desc">…</p>）。所以先看自己，
        再看父节点。只回溯一层——再往上就是列表容器了，会把第一条的摘要
        当成每一条的摘要。
        """
        selector = source.get("excerpt_selector")
        if not selector:
            return ""
        node = tag.select_one(str(selector))
        if not isinstance(node, Tag) and isinstance(tag.parent, Tag):
            node = tag.parent.select_one(str(selector))
        if not isinstance(node, Tag):
            return ""
        return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()[:EXCERPT_LIMIT]

    @asynccontextmanager
    async def article_client(self):
        """抓正文用的共享连接池。

        一次流水线最多抓几十篇正文，每篇单开一个 AsyncClient 就等于
        每次重做 TLS 握手、复用不了连接，所以连接池要在外面开一次。
        """
        async with httpx.AsyncClient(
            timeout=self._cfg_int("request_timeout_seconds", 20),
            follow_redirects=True,
            headers=DEFAULT_HEADERS,
        ) as client:
            yield client

    async def fetch_article_text(
        self,
        url: str,
        limit: int = 1500,
        client: httpx.AsyncClient | None = None,
        content_selector: str | None = None,
    ) -> str:
        """抓详情页正文并截断。失败返回空串，绝不抛异常。

        传 client 可复用连接池；不传就单开一个（独立调用时用）。
        content_selector 指定正文容器：给了就只从容器里取，取不到宁可返回空串，
        也绝不退回整页文本——否则抓回来的是全站导航菜单，喂给模型的
        「时间/地点/要求」全成了「搜索 旧版入口 基金会」。
        """
        if not url:
            return ""
        try:
            if client is not None:
                response = await client.get(url)
            else:
                async with self.article_client() as own_client:
                    response = await own_client.get(url)
            return self._article_from(response, limit, content_selector)
        except Exception as exc:  # 降级为「仅标题」，不中断整条流水线
            logger.info(f"[简报] 正文抓取失败，降级为仅标题 {url}: {exc}")
            return ""

    @staticmethod
    def _article_from(
        response: httpx.Response, limit: int, content_selector: str | None = None
    ) -> str:
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        root: Tag | BeautifulSoup = soup
        if content_selector:
            node = soup.select_one(content_selector)
            if not isinstance(node, Tag):
                logger.warning(
                    f"[简报] 正文容器未命中 {response.url} "
                    f"selector={content_selector}，本次不取正文（避免抓到导航菜单）"
                )
                return ""
            root = node

        for selector in _NOISE_SELECTORS:
            for node in root.select(selector):
                node.decompose()
        text = re.sub(r"\s+", " ", root.get_text(" ", strip=True)).strip()
        return text[:limit]

    # ── 日期 ────────────────────────────────────────────────────

    def _extract_published_at(self, tag: Tag) -> datetime:
        candidates: Iterable[str] = (
            tag.get_text(" ", strip=True),
            *self._iter_ancestor_texts(tag, depth=3),
            self._collect_sibling_text(tag),
        )
        for text in candidates:
            parsed = self._parse_date(text)
            if parsed is not None:
                return parsed
        return datetime.now(CHINA_TZ)

    def _iter_ancestor_texts(self, tag: Tag, depth: int) -> Iterable[str]:
        current = tag.parent
        steps = 0
        while isinstance(current, Tag) and steps < depth:
            text = current.get_text(" ", strip=True)
            if text:
                yield text
            current = current.parent
            steps += 1

    def _collect_sibling_text(self, tag: Tag) -> str:
        texts: list[str] = []
        for sibling in list(tag.previous_siblings)[:2]:
            texts.append(self._node_text(sibling))
        for sibling in list(tag.next_siblings)[:2]:
            texts.append(self._node_text(sibling))
        return " ".join(text for text in texts if text)

    def _node_text(self, node: object) -> str:
        if isinstance(node, Tag):
            return node.get_text(" ", strip=True)
        return str(node).strip()

    def _parse_date(self, text: str) -> datetime | None:
        if not text:
            return None
        match = DATE_PATTERN.search(text)
        if not match:
            return None
        try:
            return datetime(
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
                tzinfo=CHINA_TZ,
            )
        except ValueError:
            return None

    # ── 杂项 ────────────────────────────────────────────────────

    def _make_notice_id(self, source_key: str, link: str) -> str:
        digest = sha1(f"{source_key}|{link}".encode("utf-8")).hexdigest()
        return f"{source_key}:{digest}"

    def _request_headers(self, source: SourceConfig, request_url: str | None = None) -> dict[str, str]:
        headers = dict(DEFAULT_HEADERS)
        headers["Referer"] = str(source.get("base_url") or request_url or source.get("url") or "")
        return headers

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            return int(self.config.get(key, default))
        except (ValueError, TypeError):
            return default
