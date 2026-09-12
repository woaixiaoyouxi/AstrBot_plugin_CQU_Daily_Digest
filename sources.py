"""重庆大学信息源声明。

加一个源 = 加一个 dict。字段说明：

    key               唯一标识，用于去重（改了 key 会导致该源已推条目重推）
    name              显示名，会出现在简报里
    url               列表页 URL
    selector          CSS 选择器，命中列表里的每条条目（通常是 <a>）
    parser            从选中标签里抠标题的函数，见 parsers.py
    category          分类：campus（校内通知）/ competition（竞赛）
    base_url          相对链接补全用；缺省用 url
    extra_urls        可选，同一来源的额外列表页
    excerpt_selector  可选，条目里自带摘要的节点（如 <p class="desc">）。
                      列表页白送的正文，不抓详情页也有内容可喂给模型。
                      先在本条目里找，找不到再往上找一层父节点（摘要常是链接的兄弟）。
    article_selector  可选，详情页正文容器的选择器。配了它，抓正文时就只从
                      这个容器里取；取不到宁可空手而归，也不退回整页——整页
                      抓回来的是全站导航菜单，跟正文毫无关系。
"""

from collections.abc import Callable
from typing import TypedDict

from bs4 import Tag

from parsers import child_text, parse_text_content, parse_title_attr

Parser = Callable[[Tag], str]

CATEGORY_TITLES: dict[str, str] = {
    "campus": "校内通知",
    "competition": "竞赛",
}


class SourceConfig(TypedDict, total=False):
    key: str
    name: str
    url: str
    selector: str
    parser: Parser
    category: str
    base_url: str
    extra_urls: list[str]
    excerpt_selector: str
    article_selector: str


SOURCES: list[SourceConfig] = [
    # ── 校内通知 ────────────────────────────────────────────────
    # 条目形如：
    #   <ul class="jl-list1"><li>
    #     <a href="info/1631/68481.htm" class="con">
    #       <div class="date"><span>2026-09-03</span>…浏览数…</div>
    #       <div class="tit" title="…">重庆大学交通车时刻表</div>
    #     </a>
    #   </li></ul>
    # 日期和标题都在 a 的子节点里，a 自己的文本混着「浏览数」，所以标题要再选一层。
    # 该页 15 条约覆盖 7 周（约 3.5 天一条），单页足够，不配 extra_urls。
    {
        "key": "cqu_www_tzgg",
        "name": "重庆大学 通知公告",
        "url": "https://www.cqu.edu.cn/tzgg.htm",
        "selector": "ul.jl-list1 li a.con",
        "parser": child_text(".tit"),
        "category": "campus",
        "base_url": "https://www.cqu.edu.cn/",
        # 西安博达 CMS 的正文容器，正文（含时间/地点/要求）全在这里面。
        # 不配这个的话整页抓回来的前 1500 字全是导航菜单。
        "article_selector": "div.v_news_content",
    },
    # ── 竞赛 ────────────────────────────────────────────────────
    # 条目形如（Nuxt SSR，列表在服务端就渲染好了）：
    #   <div class="news-item list-item list-item-news"><div class="content">
    #     <div class="top"><span class="time">2026 / 09 / 08</span>…</div>
    #     <a href="/content/news-detail?…" class="title" title="…">…</a>
    #     <p class="desc">根据中国数学会…</p>
    #   </div></div>
    # 标题 a 自身只有标题，日期在同级 div.top 里——靠 fetcher 的祖先回溯拿到。
    # desc 是白送的摘要：竞赛默认不抓详情页，没有它模型就只看得到标题。
    {
        "key": "cqu_sxic_competition",
        "name": "重庆大学学生交叉创新中心 竞赛",
        "url": "https://sxic.cqu.edu.cn/content/news-list?menuIds=68,71&params=39&singlePage=1",
        "selector": "div.news-item a.title",
        "parser": parse_title_attr,
        "category": "competition",
        "base_url": "https://sxic.cqu.edu.cn/",
        "excerpt_selector": "p.desc",
        # 竞赛默认不抓正文，但开关一开就得抓对地方：这里除了标题就剩
        # 正文，没有导航噪声。
        "article_selector": "#content-print",
    },
]

SOURCES_BY_KEY: dict[str, SourceConfig] = {
    str(source["key"]): source for source in SOURCES if source.get("key")
}


def sources_of(categories: set[str] | None = None) -> list[SourceConfig]:
    """按分类筛选源；categories 为 None 时返回全部。"""
    if categories is None:
        return list(SOURCES)
    return [source for source in SOURCES if source.get("category") in categories]


def category_title(category: str) -> str:
    return CATEGORY_TITLES.get(category, category)


def format_source_lines() -> list[str]:
    """给 /digest sources 指令用的一行式列表。"""
    lines: list[str] = []
    for source in SOURCES:
        url = source.get("url") or "（未配置 URL）"
        lines.append(f"- [{category_title(str(source.get('category', '')))}] "
                     f"{source.get('key')}: {source.get('name')} — {url}")
    return lines


def unconfigured_keys() -> list[str]:
    """URL 或 selector 还没填的源——启动时提醒用。"""
    return [
        str(source["key"])
        for source in SOURCES
        if not source.get("url") or not source.get("selector")
    ]
