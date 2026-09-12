from bs4 import Tag


def parse_title_attr(tag: Tag) -> str:
    return (
        str(tag.get("title")) if tag.get("title") else tag.get_text(" ", strip=True)
    ).strip()


def parse_h2_child(tag: Tag) -> str:
    h2 = tag.find("h2")
    if isinstance(h2, Tag):
        return h2.get_text(" ", strip=True)
    return tag.get_text(" ", strip=True)


def parse_text_content(tag: Tag) -> str:
    return tag.get_text(" ", strip=True)


def child_text(selector: str):
    """返回一个「再往里选一层子节点取文本」的解析器。

    列表页的条目标签上常常混着日期、浏览量等杂字，标题单独放在子节点里
    （如 <a><div class="date">2026-09-03</div><div class="tit">标题</div></a>）。
    直接取 a 的文本会把日期一起吞进去，所以要能指定从哪个子节点取。
    """
    def parser(tag: Tag) -> str:
        node = tag.select_one(selector)
        return parse_title_attr(node) if isinstance(node, Tag) else ""

    return parser


def parse_title_with_keyword(tag: Tag, keyword: str = "开发区") -> str:
    """解析标题，仅当标题包含指定关键词时返回标题，否则返回空字符串"""
    title = (
        str(tag.get("title")) if tag.get("title") else tag.get_text(" ", strip=True)
    ).strip()
    if keyword in title:
        return title
    return ""


def filter_title_by_keyword(title: str, keyword: str = "开发区") -> str:
    """根据关键词过滤字符串标题，用于非 HTML 源（如 JSON API）"""
    title = title.strip()
    if keyword in title:
        return title
    return ""
