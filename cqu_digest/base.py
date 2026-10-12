"""最底层的纯定义：常量 + 数据结构。

★ 这个模块【零依赖】—— 除了标准库什么都不 import。
  放这儿的理由：这些东西被很多模块用，如果它们住在 fetch/engine.py 里，
  那么 config.py / schedule.py 这种最底层的模块，为了拿一个「东八区」常量，
  就得把 httpx 和 bs4 一起拖进来 —— 概念上也不对，时区跟「抓取引擎」没关系。
"""

from datetime import datetime, timedelta, timezone
from typing import TypedDict

CHINA_TZ = timezone(timedelta(hours=8))
"""东八区。日期分界、抓取时间戳、简报落款都用它。"""


class Notice(TypedDict):
    """一条抓来的通知。引擎产出，解析层和渲染层消费。"""

    id: str
    title: str
    link: str
    source: str
    source_key: str
    category: str
    date: str
    published_at: datetime
    body: str
