"""最底层的纯定义：常量 + 数据结构。

★ 这个模块【零依赖】—— 除了标准库什么都不 import。
  放这儿的理由：这些东西被很多模块用，如果它们住在 fetch/engine.py 里，
  那么 config.py / schedule.py 这种最底层的模块，为了拿一个「东八区」常量，
  就得把 httpx 和 bs4 一起拖进来 —— 概念上也不对，时区跟「抓取引擎」没关系。
"""

from datetime import datetime, timedelta, timezone
from typing import TypedDict

def cfg_int(config: dict | None, key: str, default: int) -> int:
    """读整数配置。类型不对、或者范围离谱的，一律回落到默认值。

    ★ 住在 base.py（零依赖）而不是 config.py：Digester 那种不继承 mixin 的类
      也要读配置，如果从 config 拿，就会连带依赖 astrbot —— renderer 的可测性就没了。
      同一个规则也只有这一处实现。
    """
    try:
        value = int((config or {}).get(key, default))
    except (ValueError, TypeError):
        return default
    if value < 0 or value > 100000:
        return default
    return value


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
