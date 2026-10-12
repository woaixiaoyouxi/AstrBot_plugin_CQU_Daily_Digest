"""配置读取。AstrBot 按分组存配置，所以这里带一个摊平函数。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from cqu_digest.fetch.engine import CHINA_TZ

def _flatten_config(config: Any) -> dict[str, Any]:
    """把分组配置摊平一层，让平铺读法能读到值。

    AstrBot 按 _conf_schema.json 里的「组」把配置存成 {组名: {键: 值}}，
    但本插件各处都按平铺键读 config.get("key")——不摊平就永远读到 None，
    再静默回落到默认值。command_allow_from 的默认值恰好是空列表（谁都不能用），
    所以指令会被一声不吭地全部吞掉。
    """
    if not isinstance(config, dict):
        return {}
    flat: dict[str, Any] = {}
    for key, value in config.items():
        if isinstance(value, dict):
            flat.update(value)
        else:
            flat[key] = value
    return flat

class ConfigMixin:
    def _cfg_bool(self, key: str, default: bool) -> bool:
        value = self.config.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            return int(self.config.get(key, default))
        except (ValueError, TypeError):
            return default

    def _cfg_list(self, key: str, default: list[str]) -> list[str]:
        """读字符串列表配置；类型不对就当空列表，别让脏配置炸掉整条流水线。"""
        value = self.config.get(key, default)
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip()]

    def _push_time(self) -> tuple[int, int]:
        raw = str(self.config.get("push_time", "09:00") or "09:00")
        head, _, tail = raw.partition(":")
        try:
            hour, minute = int(head), int(tail)
        except ValueError:
            return 9, 0
        if 0 <= hour < 24 and 0 <= minute < 60:
            return hour, minute
        return 9, 0

    def _enabled_categories(self) -> set[str]:
        categories: set[str] = set()
        if self._cfg_bool("enable_campus", True):
            categories.add("campus")
        if self._cfg_bool("enable_competition", True):
            categories.add("competition")
        return categories

    def _push_targets(self) -> list[str]:
        raw = self.config.get("push_targets", [])
        if not isinstance(raw, list):
            return []
        return [str(item) for item in raw if str(item).strip()]

    @staticmethod
    def _today(now: datetime | None = None) -> str:
        return (now or datetime.now(CHINA_TZ)).strftime("%Y-%m-%d")
