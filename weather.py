"""今日天气，数据源 wttr.in（无需 API key）。

wttr.in 的 j1 接口返回 JSON；带 lang=zh 时描述会额外给 lang_zh 字段。
任何失败都返回空串——天气缺了不该拖垮整份简报。
"""

from typing import Any
from urllib.parse import quote

import httpx

from astrbot.api import logger

WEATHER_URL = "https://wttr.in/{city}?format=j1&lang=zh"


class WeatherClient:
    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}

    async def today(self, city: str) -> str:
        city = (city or "").strip()
        if not city:
            return ""
        url = WEATHER_URL.format(city=quote(city))
        try:
            async with httpx.AsyncClient(timeout=self._timeout()) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
        except Exception as exc:
            logger.info(f"[简报] 天气获取失败 {city}: {exc}")
            return ""

        return self._format(city, data)

    def _format(self, city: str, data: Any) -> str:
        if not isinstance(data, dict):
            return ""
        current_list = data.get("current_condition") or []
        weather_list = data.get("weather") or []
        if not current_list and not weather_list:
            return ""

        parts: list[str] = [f"今日天气（{city}）"]

        if weather_list and isinstance(weather_list[0], dict):
            today = weather_list[0]
            low = today.get("mintempC")
            high = today.get("maxtempC")
            if low is not None and high is not None:
                parts.append(f"{low}~{high}°C")

        if current_list and isinstance(current_list[0], dict):
            current = current_list[0]
            desc = self._desc(current)
            if desc:
                parts.append(desc)
            temp = current.get("temp_C")
            if temp is not None:
                parts.append(f"当前 {temp}°C")
            feels = current.get("FeelsLikeC")
            if feels is not None:
                parts.append(f"体感 {feels}°C")
            humidity = current.get("humidity")
            if humidity is not None:
                parts.append(f"湿度 {humidity}%")

        return "，".join(parts)

    @staticmethod
    def _desc(current: dict[str, Any]) -> str:
        lang = current.get("lang_zh")
        if isinstance(lang, list) and lang and isinstance(lang[0], dict):
            value = lang[0].get("value")
            if value:
                return str(value)
        desc = current.get("weatherDesc")
        if isinstance(desc, list) and desc and isinstance(desc[0], dict):
            value = desc[0].get("value")
            if value:
                return str(value)
        return ""

    def _timeout(self) -> int:
        try:
            return int(self.config.get("request_timeout_seconds", 20))
        except (ValueError, TypeError):
            return 20
