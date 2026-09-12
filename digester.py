"""简报组装：喂给 LLM，渲染成纯文本。

「哪些该摘」是模型判断的，代码不维护关键词表：SYSTEM_PROMPT 里写明了只留
跟在校本科生相关的（活动竞赛、评奖选课、宿舍食堂……），行政公文由模型略去。

三重降级（任一环节失败，简报绝不为空）：
    1. 字段缺失 / LLM 不听话 → prompt 里就要求写死「未提及」
    2. LLM 未配置 / 调用失败 / 超时 → _fallback_render()，只输出标题+日期+链接
       （注意：这条路不走筛选，会把抓到的全列出来。宁可多给，不可漏掉。）
    3. 超过 max_digest_chars → 硬截断

这里不做任何网络请求，只负责拼字符串和调 context.llm_generate。
"""

import asyncio
import re
from typing import Any

from astrbot.api import logger

from fetcher import CHINA_TZ, Notice
from sources import category_title

CATEGORY_ORDER = ("campus", "competition")

SECTION_GAP = "\n\n\n\n"
"""分类之间的空行——换行符+3 个空行。"""

_HEADER_RE = re.compile(r"^【[^】]+】$")
"""分类小标题的识别式，如「【校内通知】」。模型排版不可控，靠这个重新拼。"""

SYSTEM_PROMPT = """你在为一名重庆大学计算机学院的本科生整理「每日简报」。

先做相关性筛选，这一步最重要：
- 只保留跟「在校本科生」沾边的条目。包括但不限于：能报名或参加的活动、
  竞赛、讲座、文体比赛、志愿服务；评奖评优、奖学金助学金；选课、考试、
  学籍、培养方案；住宿、食堂、校园卡、网络、水电、校内交通、医疗、安全
  提醒这类生活后勤。
- 下面这些一律丢掉，一个字都别写进简报：人事任免与教职工招聘、职称评聘、
  招标采购与成交公告、财务审计与预决算、基建修缮、会议纪要、党务与巡视、
  科研项目申报与结题验收、学报与学术委员会事务、校友与基金会、离退休与
  工会事务。
- 拿不准就留下。宁可多留一条，也别漏掉学生真正用得上的。
- 某个分类被筛空，就不要写那个分类的小标题。

硬性要求：
- 只依据用户提供的文本，禁止编造任何信息。原文没写的，一律写「未提及」。
- 完全按给定的分类分组，不要新增、合并或改名分类。
- 每个分类前面单独占一行写分类名，用【】括起来，只能是这三个：
  【校内通知】、【竞赛】、【群消息】。分类之间不要自己加空行——
  空行由程序统一排版，你只管写内容和分类名。
- 全中文，纯文本，不要 Markdown 标记（不要 #、*、-、`）。
- 每行只写一件事，不要展开成长段落。
- 不要写开场白、结语、总结或任何评论。直接输出条目。
- 如果筛完之后一条都不剩，就只输出这一行：今日无与学生相关的新通知。

每个条目的格式（校内通知 / 竞赛），**固定五行，一行都不能少**：
标题
时间：xxx
地点：xxx
要求：xxx
链接：xxx

找不到的字段写「未提及」，但那一行必须照写——不许省略、不许留空、
不许把两行并成一行。哪怕原文完全没提时间地点，也要写
「时间：未提及」「地点：未提及」。

群消息的格式（固定三行）：
群号：xxx　发布者：xxx
内容：xxx（一句话概括，保留原始链接若有）

链接一律照抄，不要改动。"""


class Digester:
    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}

    async def build(
        self,
        context: Any,
        *,
        date_str: str,
        weather: str,
        notices_by_category: dict[str, list[Notice]],
        group_messages: list[dict[str, Any]],
    ) -> str:
        """返回最终要推送的纯文本。"""
        data_block = self._render_data(notices_by_category, group_messages)
        if not data_block:
            return self._header(date_str, weather) + "\n今日无新增通知。"

        prompt = self._build_prompt(date_str, weather, data_block)

        provider_id = self._resolve_provider_id(context)
        if not provider_id:
            logger.warning(
                "[简报] 没有可用的 LLM Provider（llm_provider 未配且全局默认也取不到），"
                "简报降级为纯列表"
            )
            return self._fallback_render(date_str, weather, notices_by_category, group_messages)

        try:
            timeout = self._cfg_int("llm_timeout_seconds", 60)
            response = await asyncio.wait_for(
                context.llm_generate(chat_provider_id=provider_id, prompt=prompt,
                                     system_prompt=SYSTEM_PROMPT),
                timeout=timeout,
            )
        except Exception as exc:
            logger.warning(f"[简报] LLM 调用失败，降级为纯列表：{exc}")
            return self._fallback_render(date_str, weather, notices_by_category, group_messages)

        head = self._header(date_str, weather)
        text = str(getattr(response, "completion_text", "") or "").strip()
        if not text:
            # 调用成功、但模型一条都没留 = 「筛完没剩下」，这是正常结果。
            # 绝不能退回 _fallback_render——那会把刚被筛掉的行政公文原样倒出来。
            logger.info("[简报] 模型没留下任何条目，按「今日无相关通知」处理")
            return self._truncate(f"{head}\n\n今日无与学生相关的新通知。")
        return self._truncate(f"{head}\n\n{self._space_sections(text)}")

    # ── Provider 选择 ───────────────────────────────────────────

    def _resolve_provider_id(self, context: Any) -> str:
        """定出这次调用用哪个 LLM Provider。

        `llm_generate` 的 chat_provider_id 是必填参数（传空会抛
        ProviderNotFoundError），但插件被消息触发时，Provider 是由会话决定的，
        插件不用管。定时任务没有 event、没有会话，只能自己定：

        1. 配置里显式写了 llm_provider → 用它（多 Provider 时用来指定）
        2. 没写 → 用 AstrBot 的全局默认（provider_settings.default_provider_id，
           没设则退回第一个已注册的 Provider）

        所以 llm_provider 是「覆盖项」而非「必填项」，留空也能跑。
        """
        configured = str(self.config.get("llm_provider", "") or "").strip()
        if configured:
            return configured

        getter = getattr(context, "get_using_provider", None)
        if not callable(getter):
            return ""
        try:
            provider = getter()
        except Exception as exc:
            logger.warning(f"[简报] 取全局默认 Provider 失败：{exc}")
            return ""
        config = getattr(provider, "provider_config", None)
        if not isinstance(config, dict):
            return ""
        return str(config.get("id", "") or "").strip()

    # ── 数据段 ──────────────────────────────────────────────────

    def _render_data(
        self,
        notices_by_category: dict[str, list[Notice]],
        group_messages: list[dict[str, Any]],
    ) -> str:
        blocks: list[str] = []
        limit = self._cfg_int("llm_max_items_per_cat", 8)

        for category in CATEGORY_ORDER:
            notices = notices_by_category.get(category) or []
            if not notices:
                continue
            lines = [f"## {category_title(category)}"]
            for index, item in enumerate(notices[:limit], 1):
                body = item.get("body") or "（无正文，仅标题）"
                lines.append(
                    f"{index}. 标题：{item['title']} | 来源：{item['source']} "
                    f"| 链接：{item['link']} | 正文：{body}"
                )
            blocks.append("\n".join(lines))

        if group_messages:
            lines = ["## 群消息"]
            for index, message in enumerate(group_messages[:limit * 2], 1):
                lines.append(
                    f"{index}. 群号：{message.get('gid', '')} "
                    f"| 发布者：{message.get('sender', '')} "
                    f"| 内容：{message.get('text', '')}"
                )
            blocks.append("\n".join(lines))

        return "\n\n".join(blocks)

    def _build_prompt(self, date_str: str, weather: str, data_block: str) -> str:
        head = self._header(date_str, weather)
        return (
            f"{head}\n\n"
            "下面是今天抓到的原始数据，请按系统要求整理成简报正文"
            "（不要重复「每日简报」标题和天气，它们已经在了）：\n\n"
            f"{data_block}"
        )

    # ── 排版 ────────────────────────────────────────────────────

    @staticmethod
    def _space_sections(text: str) -> str:
        """把各分类之间的空行统一成三行。

        模型吐出来的空行数每轮都不一样，指望它稳定排版不现实，所以把正文按
        分类小标题切块，再用固定间距拼回去。找不到小标题（模型没照格式写）
        就原样返回，不做猜测。
        """
        blocks: list[list[str]] = []
        current: list[str] = []
        for line in text.split("\n"):
            if _HEADER_RE.match(line.strip()):
                if current:
                    blocks.append(current)
                current = [line.strip()]
            else:
                current.append(line)
        if current:
            blocks.append(current)

        if len(blocks) <= 1:
            return text.strip()

        rendered = ["\n".join(block).strip("\n") for block in blocks]
        return SECTION_GAP.join(rendered)

    # ── 降级渲染 ────────────────────────────────────────────────

    def _fallback_render(
        self,
        date_str: str,
        weather: str,
        notices_by_category: dict[str, list[Notice]],
        group_messages: list[dict[str, Any]],
    ) -> str:
        lines = [self._header(date_str, weather)]
        first_block = True

        for category in CATEGORY_ORDER:
            notices = notices_by_category.get(category) or []
            if not notices:
                continue
            lines.extend([""] * (1 if first_block else 3))
            first_block = False
            lines.append(f"【{category_title(category)}】")
            for index, item in enumerate(notices, 1):
                lines.append(f"{index}. {item['title']}")
                lines.append(f"   {item['date']}")
                lines.append(f"   {item['link']}")

        if group_messages:
            lines.extend([""] * (1 if first_block else 3))
            first_block = False
            lines.append("【群消息】")
            for index, message in enumerate(group_messages, 1):
                lines.append(
                    f"{index}. 群 {message.get('gid', '')} · "
                    f"发布者 {message.get('sender', '')}"
                )
                lines.append(f"   {message.get('text', '')}")

        if len(lines) == 1:
            lines.append("")
            lines.append("今日无新增通知。")

        return self._truncate("\n".join(lines))

    @staticmethod
    def _header(date_str: str, weather: str) -> str:
        head = f"【每日简报】{date_str}"
        if weather:
            head = f"{head}\n{weather}"
        return head

    def _truncate(self, text: str) -> str:
        limit = self._cfg_int("max_digest_chars", 3500)
        if len(text) <= limit:
            return text
        return text[: limit - 20].rstrip() + "\n……（已截断）"

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            return int(self.config.get(key, default))
        except (ValueError, TypeError):
            return default
