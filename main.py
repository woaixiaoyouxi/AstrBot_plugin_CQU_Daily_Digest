"""重庆大学每日简报。

每天定时（默认 09:00）：抓校内通知 / 竞赛 → 取近 24h 群消息 → 加今日天气
→ 交给 LLM 整理成纯文本简报 → 推送到登记的 QQ 会话。

骨架层 fork 自 astrbot_plugin_DUT_Notices（AGPL-3.0），抓取引擎保留，
推送方式由「逐条推」改成「聚合成一份简报再推」。
"""

import asyncio
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register


def _bootstrap_path() -> None:
    """挂 sys.path + 清掉本插件的子包缓存。两件事都必须做。

    ① AstrBot 是按【文件】加载 main.py 的（不是按包），所以得自己加路径。

    ② ★ 热重载必须清 sys.modules。Python 的 import 会先查 sys.modules，
       不把 cqu_digest.* 清掉的话，点了【重载插件】也只会拿到旧模块 ——
       表现就是"改了代码不生效，只有重启才行"。
       （原来的 _load_local_module 里有 sys.modules.pop，换成包导入时漏了。）

    包名带前缀还顺手避开一个雷：以前本插件与 dut_notices 都往 sys.modules
    塞顶层的 sources / parsers，再装第三个同名模块的插件就可能互相覆盖。
    """
    here = str(Path(__file__).resolve().parent)
    if here not in sys.path:
        sys.path.insert(0, here)
    stale = [n for n in list(sys.modules) if n == "cqu_digest" or n.startswith("cqu_digest.")]
    for name in stale:
        sys.modules.pop(name, None)
    if stale:
        logger.info(f"[简报] 热重载：清掉 {len(stale)} 个旧的子包缓存")


_bootstrap_path()

from cqu_digest import auth, commands, config, pipeline, push, schedule  # noqa: E402
from cqu_digest.config import _flatten_config                  # noqa: E402
from cqu_digest.digest.group_collector import GroupCollector   # noqa: E402
from cqu_digest.digest.renderer import Digester                # noqa: E402
from cqu_digest.auth import AuthMixin                          # noqa: E402
from cqu_digest.commands import CommandsMixin                  # noqa: E402
from cqu_digest.config import ConfigMixin                      # noqa: E402
from cqu_digest.pipeline import PipelineMixin                  # noqa: E402
from cqu_digest.push import PushMixin                          # noqa: E402
from cqu_digest.schedule import ScheduleMixin                  # noqa: E402
from cqu_digest.digest.storage import DigestStore              # noqa: E402
from cqu_digest.base import CHINA_TZ, Notice  # noqa: E402
from cqu_digest.fetch.engine import NoticeFetcher  # noqa: E402
from cqu_digest.fetch.sources import (                         # noqa: E402
    SOURCES_BY_KEY,
    format_source_lines,
    unconfigured_keys,
)
from cqu_digest.fetch.weather import WeatherClient             # noqa: E402

class CquDailyDigestPlugin(
    ConfigMixin, ScheduleMixin, PipelineMixin, PushMixin, AuthMixin,
    CommandsMixin, Star,
):
    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        super().__init__(context)
        # 原始配置留着写回（分组形态，面板要照它渲染）；读取一律走摊平后的
        self._raw_config: dict[str, Any] = config if isinstance(config, dict) else {}
        self.config = _flatten_config(self._raw_config)
        self._stop_event = asyncio.Event()
        self._scheduler_task: asyncio.Task | None = None
        self._catchup_task: asyncio.Task | None = None
        self._run_lock = asyncio.Lock()
        # 公开查询的冷却缓存：umo -> (取到的时刻, 简报文本)。
        # 不写 KV 也不写 digest_last——digest_last 是「他那一份」，被群查询覆盖掉就乱了。
        self._query_cache: dict[str, tuple[float, str, list | None]] = {}
        # 机器人自己的 QQ 号：event 触发时拿到就存起来，定时推送时用它
        self._cached_self_id = ""
        # 正在跑 + 在排队的流水线数（排队提示要报位置）
        self._waiting = 0
        # 合并转发失败的原因（降级提示要说清）
        self._forward_fail_reason = ""

        self._fetcher = NoticeFetcher(self.config)
        self._store = DigestStore(self.get_kv_data, self.put_kv_data)
        self._weather = WeatherClient(self.config)
        self._collector = GroupCollector(self._store, self.config)
        self._digester = Digester(self.config)

    # ── 生命周期 ────────────────────────────────────────────────

    async def initialize(self):
        self._stop_event.clear()
        self._warn_unconfigured()
        # 补偿绝不能 await：AstrBot 是逐个 await 各插件的 initialize()，
        # 一次补偿要抓网页+调模型，堵在这里会把整个插件加载拖住。
        if await self._should_catch_up():
            logger.info("[简报] 启动补偿：今天已过推送点且尚未推送，后台补跑一次")
            self._catchup_task = asyncio.create_task(self._catch_up())
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("[简报] 插件初始化完成，定时任务已启动。")

    async def terminate(self):
        self._stop_event.set()
        for task in (self._scheduler_task, self._catchup_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        logger.info("[简报] 插件已停止。")

    def _warn_unconfigured(self):
        missing = unconfigured_keys()
        if missing:
            logger.warning(
                f"[简报] 以下来源还没有 URL/selector，抓不到内容：{', '.join(missing)}"
            )
        if not str(self.config.get("llm_provider", "") or "").strip():
            logger.info("[简报] 未指定 llm_provider，将使用 AstrBot 的全局默认模型")
        if not self._push_targets():
            logger.warning("[简报] 没有推送目标，去插件配置里填 push_targets")
        if not self._cfg_list("command_allow_from", []):
            logger.warning(
                "[简报] command_allow_from 是空的，管理指令（/digest now 之类）对谁都不响应。"
                "要在 QQ 上用，请在插件配置里填上自己的 QQ 号"
            )
        groups = self._cfg_list("command_allow_groups", [])
        if groups:
            logger.info(f"[简报] {len(groups)} 个群开放了公开查询：{', '.join(groups)}")

    # ── 调度 ────────────────────────────────────────────────────

    @filter.command("digest")
    async def digest_entry(self, event: AstrMessageEvent, sub: str = ""):
        """每日简报：直接发 /digest 取一份，或跟 help/now/test/all/status/reset 子指令。"""
        action = str(sub or "").strip().lower()

        if action in ("", "query"):
            async for result in self._cmd_query(event):
                yield result
            return

        handler = {
            "all": self._cmd_query_all,
            "help": self._cmd_help,
            "now": self._cmd_now,
            "reset": self._cmd_reset,
            "status": self._cmd_status,
            "test": self._cmd_test,
        }.get(action)
        if handler is None:
            # 认不出的子指令：给说明。能走到这里说明已被授权，回一句不算冒泡。
            if self._deny(event):
                return
            yield event.plain_result(self._help_text())
            return

        async for result in handler(event):
            yield result

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        # ★ 群策略闸门：先看这个群是什么模式，再决定要不要采集 / 要不要让 AI 接话
        mode = self._session_mode(event)
        if mode == "ignore":
            event.stop_event()
            return
        try:
            await self._collector.record(event)
        except Exception as exc:
            logger.warning(f"[简报] 群消息入库失败：{exc}")
        if mode == "readonly_notice":
            # 只采集进简报，一个字都不说
            event.stop_event()
        elif mode == "chat_passive":
            # 只在被 @ 或叫名字时回
            if not self._is_addressed_to_bot(event):
                event.stop_event()
        # chat_active：放行，走 AstrBot 原生对话

    def _session_mode(self, event: AstrMessageEvent) -> str:
        """查会话模式：chat_active / chat_passive / readonly_notice / ignore。

        配置格式（list 每项一条）："<群号或QQ号>:<模式>"
        没列出的会话默认 ignore（什么都不做）。
        """
        spec = self._cfg_list("session_modes", [])
        mapping: dict[str, str] = {}
        for line in spec:
            key, _, value = str(line).partition(":")
            key = key.strip()
            if key:
                mapping[key] = (value.strip() or "ignore")
        try:
            gid = str(event.get_group_id() or "")
        except Exception:
            gid = ""
        try:
            uid = str(event.get_sender_id() or "")
        except Exception:
            uid = ""
        for key in (gid, uid):
            if key and key in mapping:
                return mapping[key]
        return "ignore"

    def _is_addressed_to_bot(self, event: AstrMessageEvent) -> bool:
        """这条消息是不是在叫机器人（@ 或叫名字）。"""
        try:
            if bool(getattr(event, "is_at_or_wake_command", False)):
                return True
        except Exception:
            pass
        text = str(getattr(event, "message_str", "") or "")
        name = str(self.config.get("bot_name", "") or "").strip()
        return bool(name) and name in text

    # ── 指令 ────────────────────────────────────────────────────
    #
    # 用一条扁平指令 /digest 收所有子指令，不用 command_group：指令组的匹配器
    # 在「只发 /digest、不带子指令」时会抛 ValueError，AstrBot 接住后**往会话里
    # 回一条报错**（waking_check/stage.py 的 except 分支）——那是在任何权限判断
    # 之前发生的，白名单外的人照样会收到回复，静默就破了。扁平指令自己分派，
    # 权限全在 _deny 里收口。
