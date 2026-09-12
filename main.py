"""重庆大学每日简报。

每天定时（默认 09:00）：抓校内通知 / 竞赛 → 取近 24h 群消息 → 加今日天气
→ 交给 LLM 整理成纯文本简报 → 推送到登记的 QQ 会话。

骨架层 fork 自 astrbot_plugin_DUT_Notices（AGPL-3.0），抓取引擎保留，
推送方式由「逐条推」改成「聚合成一份简报再推」。
"""

import asyncio
import importlib.util
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register


def _load_local_module(module_name: str):
    module_path = Path(__file__).resolve().with_name(f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ModuleNotFoundError(f"Cannot load local plugin module: {module_name}")

    sys.modules.pop(module_name, None)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


for _module_name in (
    "parsers",
    "sources",
    "fetcher",
    "storage",
    "weather",
    "group_collector",
    "digester",
):
    _load_local_module(_module_name)

from digester import Digester
from fetcher import CHINA_TZ, Notice, NoticeFetcher
from group_collector import GroupCollector
from sources import SOURCES_BY_KEY, format_source_lines, unconfigured_keys
from storage import DigestStore
from weather import WeatherClient

BODY_FETCH_CONCURRENCY = 5
ZWSP = "​"  # aiocqhttp 会 strip 纯文本，用零宽空格把首尾空白钉住

QUERY_RECENT_DAYS = 3
"""公开查询只看最近几天的通知。

查询走的是 dry_run，没有 seen_ids 基线，不过滤就等于把整站历史倒出来，
几千字被 max_digest_chars 截成半截。按日期卡一个窗口，输出才像一份简报。
"""


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


@register(
    "astrbot_plugin_cqu_daily_digest",
    "woaixiaoyouxi",
    "聚合重庆大学校内通知与校外竞赛，合并 QQ 群消息与天气，每天定时推送一份纯文本简报",
    "0.1.2",
)
class CquDailyDigestPlugin(Star):
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
        self._query_cache: dict[str, tuple[float, str]] = {}

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
            logger.warning("[简报] 没有推送目标，用 /digest register 在当前会话登记")
        if not self._cfg_list("command_allow_from", []):
            logger.warning(
                "[简报] command_allow_from 是空的，管理指令（/digest now 之类）对谁都不响应。"
                "要在 QQ 上用，请在插件配置里填上自己的 QQ 号"
            )
        groups = self._cfg_list("command_allow_groups", [])
        if groups:
            logger.info(f"[简报] {len(groups)} 个群开放了公开查询：{', '.join(groups)}")

    # ── 调度 ────────────────────────────────────────────────────

    async def _should_catch_up(self) -> bool:
        """今天已过推送点、但今天还没跑过 → 需要补一次（asyncio 重启即断的兜底）。"""
        if not self._cfg_bool("catch_up_on_start", True):
            return False
        now = datetime.now(CHINA_TZ)
        if now < self._due_time(now):
            return False
        return await self._store.get_last_run_date() != self._today(now)

    async def _catch_up(self):
        try:
            await self._run_once()
        except Exception as exc:
            logger.error(f"[简报] 启动补偿失败：{exc}")

    async def _scheduler_loop(self):
        while not self._stop_event.is_set():
            delay = self._seconds_until_next_push()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                return  # 收到停止信号
            except asyncio.TimeoutError:
                pass
            try:
                await self._run_once()
            except Exception as exc:
                logger.error(f"[简报] 定时任务失败：{exc}")

    def _due_time(self, now: datetime) -> datetime:
        hour, minute = self._push_time()
        return now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    def _seconds_until_next_push(self) -> float:
        now = datetime.now(CHINA_TZ)
        target = self._due_time(now)
        if target <= now:
            target += timedelta(days=1)
        return max(1.0, (target - now).total_seconds())

    # ── 主流水线 ────────────────────────────────────────────────

    @staticmethod
    def _result(
        *,
        text: str = "",
        new: int = 0,
        pushed: bool = False,
        seeded: bool = False,
        skipped: bool = False,
    ) -> dict[str, Any]:
        """统一返回值形状——调用方固定读这五个键，别各写各的 dict。"""
        return {"text": text, "new": new, "pushed": pushed, "seeded": seeded, "skipped": skipped}

    async def _run_once(
        self,
        *,
        dry_run: bool = False,
        recent_days: int | None = None,
        with_group_messages: bool = True,
    ) -> dict[str, Any]:
        """跑一次完整流程。

        已有一次在跑就直接跳过——两条流水线并发会读到同一份 seen_ids，
        算出同一批「新增」，然后推两遍。
        """
        if self._run_lock.locked():
            logger.info("[简报] 已有一次运行在进行中，本次跳过")
            return self._result(skipped=True)
        async with self._run_lock:
            return await self._run_pipeline(
                dry_run=dry_run,
                recent_days=recent_days,
                with_group_messages=with_group_messages,
            )

    async def _run_pipeline(
        self,
        *,
        dry_run: bool = False,
        recent_days: int | None = None,
        with_group_messages: bool = True,
    ) -> dict[str, Any]:
        """跑一次完整流程。dry_run 不推送、不写 seen_ids（调试源 / 公开查询用）。"""
        categories = self._enabled_categories()
        notices = await self._fetcher.fetch_notices(categories=categories) if categories else []

        seen = await self._store.get_seen_ids()
        # 首次运行：不把整站历史一次性推给用户，只记基线
        seeded = not dry_run and not seen and not await self._store.get_last_run_date()

        if dry_run or seeded:
            fresh = list(notices)
        else:
            fresh = [item for item in notices if item["id"] not in seen]

        if recent_days:
            fresh = self._within_days(fresh, recent_days)

        await self._fill_bodies(fresh)
        if not with_group_messages:
            group_messages: list[dict[str, Any]] = []
        else:
            group_messages = (
                await self._collector.peek_recent()
                if dry_run
                else await self._collector.consume_recent()
            )

        text = await self._digester.build(
            self.context,
            date_str=self._today(),
            weather=await self._weather_text(),
            notices_by_category=self._group_by_category(fresh),
            group_messages=group_messages,
        )

        pushed = False
        if not dry_run:
            await self._store.set_last_digest(text)
            if seeded:
                logger.info(f"[简报] 首次运行：记录 {len(notices)} 条为基线，本次不推送")
            else:
                pushed = await self._push(text)
            await self._store.add_seen_ids([item["id"] for item in notices])
            await self._store.set_last_run_date(self._today())

        return self._result(text=text, new=len(fresh), pushed=pushed, seeded=seeded)

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

            await asyncio.gather(*(fill(item) for item in targets), return_exceptions=True)

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

    async def _weather_text(self) -> str:
        if not self._cfg_bool("weather_enabled", True):
            return ""
        city = str(self.config.get("weather_city", "Chongqing") or "Chongqing")
        return await self._weather.today(city)

    # ── 推送 ────────────────────────────────────────────────────

    async def _push(self, text: str) -> bool:
        targets = self._push_targets()
        if not targets:
            logger.warning("[简报] 没有推送目标，本次简报只写进了 digest_last")
            return False

        sent = False
        for umo in targets:
            try:
                # send_message 只在会话格式非法时抛错；平台没接上时返回 False
                ok = await self.context.send_message(
                    umo, MessageChain().message(f"{ZWSP}{text}{ZWSP}")
                )
            except Exception as exc:
                logger.warning(f"[简报] 推送到 {umo} 失败：{exc}")
                continue
            if ok:
                sent = True
            else:
                logger.warning(
                    f"[简报] 会话 {umo} 没匹配到在线平台，消息没发出去"
                    "（检查 aiocqhttp 平台是否已启用）"
                )
        return sent

    # ── 群消息采集（绝不 yield，yield 会把内容发回群里）─────────

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        try:
            await self._collector.record(event)
        except Exception as exc:
            logger.warning(f"[简报] 群消息入库失败：{exc}")

    # ── 指令 ────────────────────────────────────────────────────
    #
    # 用一条扁平指令 /digest 收所有子指令，不用 command_group：指令组的匹配器
    # 在「只发 /digest、不带子指令」时会抛 ValueError，AstrBot 接住后**往会话里
    # 回一条报错**（waking_check/stage.py 的 except 分支）——那是在任何权限判断
    # 之前发生的，白名单外的人照样会收到回复，静默就破了。扁平指令自己分派，
    # 权限全在 _deny 里收口。

    @filter.command("digest")
    async def digest_entry(self, event: AstrMessageEvent, sub: str = ""):
        """每日简报：直接发 /digest 取一份，或跟 help/now/test/last 等子指令。"""
        action = str(sub or "").strip().lower()

        if action in ("", "query"):
            async for result in self._cmd_query(event):
                yield result
            return

        handler = {
            "help": self._cmd_help,
            "now": self._cmd_now,
            "test": self._cmd_test,
            "last": self._cmd_last,
            "sources": self._cmd_sources,
            "register": self._cmd_register,
            "unregister": self._cmd_unregister,
            "targets": self._cmd_targets,
        }.get(action)
        if handler is None:
            # 认不出的子指令：给说明。能走到这里说明已被授权，回一句不算冒泡。
            if self._deny(event):
                return
            yield event.plain_result(self._help_text())
            return

        async for result in handler(event):
            yield result

    async def _cmd_query(self, event: AstrMessageEvent):
        """公开查询：现算一份给当前会话。

        走 dry_run——不推送、不写 seen_ids、不消费群缓冲，所以外人刷它不会
        动到他的增量基线。按 umo 记冷却，冷却期内直接回上一次算好的那份，
        避免有人连点把 API 额度烧穿。
        """
        if self._deny(event):
            return

        key = str(getattr(event, "unified_msg_origin", "") or "")
        cooldown = self._cfg_int("query_cooldown_seconds", 120)
        cached = self._query_cache.get(key)
        if cached and cooldown > 0:
            waited = time.monotonic() - cached[0]
            if waited < cooldown:
                yield event.plain_result(
                    f"距上次生成才 {int(waited)} 秒，先看这一份"
                    f"（{cooldown} 秒后可再算）：\n\n{cached[1]}"
                )
                return

        result = await self._run_once(
            dry_run=True,
            recent_days=QUERY_RECENT_DAYS,
            with_group_messages=False,
        )
        if result["skipped"]:
            yield event.plain_result("刚好在生成简报，稍等十几秒再来。")
            return

        text = result["text"] or "（什么都没抓到，可能是源还没配好）"
        if cooldown > 0:
            self._query_cache[key] = (time.monotonic(), text)
        yield event.plain_result(text)

    async def _cmd_help(self, event: AstrMessageEvent):
        """查看使用说明。"""
        if self._deny(event):
            return
        yield event.plain_result(self._help_text())

    async def _cmd_now(self, event: AstrMessageEvent):
        """立即跑一次完整流程并推送。"""
        if self._deny(event, admin_only=True):
            return
        result = await self._run_once()
        if result["skipped"]:
            yield event.plain_result("已经有一次运行在进行中，稍等片刻再试。")
            return
        if result["seeded"]:
            yield event.plain_result(
                f"首次运行：已把当前 {result['new']} 条记为基线，本次不推送"
                "（避免一次性刷屏）。\n再触发一次即可看到真正的增量。"
            )
            return
        if not result["pushed"]:
            yield event.plain_result(
                "简报已生成，但没有推送目标——用 /digest register 在当前会话登记。\n\n"
                + result["text"]
            )
            return
        yield event.plain_result(f"已推送，本次新增 {result['new']} 条。")

    async def _cmd_test(self, event: AstrMessageEvent):
        """只看简报长什么样：抓取+渲染，但不推送、不写去重记录。"""
        if self._deny(event, admin_only=True):
            return
        result = await self._run_once(dry_run=True)
        if result["skipped"]:
            yield event.plain_result("已经有一次运行在进行中，稍等片刻再试。")
            return
        yield event.plain_result(result["text"] or "（没有抓到任何内容，先检查源配置）")

    async def _cmd_last(self, event: AstrMessageEvent):
        """查看最近一次生成的简报原文。"""
        if self._deny(event, admin_only=True):
            return
        text = await self._store.get_last_digest()
        yield event.plain_result(text or "还没有生成过简报。")

    async def _cmd_sources(self, event: AstrMessageEvent):
        """列出所有信息源及其配置状态。"""
        if self._deny(event, admin_only=True):
            return
        lines = ["信息源：", *format_source_lines()]
        missing = unconfigured_keys()
        if missing:
            lines += ["", f"⚠️ 还没配 URL/selector：{', '.join(missing)}"]
        yield event.plain_result("\n".join(lines))

    async def _cmd_register(self, event: AstrMessageEvent):
        """把当前会话登记为推送目标（私聊或群都行）。"""
        if self._deny(event, admin_only=True):
            return
        umo = event.unified_msg_origin
        targets = self._push_targets()
        if umo in targets:
            yield event.plain_result(f"当前会话已经是推送目标：{umo}")
            return
        targets.append(umo)
        self._save_push_targets(targets)
        yield event.plain_result(f"已登记为推送目标：{umo}")

    async def _cmd_unregister(self, event: AstrMessageEvent):
        """取消当前会话的推送登记。"""
        if self._deny(event, admin_only=True):
            return
        umo = event.unified_msg_origin
        targets = self._push_targets()
        if umo not in targets:
            yield event.plain_result(f"当前会话不是推送目标：{umo}")
            return
        targets.remove(umo)
        self._save_push_targets(targets)
        yield event.plain_result(f"已取消推送登记：{umo}")

    async def _cmd_targets(self, event: AstrMessageEvent):
        """列出全部推送目标。"""
        if self._deny(event, admin_only=True):
            return
        targets = self._push_targets()
        if not targets:
            yield event.plain_result("当前没有推送目标。")
            return
        yield event.plain_result("\n".join(["推送目标：", *[f"- {t}" for t in targets]]))

    # ── 配置读写 ────────────────────────────────────────────────

    def _push_targets(self) -> list[str]:
        raw = self.config.get("push_targets", [])
        if not isinstance(raw, list):
            return []
        return [str(item) for item in raw if str(item).strip()]

    def _save_push_targets(self, targets: list[str]) -> None:
        self.config["push_targets"] = targets
        # 写回分组里的那份，否则面板上看不到新登记的会话
        raw = self._raw_config
        for value in raw.values():
            if isinstance(value, dict) and "push_targets" in value:
                value["push_targets"] = targets
        save = getattr(raw, "save_config", None)
        if callable(save):
            try:
                save()
            except Exception as exc:
                logger.warning(f"[简报] 配置保存失败：{exc}")

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

    def _is_authorized(self, event: AstrMessageEvent) -> bool:
        """指令只认白名单里的 QQ，其余静默忽略。

        白名单为空 = **谁都不能用**（不是「不设限」）。这是故意的：默认值会随
        插件一起公开，写成「留空即全开」的话，别人装完忘了配就等于把 /digest now
        对外开放——那玩意儿会烧 API 额度、还会往群里发消息。宁可默认锁死。

        拒绝时故意不回「无权限」：机器人待在没禁言的群里，任何一句回复都是噪音。
        """
        allowed = self._cfg_list("command_allow_from", [])
        if not allowed:
            return False
        getter = getattr(event, "get_sender_id", None)
        if not callable(getter):
            return False
        try:
            sender = str(getter() or "").strip()
        except Exception as exc:
            logger.warning(f"[简报] 取发送者 QQ 失败：{exc}")
            return False
        return bool(sender) and sender in allowed

    def _is_group_query_allowed(self, event: AstrMessageEvent) -> bool:
        """群名单里的群，任何人都可以查询。

        跟 command_allow_from 一样，留空 = 谁都不能查（不是「不设限」）。
        名单填的是**群号**，判的不是发送者——群里的人来来去去，按人管不过来。
        """
        allowed = self._cfg_list("command_allow_groups", [])
        if not allowed:
            return False
        group_id = self._group_id(event)
        return bool(group_id) and group_id in allowed

    @staticmethod
    def _group_id(event: Any) -> str:
        """取群号；私聊或取不到就是空串（空串永远不会撞上名单）。"""
        getter = getattr(event, "get_group_id", None)
        if not callable(getter):
            return ""
        try:
            return str(getter() or "").strip()
        except Exception as exc:
            logger.warning(f"[简报] 取群号失败：{exc}")
            return ""

    def _deny(self, event: AstrMessageEvent, *, admin_only: bool = False) -> bool:
        """不在授权范围就吞掉这条消息。返回 True = 已拒绝，调用方直接 return。

        授权范围分两级：
        - 管理（command_allow_from 里的 QQ）：全部指令
        - 查询（command_allow_groups 里的群，群内任何人）：只有 /digest query/help

        光「不回复」不够：消息会继续流向别的插件和 AstrBot 的 AI 对话，机器人
        照样会在群里说话。stop_event() 把它截在这儿。
        """
        if self._is_authorized(event):
            return False
        if not admin_only and self._is_group_query_allowed(event):
            return False
        stop = getattr(event, "stop_event", None)
        if callable(stop):
            stop()
        return True

    @staticmethod
    def _today(now: datetime | None = None) -> str:
        return (now or datetime.now(CHINA_TZ)).strftime("%Y-%m-%d")

    def _help_text(self) -> str:
        return "\n".join(
            [
                "每日简报——使用说明",
                "",
                "任何人可用（限已开放的群）：",
                "- /digest：现算一份发到本群（只含校内通知与竞赛）",
                "- /digest query：同上",
                "- /digest help：查看本说明",
                "",
                "仅管理员可用：",
                "- /digest now：立即跑一次完整流程并推送",
                "- /digest test：看完整简报（含群消息）长什么样，不推送、不写去重",
                "- /digest last：查看最近一次推送的原文",
                "- /digest sources：查看信息源配置状态",
                "- /digest register：把当前会话登记为推送目标",
                "- /digest unregister：取消当前会话的推送登记",
                "- /digest targets：列出全部推送目标",
                "",
                "查询有冷却时间，同一个会话短时间内只会现算一次，",
                "期内再来直接返回上一次的结果。简报只会主动发到登记过的",
                "会话，不会自己往群里冒泡。",
                "",
                "推送时间与内容开关都在插件配置里改。简报只摘与在校学生",
                "相关的条目（活动竞赛、评奖选课、宿舍食堂等），行政公文由",
                "模型自行略去。",
            ]
        )
