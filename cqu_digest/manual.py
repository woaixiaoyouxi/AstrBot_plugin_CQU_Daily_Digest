"""管理员手工录入 + 转发。

为什么单独一个文件：它和「抓取 -> 简报」是两条独立的入口，共用同一套
条目格式（entries.py）和存储（DigestStore），但不碰抓取引擎。

三条路径：
  · 新通知     管理员私聊发内容 -> LLM 整理成条目 -> 入库 -> 分发到目标群
  · 结         引用某条已入库的消息 + 「结」 -> 把那条标记为已结束
  · 追加       引用某条已入库的消息 + 其他内容 -> 补进那条的正文

引用是最可靠的关联方式：被引用消息的 id 就在消息段里，不用靠标题去猜。
bot 首次处理一条手工消息时会把「它的消息 id -> 通知 id」记下来，
之后你引用它说「结」，就能对上。
"""

import asyncio
from hashlib import sha1
from typing import Any

from astrbot.api import logger
from astrbot.api.event import MessageChain

from cqu_digest.base import CHINA_TZ
from cqu_digest.digest.entries import split_entries, to_records

KEY_MSG_LINKS = "manual_msg_links"
"""{消息 id: 通知 id}。管理员引用一条消息说「结」时，靠它把两边对上。

存成 dict 而不是两个平行 list：查的时候只要一次 get，不用每次对齐索引。
"""

BROADCAST_GAP_SECONDS = 3
"""发到下一个群之间等几秒。连续秒发容易被 QQ 判定成异常。"""

CLOSE_WORDS = ("结", "已结", "截止", "已截止", "结束", "已结束", "结项")
"""判「结」用的词。整条消息等于其中任意一个才算，不做包含匹配 ——
「报名截止」这种正文片段太常见，按包含判会误伤。"""

EXTRACT_HINT = """你是一个信息整理器。把用户发来的消息整理成下面这个格式，只输出格式，不要任何解释：

[标题]
内容简述：一句话说清是什么事
时间：YY/MM/DD（报名截止）
地点：xxx
链接：xxx

规则：
- 没有的字段整行不要写
- 时间只写消息里明确出现的日期，认不出就整行不写
- 标题不超过 40 字
- 如果这条消息完全不是通知类内容（闲聊、表情、无意义文字），只输出 NO_NOTICE
"""


class ManualMixin:
    # ── 权限 ────────────────────────────────────────────────────

    def _manual_admins(self) -> list[str]:
        """谁能用转发。优先取专门的名字，没配就沿用指令管理员名单。"""
        explicit = self._cfg_list("broadcast_admin_qq", [])
        if explicit:
            return explicit
        return self._cfg_list("command_allow_from", [])

    def _manual_allowed(self, event: Any) -> bool:
        admins = self._manual_admins()
        if not admins:
            return False
        try:
            sender = str(event.get_sender_id() or "").strip()
        except Exception as exc:                            # noqa: BLE001
            logger.warning(f"[简报] 手工消息取发送者失败：{exc}")
            return False
        return bool(sender) and sender in admins

    # ── 消息解析 ────────────────────────────────────────────────

    @staticmethod
    def _segments(event: Any) -> list[Any]:
        obj = getattr(event, "message_obj", None)
        chain = getattr(obj, "message", None) if obj is not None else None
        return list(chain or [])

    def _reply_id(self, event: Any) -> str:
        """这条消息引用了哪一条；没引用返回空串。"""
        for seg in self._segments(event):
            name = type(seg).__name__
            if name == "Reply":
                return str(getattr(seg, "id", "") or "").strip()
        return ""

    def _text_of(self, event: Any) -> str:
        """去掉引用段的纯文本。"""
        parts = []
        for seg in self._segments(event):
            if type(seg).__name__ == "Plain":
                parts.append(str(getattr(seg, "text", "") or ""))
        return "".join(parts).strip()

    def _images_of(self, event: Any) -> list[dict[str, str]]:
        """取图片段，整理成 OneBot 能直接吃的字段。"""
        out: list[dict[str, str]] = []
        for seg in self._segments(event):
            if type(seg).__name__ != "Image":
                continue
            url = str(getattr(seg, "url", "") or "").strip()
            file = str(getattr(seg, "file", "") or "").strip()
            if url or file:
                out.append({"file": file or url, "url": url or file})
        return out

    # ── 主入口 ──────────────────────────────────────────────────

    async def handle_manual(self, event: Any, text_override: str | None = None) -> str:
        """处理一条管理员私聊消息，返回回执（空串 = 不说话）。

        text_override：调用方剥掉 /b 前缀之后传进来的正文。传 None 表示不覆盖。
        """
        if not self._cfg_bool("broadcast_enabled", False):
            return ""
        if not self._manual_allowed(event):
            return ""

        text = text_override if text_override is not None else self._text_of(event)
        images = self._images_of(event)
        reply_id = self._reply_id(event)

        # 引用 + 「结」：先把那条关掉
        if reply_id and text in CLOSE_WORDS:
            return await self._close_by_reply(reply_id)

        # 引用 + 其他内容：补进那条
        if reply_id and text:
            return await self._append_by_reply(reply_id, text, images)

        # 没有引用：当新通知处理
        if not text and not images:
            return ""
        return await self._new_from_message(event, text, images)

    # ── 结 ──────────────────────────────────────────────────────

    async def _close_by_reply(self, reply_id: str) -> str:
        links = await self._get_links()
        notice_id = str(links.get(reply_id, "") or "")
        if not notice_id:
            return ("引用的那条我没记过，找不到对应通知。"
                    "你引用的是我发的简报的话我认不出来 —— 请引用你当时发给我的那条原文。")
        entries = await self._store.get_entries()
        if notice_id not in entries:
            return "找到了关联，但那条通知已经不在库里了（可能过期被清了）。"
        entries[notice_id]["closed"] = True
        entries[notice_id]["closed_date"] = self._today()
        await self._store.set_entries(entries)
        title = str(entries[notice_id].get("title", "") or "")
        logger.info(f"[简报] 手工关闭：{notice_id} {title}")
        return f"已结掉「{title}」。"

    async def _append_by_reply(self, reply_id: str, text: str,
                               images: list[dict[str, str]]) -> str:
        links = await self._get_links()
        notice_id = str(links.get(reply_id, "") or "")
        if not notice_id:
            return "引用的那条我找不到对应通知，能告诉我你指的是哪一条吗？"
        entries = await self._store.get_entries()
        if notice_id not in entries:
            return "关联到了，但那条已经不在库里了。"
        record = entries[notice_id]
        fields = record.get("fields")
        if not isinstance(fields, dict):
            fields = {}
            record["fields"] = fields
        old = str(fields.get("内容简述", "") or "")
        fields["内容简述"] = (old + "；" + text) if old else text
        if images:
            fields["链接"] = fields.get("链接") or images[0]["url"]
        entries[notice_id] = record
        await self._store.set_entries(entries)
        return "已补进上面那条通知。"

    # ── 新通知 ──────────────────────────────────────────────────

    async def _new_from_message(self, event: Any, text: str,
                                images: list[dict[str, str]]) -> str:
        entry = await self._extract_entry(text)
        if entry is None:
            return ("这条看起来不像是通知（没有可提取的信息）。"
                    "如果确实要发出去，回我一句「照发」，我就原样转发。")

        record = {
            "id": "",
            "cat": "【手工录入】",
            "title": entry["title"],
            "fields": entry["fields"],
            "times": entry["times"],
            "link": entry.get("link", ""),
            "time": entry.get("time", ""),
            "due": entry.get("due", ""),
            # 只存 URL，不存图片本身。合并转发时能嵌成真图片，
            # 退回纯文本时降级成一行「图片：地址」。
            "images": [img["url"] for img in images if str(img.get("url") or "").strip()],
        }
        seed = "manual|" + record["title"] + "|" + self._today()
        record["id"] = "m" + sha1(seed.encode("utf-8")).hexdigest()[:16]

        await self._store.upsert_entries([record], self._today())
        await self._store.add_seen_ids([record["id"]])

        # 记下「你发的这条消息 -> 这条通知」，以后引用它就能对「结」
        msg_id = self._current_msg_id(event)
        if msg_id:
            await self._set_link(msg_id, record["id"])

        forwarded = await self._broadcast(event, text, images)
        head = f"已录入「{record['title']}」"
        if forwarded:
            head += f"，转发到 {forwarded} 个群"
        if not msg_id:
            head += ("（拿不到消息 id，下次引用这条说「结」可能对不上，"
                     "到时候直接说标题也行）")
        return head

    def _current_msg_id(self, event: Any) -> str:
        obj = getattr(event, "message_obj", None)
        if obj is None:
            return ""
        return str(getattr(obj, "message_id", "") or "").strip()

    # ── LLM 抽取 ────────────────────────────────────────────────

    async def _extract_entry(self, text: str) -> dict | None:
        """把一段自由文本交给模型整理成条目；整理不出来就返回 None。

        模型不可用时退回最朴素的做法：整段当标题，不编造字段 ——
        编造的日期比没有日期危险得多。
        """
        if not text.strip():
            return None
        provider_id = self._digester._resolve_provider_id(self.context)
        if not provider_id:
            return self._fallback_entry(text)

        prompt = EXTRACT_HINT + "\n\n消息内容：\n" + text
        try:
            response = await asyncio.wait_for(
                self.context.llm_generate(chat_provider_id=provider_id, prompt=prompt),
                timeout=self._cfg_int("llm_timeout_seconds", 60),
            )
        except Exception as exc:                            # noqa: BLE001
            logger.warning(f"[简报] 手工消息整理失败，退回朴素解析：{exc}")
            return self._fallback_entry(text)

        raw = str(getattr(response, "completion_text", "") or "").strip()
        if not raw or "NO_NOTICE" in raw:
            return None
        parsed = split_entries(raw)
        if not parsed:
            return self._fallback_entry(text)
        first = parsed[0]
        fields = dict(first.get("fields") or {})
        link = str(fields.pop("链接", "") or "")
        return {
            "title": str(first.get("title", "") or text[:40]),
            "fields": fields,
            "times": first.get("times") or [],
            "link": link,
            "time": str(first.get("time", "") or ""),
            "due": str(first.get("due", "") or ""),
        }

    @staticmethod
    def _fallback_entry(text: str) -> dict:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        title = (lines[0] if lines else text)[:60]
        brief = " ".join(lines[1:])[:300] if len(lines) > 1 else ""
        fields = {"内容简述": brief} if brief else {}
        return {"title": title, "fields": fields, "times": [],
                "link": "", "time": "", "due": ""}

    # ── 转发 ────────────────────────────────────────────────────

    async def _broadcast(self, event: Any, text: str,
                         images: list[dict[str, str]]) -> int:
        """把管理员刚发的内容原样发到目标群，返回成功的群数。"""
        groups = [g for g in self._cfg_list("broadcast_groups", []) if g.isdigit()]
        if not groups:
            logger.warning("[简报] 转发开关开着，但目标群列表是空的")
            return 0

        platform = self._platform_of(event)
        if not platform:
            logger.warning("[简报] 转发失败：拿不到平台 ID")
            return 0

        sent = 0
        for index, group in enumerate(groups):
            if index:
                await asyncio.sleep(BROADCAST_GAP_SECONDS)
            umo = f"{platform}:GroupMessage:{group}"
            try:
                if await self._send_manual(umo, text, images):
                    sent += 1
                else:
                    logger.warning(f"[简报] 群 {group} 没发出去")
            except Exception as exc:                        # noqa: BLE001
                logger.warning(f"[简报] 群 {group} 转发异常：{exc}")
        return sent

    async def _send_manual(self, umo: str, text: str,
                           images: list[dict[str, str]]) -> bool:
        try:
            from astrbot.api.message_components import Image
        except Exception as exc:                            # noqa: BLE001
            logger.warning(f"[简报] 取不到消息组件：{exc}")
            return False

        if not text and not images:
            return False

        chain = MessageChain()
        if text:
            chain.message(text)
        for image in images:
            try:
                chain.message(Image(file=image["file"]))
            except Exception:                               # noqa: BLE001
                if not text:
                    chain.message("图片：" + image["url"])
        return bool(await self.context.send_message(umo, chain))

    @staticmethod
    def _platform_of(event: Any) -> str:
        umo = str(getattr(event, "unified_msg_origin", "") or "")
        return umo.split(":", 1)[0] if ":" in umo else ""

    # ── 关联表 ──────────────────────────────────────────────────

    async def _get_links(self) -> dict[str, str]:
        raw = await self.get_kv_data(KEY_MSG_LINKS, {})
        if not isinstance(raw, dict):
            return {}
        return {str(k): str(v) for k, v in raw.items()}

    async def _set_link(self, msg_id: str, notice_id: str) -> None:
        links = await self._get_links()
        links[msg_id] = notice_id
        # 只留最近 300 条，别让它无限长
        if len(links) > 300:
            links = dict(list(links.items())[-300:])
        await self.put_kv_data(KEY_MSG_LINKS, links)
