"""推送与合并转发。合并转发失败会自动退回纯文本。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from cqu_digest.digest import blocks as block_utils

ZWSP = "\u200b"
"""零宽空格：aiocqhttp 会 strip 纯文本，用它把首尾空白钉住。"""


class PushMixin:
    async def _push(self, text: str, blocks: list | None = None) -> int:
        """推送，返回【成功发出去的会话数】。

        ★ 用计数而不是 bool：多目标时原来只要有一个成功就返回 True，
          用户看到「已推送」，另一个目标的失败只留在日志里 ——
          而日志文件默认是关的，他看不到。
        """
        if not self._cfg_bool("push_enabled", True):
            logger.info("[简报] push_enabled 关着，本次不推送（抓取和记账照常）")
            return 0
        targets = self._push_targets()
        if not targets:
            logger.warning("[简报] 没有推送目标，本次简报只写进了 digest_last")
            return 0

        sent = 0
        for umo in targets:
            if await self._send_one(umo, text, blocks):
                sent += 1
            else:
                logger.warning(f"[简报] 会话 {umo} 没发出去")
        if sent < len(targets):
            logger.warning(f"[简报] 推送目标 {len(targets)} 个，只有 {sent} 个成功")
        return sent

    async def _send_one(self, umo: str, text: str, blocks: list | None = None,
                        uin: str = "") -> bool:
        """给一个会话发一份简报：先试合并转发卡片，失败退回纯文本。

        这是【唯一】的发消息出口 —— 定时推送、回当前会话、通知管理员都走它。
        所以 use_forward_msg 这类开关只要在这里判断一次，就对所有路径生效。
        （以前 _emit_digest 自己发卡片，那条路绕过了这个开关。）

        失败时把原因记在 self._forward_fail_reason，
        好让调用方决定要不要把原因写给用户看（日志文件默认是关的）。
        """
        self._forward_fail_reason = ""
        if self._cfg_bool("use_forward_msg", True):
            chain = self._build_forward_chain(text, uin=uin, blocks=blocks)
            if chain is not None:
                try:
                    ok = await self.context.send_message(umo, chain)
                    if ok:
                        return True
                    self._forward_fail_reason = "平台没接上（send_message 返回 False）"
                    logger.warning("[简报] 合并转发没发出去，退回纯文本重发")
                except Exception as exc:                    # noqa: BLE001
                    self._forward_fail_reason = f"{type(exc).__name__}: {exc}"
                    logger.warning(
                        f"[简报] 合并转发失败（{type(exc).__name__}: {exc}），退回纯文本重发"
                    )
        plain = block_utils.expand_block_text(text, blocks or [])
        try:
            # send_message 只在会话格式非法时抛错；平台没接上时返回 False
            ok = await self.context.send_message(
                umo, MessageChain().message(f"{ZWSP}{plain}{ZWSP}")
            )
        except Exception as exc:                            # noqa: BLE001
            self._forward_fail_reason = f"纯文本也没发出去：{type(exc).__name__}: {exc}"
            logger.warning(f"[简报] 推送到 {umo} 失败：{exc}")
            return False
        if not ok:
            self._forward_fail_reason = "纯文本也没匹配到在线平台"
            logger.warning(
                f"[简报] 会话 {umo} 没匹配到在线平台，消息没发出去"
                "（检查 aiocqhttp 平台是否已启用）"
            )
        return bool(ok)

    def _self_id(self) -> str:
        """机器人自己的 QQ 号。

        合并转发每个节点都要带一个 user_id，否则协议端可能直接拒收。
        定时任务没有 event，所以顺序是：
            ① 配置里的 bot_uin
            ② 最近一次从 event 拿到的（_remember_self_id 缓存的）
            ③ 实在没有才退回 0（并记下原因，降级时会写给你看）
        """
        up = str(self.config.get("bot_uin", "") or "").strip()
        if up:
            return up
        return str(getattr(self, "_cached_self_id", "") or "").strip()

    def _remember_self_id(self, event: Any) -> str:
        """从 event 里取机器人 QQ 号并存起来，给以后的定时推送用。"""
        getter = getattr(event, "get_self_id", None)
        if not callable(getter):
            return ""
        try:
            sid = str(getter() or "").strip()
        except Exception as exc:                                # noqa: BLE001
            logger.warning(f"[简报] 取机器人 QQ 号失败：{exc}")
            return ""
        if sid:
            self._cached_self_id = sid
        return sid

    def _build_forward_chain(self, text: str, uin: str = "", blocks: list | None = None):
        """把简报切成节点拼成合并转发链。切不出来或超上限就返回 None（走纯文本）。

        失败时把原因记在 self._forward_fail_reason，好让降级提示说得清。
        """
        self._forward_fail_reason = ""
        try:
            from astrbot.api.message_components import Image, Node, Nodes, Plain
        except Exception as exc:                                # noqa: BLE001
            self._forward_fail_reason = f"取不到合并转发组件：{exc}"
            logger.warning("[简报] " + self._forward_fail_reason)
            return None

        segs = self._digester.split_segments(text)
        limit = self._cfg_int("max_forward_nodes", 60)
        # 群消息原文会被展开成额外的节点，所以按展开后的数量估算上限
        estimate = 0
        for _name, body in segs:
            estimate += 1
            for line in body.split(chr(10)):
                if block_utils.block_ref_of(line) is not None:
                    estimate += 1
        if not segs or estimate > limit:
            self._forward_fail_reason = f"节点数 {estimate} 超出上限 {limit}"
            logger.info("[简报] " + self._forward_fail_reason + "，改用纯文本")
            return None

        use_uin = (uin or self._self_id() or "").strip()
        if not use_uin:
            self._forward_fail_reason = "拿不到机器人 QQ 号（可在插件配置里填 bot_uin）"
            logger.warning("[简报] " + self._forward_fail_reason)
            use_uin = "0"

        nodes = []
        for name, body in segs:
            refs = [
                block_utils.block_ref_of(line)
                for line in body.split(chr(10))
                if block_utils.block_ref_of(line) is not None
            ]
            if refs and blocks:
                # 原文内嵌卡片在前，去掉占位符的规范化条目在后
                for block_no in refs:
                    nodes.append(
                        self._make_block_node(block_no, blocks, use_uin, Node, Plain, Image)
                    )
                cleaned = chr(10).join(
                    line for line in body.split(chr(10))
                    if block_utils.block_ref_of(line) is None
                ).strip()
                nodes.append(Node(content=[Plain(cleaned or name)], name=name, uin=use_uin))
            else:
                cleaned = chr(10).join(
                    line for line in body.split(chr(10))
                    if block_utils.block_ref_of(line) is None
                ).strip()
                nodes.append(Node(content=[Plain(cleaned or body)], name=name, uin=use_uin))
        logger.info(f"[简报] 合并转发：{len(nodes)} 个节点，uin={use_uin}")
        return MessageChain([Nodes(nodes)])

    def _make_block_node(self, block_no: int, blocks: list, uin: str,
                         Node, Plain, Image):
        """把一个群消息块拼成【内嵌卡片节点】（Node 里塞 Node）。

        2026-10-12 实测：NapCat 认这种嵌套；塞 Nodes 容器会报
        「未知的消息类型：undefined」，所以只能塞【单个 Node】。
        """
        inner = []
        for who, parts in block_utils.block_card_nodes(blocks, block_no):
            content = []
            for kind, value in parts:
                if kind == "image":
                    try:
                        content.append(Image(file=value))
                    except Exception:                       # noqa: BLE001
                        content.append(Plain("图片：" + value))
                else:
                    content.append(Plain(value))
            inner.append(Node(content=content or [Plain("（空）")],
                              name=who or "群消息", uin=uin))
        if not inner:
            inner = [Node(content=[Plain("（空）")], name="群消息", uin=uin)]
        return Node(content=[Plain("群消息原文")] + inner, name="群消息原文", uin=uin)

    async def _notify_admin_new(self, event: Any, text: str,
                                blocks: list | None = None) -> bool:
        """群友查到新通知 -> 给管理员私聊发一句提示 + 同一份卡片。

        返回"有没有至少一个管理员真的收到了"。调用方拿它决定要不要写 seen_ids：
        收到了才写（不然定时推送会再推一次），没收到就不写（留给定时补）。
        """
        if not self._cfg_bool("query_notify_admin", True):
            return False
        admins = [str(x).strip() for x in self._cfg_list("command_allow_from", [])
                  if str(x).strip()]
        if not admins:
            return False

        # ★ 是管理员自己在查就不用通知他自己。
        #   原来这里判的是 `target == umo`，但 target 永远是
        #   platform:FriendMessage:qq、群会话的 umo 是 platform:GroupMessage:gid，
        #   两者【永远不可能相等】—— 等于没判。于是管理员在开放群里发 /digest
        #   会收到私聊的「群友查到新的消息了！」，而且被记成「群友查到的」。
        if self._is_authorized(event):
            return False

        # 平台 ID 从触发它的那条 event 的 umo 里取（aiocqhttp:GroupMessage:群号）
        umo = str(getattr(event, "unified_msg_origin", "") or "")
        platform = umo.split(":", 1)[0] if ":" in umo else ""
        if not platform:
            logger.warning("[简报] 拿不到平台 ID，群友查到新通知时没法通知管理员")
            return False

        delivered = False
        for qq in admins:
            target = platform + ":FriendMessage:" + qq
            if target == umo:
                continue                    # 就是他自己在查，不用再通知一遍
            try:
                await self.context.send_message(
                    target, MessageChain().message(ZWSP + "群友查到新的消息了！" + ZWSP)
                )
            except Exception as exc:                            # noqa: BLE001
                logger.warning(f"[简报] 通知管理员 {qq} 失败：{exc}")
                continue
            if await self._send_one(target, text, blocks):
                delivered = True
        return delivered

    async def _emit_digest(self, event: AstrMessageEvent, text: str, note: str = "",
                          blocks: list | None = None):
        """把简报回给【当前会话】。发不成功时把原因一并写给用户。

        它相对 _send_one 只多做一件事：把降级原因写进回复正文。
        因为日志文件默认是关的（log_file_enable=False），只写日志的话用户什么都看不到。
        其余（开关判断、建卡片、退回纯文本）全部交给 _send_one。
        """
        body = (note + chr(10) * 2 + text) if note else text
        umo = str(getattr(event, "unified_msg_origin", "") or "")
        sid = self._remember_self_id(event)
        if umo and await self._send_one(umo, body, blocks, uin=sid):
            return
        why = getattr(self, "_forward_fail_reason", "") or "未知原因"
        logger.warning(f"[简报] 回话没成功，改用纯文本：{why}")
        yield event.plain_result("[合并转发没成功：" + why + "]"
                                + chr(10) * 2
                                + block_utils.expand_block_text(body, blocks or []))

    # ── 群消息采集（绝不 yield，yield 会把内容发回群里）─────────
