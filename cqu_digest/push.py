"""推送与合并转发。合并转发失败会自动退回纯文本。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain

ZWSP = "\u200b"
"""零宽空格：aiocqhttp 会 strip 纯文本，用它把首尾空白钉住。"""


class PushMixin:
    async def _push(self, text: str) -> bool:
        targets = self._push_targets()
        if not targets:
            logger.warning("[简报] 没有推送目标，本次简报只写进了 digest_last")
            return False

        sent = False
        for umo in targets:
            if await self._send_one(umo, text):
                sent = True
        return sent

    async def _send_one(self, umo: str, text: str) -> bool:
        """发一个会话。先试合并转发；失败就退回纯文本重发一次。

        合并转发依赖协议端实现（NapCat 认 send_private_forward_msg），
        所以这里必须留退路——否则协议端一变，简报就彻底发不出去了。
        """
        if self._cfg_bool("use_forward_msg", True):
            chain = self._build_forward_chain(text)
            if chain is not None:
                try:
                    ok = await self.context.send_message(umo, chain)
                    if ok:
                        return True
                    logger.warning("[简报] 合并转发没发出去，退回纯文本重发")
                except Exception as exc:
                    logger.warning(
                        f"[简报] 合并转发失败（{type(exc).__name__}: {exc}），退回纯文本重发"
                    )
        try:
            # send_message 只在会话格式非法时抛错；平台没接上时返回 False
            ok = await self.context.send_message(
                umo, MessageChain().message(f"{ZWSP}{text}{ZWSP}")
            )
        except Exception as exc:
            logger.warning(f"[简报] 推送到 {umo} 失败：{exc}")
            return False
        if not ok:
            logger.warning(
                f"[简报] 会话 {umo} 没匹配到在线平台，消息没发出去"
                "（检查 aiocqhttp 平台是否已启用）"
            )
        return bool(ok)

    def _build_forward_chain(self, text: str):
        """把简报切成节点拼成合并转发链。切不出来或超上限就返回 None（走纯文本）。"""
        try:
            from astrbot.api.message_components import Node, Nodes, Plain
        except Exception as exc:
            logger.warning(f"[简报] 取不到合并转发组件，改用纯文本：{exc}")
            return None

        segs = self._digester.split_segments(text)
        limit = self._cfg_int("max_forward_nodes", 40)
        if not segs or len(segs) > limit:
            logger.info(f"[简报] 节点数 {len(segs)} 超出上限 {limit}，改用纯文本")
            return None

        uin = str(self.config.get("bot_uin", "") or "").strip() or "0"
        nodes = [Node(content=[Plain(body)], name=name, uin=uin) for name, body in segs]
        logger.info(f"[简报] 合并转发：{len(nodes)} 个节点，uin={uin}")
        return MessageChain([Nodes(nodes)])

    async def _emit_digest(self, event: AstrMessageEvent, text: str, note: str = ""):
        """把简报正文以合并转发卡片回给当前会话；不行就退回纯文本。

        查询 / test / last / now 的正文都走这里，
        保证"点进去一条一条"的观感一致。
        """
        body = (note + chr(10) * 2 + text) if note else text
        chain = self._build_forward_chain(body)
        if chain is not None:
            umo = str(getattr(event, "unified_msg_origin", "") or "")
            try:
                ok = await self.context.send_message(umo, chain)
                if ok:
                    return
                logger.warning("[简报] 合并转发没发出去，本条改用纯文本")
            except Exception as exc:
                logger.warning(
                    f"[简报] 合并转发失败（{type(exc).__name__}: {exc}），本条改用纯文本"
                )
        yield event.plain_result(body)

    # ── 群消息采集（绝不 yield，yield 会把内容发回群里）─────────
