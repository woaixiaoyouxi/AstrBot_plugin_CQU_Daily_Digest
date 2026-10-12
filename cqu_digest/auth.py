"""两级权限：管理员 QQ 全部指令；开放群只能查询。非授权消息静默 + stop_event。"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain

class AuthMixin:
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
