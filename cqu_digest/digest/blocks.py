"""群消息块：把扁平的消息聚成「一条通知」，以及它的各种形态。

★ 这个模块是【纯算法】—— 不碰 IO、不 import astrbot、不读配置。
  所以它能脱离 AstrBot / httpx / bs4 单独跑测试；
  协议端或 AstrBot 版本变了，这里一行都不用改。
  编排（读群缓冲、读配置）留在 group_collector.py。

「块」是什么：通知群里一条通知常常分好几条发（消息1 + 消息2 + 图），
块就是把它们合成一个整体，好让「这是不是一条通知」的判定有完整上下文。

块的形状：
    {"gid", "ts", "last_ts", "items": [原始消息, ...], "closed"}
"""

BLOCK_REF_PREFIX = "块号："
"""模型每条群消息转发条目的第一行，写它对应的块编号。"""

BLOCK_HEAD = "{{BLOCK:"
BLOCK_TAIL = "}}"
"""占位符。stamp 时写进去，发送前展开成内嵌卡片或原文。"""


def group_into_blocks(messages: list, gap_seconds: int,
                      same_sender: bool = False) -> list:
    """把消息聚成块。

    规则：同一个群 + 相邻两条间隔 <= gap_seconds，
         （可选）并且是同一个人发的。

    为什么要「同一个人」：只看时间的话，通知后面的群友回复（「收到」
    「我也想去」）会落进同一个窗口，整块被模型判成「是通知」一起转出去。
    代价是两个人接力发的一条通知会被拆开 —— 但拆开只是分成两条、不丢内容。

    纯函数：不读配置、不写 KV、不改入参。
    """
    blocks: list = []
    for item in sorted(messages or [], key=lambda x: int(x.get("ts", 0))):
        gid = str(item.get("gid", ""))
        ts = int(item.get("ts", 0))
        cur = blocks[-1] if blocks else None
        joinable = (
            cur is not None
            and cur["gid"] == gid
            and ts - cur["last_ts"] <= gap_seconds
        )
        if joinable and same_sender and cur["items"]:
            last_sender = str(cur["items"][-1].get("sender", ""))
            joinable = last_sender == str(item.get("sender", ""))
        if joinable:
            cur["items"].append(item)
            cur["last_ts"] = ts
        else:
            blocks.append({"gid": gid, "ts": ts, "last_ts": ts,
                           "items": [item], "closed": False})
    return blocks


def mark_closed(blocks: list) -> list:
    """处理「结」：把对应的块标成 closed=True。就地改，并返回同一个列表。

    三种形态：
        引用回复了某条 -> 关掉被引用那条所在的块
        跟在通知后面   -> 关掉自己所在的块
        单独发一条     -> 关掉该群里上一个还没关的块

    只是标记、不删除 —— 调用方自己决定要不要滤掉。
    """
    by_mid: dict = {}
    for index, block in enumerate(blocks):
        for item in block["items"]:
            mid = str(item.get("mid", "") or "")
            if mid:
                by_mid[mid] = index

    for index, block in enumerate(blocks):
        for pos, item in enumerate(block["items"]):
            if not item.get("is_end"):
                continue
            target = by_mid.get(str(item.get("reply_to", "") or ""))
            if target is not None:
                blocks[target]["closed"] = True
                continue
            if pos > 0:
                block["closed"] = True
                continue
            for j in range(index - 1, -1, -1):
                if blocks[j]["gid"] == block["gid"] and not blocks[j]["closed"]:
                    blocks[j]["closed"] = True
                    break
    return blocks


def block_ref_of(line: str):
    """从一行里认出 {{BLOCK:N}}，返回 N（1 起）；不是占位符返回 None。"""
    s = (line or "").strip()
    if not (s.startswith(BLOCK_HEAD) and s.endswith(BLOCK_TAIL)):
        return None
    mid = s[len(BLOCK_HEAD):-len(BLOCK_TAIL)]
    return int(mid) if mid.isdigit() else None


def stamp_block_refs(text: str, blocks: list) -> str:
    """把每条里的「块号：N」整行换成占位符 {{BLOCK:N}}。

    N 越界或不是数字就整行丢掉（模型偶尔会编一个不存在的块号）。
    """
    if not text:
        return text
    total = len(blocks or [])
    out = []
    for line in text.split(chr(10)):
        s = line.strip()
        if not s.startswith(BLOCK_REF_PREFIX):
            out.append(line)
            continue
        raw = s[len(BLOCK_REF_PREFIX):].strip()
        if raw.isdigit() and 1 <= int(raw) <= total:
            indent = line[:len(line) - len(line.lstrip())]
            out.append(indent + BLOCK_HEAD + raw + BLOCK_TAIL)
    return chr(10).join(out)


def expand_block_text(text: str, blocks: list) -> str:
    """纯文本路径：把占位符整行换成原文。

    展不开（没块了、编号越界）就丢掉那一行 ——
    绝不让 {{BLOCK:3}} 这种字样漏到用户眼前。
    """
    if not text or BLOCK_HEAD not in text:
        return text
    out = []
    for line in text.split(chr(10)):
        block_no = block_ref_of(line)
        if block_no is None:
            out.append(line)
            continue
        plain = block_plain_text(blocks, block_no)
        if plain:
            out.append(plain)
    return chr(10).join(out)


def block_plain_text(blocks: list, block_no: int) -> str:
    """一个块的纯文本形态。"""
    if not blocks or not (1 <= block_no <= len(blocks)):
        return ""
    lines = []
    for message in blocks[block_no - 1].get("items") or []:
        who = message.get("sender_name") or message.get("sender", "")
        lines.append("群 " + str(message.get("gid", "")) + " · " + str(who))
        lines.append("   " + str(message.get("text") or "（无文字）"))
        for pic in message.get("images") or []:
            lines.append("   图片：" + str(pic))
    return chr(10).join(lines)


def block_card_nodes(blocks: list, block_no: int) -> list:
    """一个块的内嵌卡片数据：[（发布者, [（类型, 值）, …]）, …]。

    类型是 text / image。★ 这里只吐【数据】，拼 Node 是 push.py 的事 ——
    这样这个模块不用 import astrbot 的消息组件，也就保住了「能单独测」。
    """
    if not blocks or not (1 <= block_no <= len(blocks)):
        return []
    out = []
    for message in blocks[block_no - 1].get("items") or []:
        who = str(message.get("sender_name") or message.get("sender") or "")
        parts = [("text", str(message.get("text") or "（无文字）"))]
        for pic in message.get("images") or []:
            parts.append(("image", str(pic)))
        out.append((who, parts))
    return out
