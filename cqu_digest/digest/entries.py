"""把简报正文解析成结构化条目。

写进条目库、算截止日、渲染【即将截止】都用它。

为什么不用让模型直接吐 JSON：JSON 解析失败的风险是实打实的，而用户
2026-10-12 定的格式本来就很规整（[标题] + 固定字段名），用字符串切分
就够了；解析不出来还能退回"整篇文本"照发。

格式：

    【分类名】
    ——————————
    [标题]
    内容简述：xxx
    时间：26/07/01（报名截止）
    地点：xxx
    链接：xxx
    要求：xxx
    能获得什么：xxx
    （空一行）

多个时间就写多行，每行「时间」后面紧跟一行「地点」，一一配套。
原文没有的字段整行不出现。
"""
import re
from datetime import datetime

SEP_LINE = "——————————"
HEADER_RE = re.compile(r"^【[^】]+】$")

# 字段名（顺序就是渲染顺序）
FIELDS = ("内容简述", "时间", "地点", "链接", "要求", "能获得什么")

# 括号里的环节词命中这些 -> 这个时间点是"截止"性质
DUE_WORDS = ("截止", "报名", "提交", "结束", "结项", "报送", "申报", "申请", "截至")


def parse_date(value: str) -> str:
    """「26/07/01」-> 「2026-07-01」；认不出来返回空串。

    两位年份按 20xx 补。刻意不用正则（正则的反斜杠容易被别处转义吃掉）。
    """
    parts = value.strip().split("/")
    if len(parts) != 3:
        return ""
    y, m, d = (p.strip() for p in parts)
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        return ""
    if len(y) == 2:
        y = "20" + y
    if len(y) != 4 or not (1 <= int(m) <= 12) or not (1 <= int(d) <= 31):
        return ""
    return y + "-" + m.zfill(2) + "-" + d.zfill(2)


def split_note(value: str) -> tuple:
    """「26/07/01（报名截止）」-> （「26/07/01」,「报名截止」）。"""
    i = value.find("（")
    j = value.find("）", i + 1)
    if i != -1 and j != -1:
        return value[:i].strip(), value[i + 1:j].strip()
    return value.strip(), ""


def is_due_note(note: str) -> bool:
    """括号里的环节词是不是"截止"性质。没有环节词时返回 False。"""
    if not note:
        return False
    return any(word in note for word in DUE_WORDS)


def item_title(line: str) -> str:
    """「[标题]」->「标题」；旧的「1. 标题」也认；不是条目标题返回空串。"""
    s = line.strip()
    if s.startswith("[") and s.endswith("]") and 2 < len(s) <= 80:
        inner = s[1:-1].strip()
        if inner and "：" not in inner and ":" not in inner:
            return inner
    head, dot, rest = s.partition(". ")
    if dot and head.isdigit():
        return rest.strip()
    return ""


def split_field(line: str):
    """「内容简述：xxx」->（「内容简述」,「xxx」）；不是字段行返回 None。"""
    for name in FIELDS:
        prefix = name + "："
        if line.startswith(prefix):
            return name, line[len(prefix):].strip()
    return None


def short_date(iso: str) -> str:
    """「2026-07-01」->「26/07/01」（渲染回用户定的那个格式）。"""
    parts = str(iso).split("-")
    if len(parts) == 3 and len(parts[0]) == 4:
        return parts[0][2:] + "/" + parts[1] + "/" + parts[2]
    return str(iso)


def days_between(start: str, end: str):
    """end - start 差几天（都是 YYYY-MM-DD）；认不出来返回 None。"""
    try:
        a = datetime.strptime(str(start), "%Y-%m-%d")
        b = datetime.strptime(str(end), "%Y-%m-%d")
    except Exception:                                       # noqa: BLE001
        return None
    return (b - a).days


def earliest_time(entry: dict) -> str:
    """条目里最早的那个日期（YYYY-MM-DD）；一个都没有就返回空串。

    截止日和活动日都算 —— "7/15 是决赛"同样是不该错过的时刻。
    """
    dates = [t["date"] for t in entry.get("times", []) if t.get("date")]
    return min(dates) if dates else ""


def is_valid(record: dict, today: str, no_due_expire_days: int = 30) -> bool:
    """这条记录还算"没过期"吗。

        有截止日 -> 截止日 >= 今天
        没截止日 -> 入库（或发布）没超过 no_due_expire_days 天 —— 就是用户说的
                    "无截止销毁日期"
    认不出日期就当作没过期（宁可多留）。
    """
    if not isinstance(record, dict):
        return False
    due = str(record.get("due", "") or "")
    if due:
        return due >= str(today)
    stamp = str(record.get("published", "") or "") or str(record.get("first_seen", "") or "")
    if not stamp:
        return True
    age = days_between(stamp, today)
    if age is None:
        return True
    return age <= no_due_expire_days


def earliest_due(entry: dict) -> str:
    """条目里最早的【截止性质】日期；没有就返回空串。"""
    dates = [t["date"] for t in entry.get("times", []) if t.get("date") and t.get("is_due")]
    return min(dates) if dates else ""


def split_entries(text: str) -> list:
    """把简报正文切成条目列表。

    返回 [{"cat","title","fields","times","block","time","due"}, ...]
        fields 是六件套里除时间/地点外的字段
        times  是 [{"date","raw","note","place","is_due"}]
        block  是这条的原始文本（要原样用的时候直接用）
        time   最早的任意日期（含活动日）
        due    最早的截止日
    """
    out: list = []
    cat = ""
    cur = None
    pending = None          # 等一个"地点"来跟它配对

    def flush() -> None:
        nonlocal cur, pending
        if cur is not None:
            out.append(cur)
        cur = None
        pending = None

    for raw in text.split(chr(10)):
        line = raw.rstrip()
        s = line.strip()
        if not s or s == SEP_LINE:
            continue

        if HEADER_RE.match(s):
            flush()
            cat = s
            continue

        title = item_title(s)
        if title:
            flush()
            cur = {"cat": cat, "title": title, "fields": {}, "times": [], "block": [line]}
            continue

        if cur is None:
            continue
        cur["block"].append(line)

        parsed = split_field(s)
        if parsed is None:
            continue
        name, value = parsed
        if not value:
            continue
        if name == "时间":
            date_raw, note = split_note(value)
            pending = {"raw": date_raw, "date": parse_date(date_raw),
                       "note": note, "place": "", "is_due": is_due_note(note)}
            cur["times"].append(pending)
        elif name == "地点" and pending is not None:
            pending["place"] = value
            pending = None
        else:
            cur["fields"][name] = value

    flush()
    for entry in out:
        entry["block"] = chr(10).join(entry["block"]).strip()
        entry["time"] = earliest_time(entry)
        entry["due"] = earliest_due(entry)
    return out
