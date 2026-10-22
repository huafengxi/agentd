"""Bounded read-only session tool observation; execution identity comes from rund."""
import datetime, io, json, os, re, stat, time
import proto
from pathlib import Path
TOOL_SESSION_BYTES=8*1024*1024
TOOL_SESSION_LINE_BYTES=1024*1024
TOOL_SESSION_ENTRIES=20000
TOOL_DISPLAY_LIMIT=4
PROTO_TS_RE=re.compile(r"^(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})(\.\d+)?$")

def ts_to_epoch(s):
    """时间戳字符串 → epoch 秒；解析失败返回 None（半截文件防御）。"""
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.strip()
    try:
        dt = datetime.datetime.fromisoformat(s)
    except ValueError:
        dt = None
    if dt is not None:
        if dt.tzinfo is not None:
            return dt.timestamp()
        return time.mktime(dt.timetuple()) + dt.microsecond / 1e6
    m = PROTO_TS_RE.match(s)
    if not m:
        return None
    try:
        y, mo, d, h, mi, se = (int(x) for x in m.groups()[:6])
        frac = float(m.group(7) or 0)
        return time.mktime((y, mo, d, h, mi, se, 0, 0, -1)) + frac
    except (ValueError, OverflowError):
        return None


def fmt_dur(secs):
    """秒数 → 紧凑时长（35s / 12m / 1h05m / 2d3h）；None/负 → `-`。"""
    if secs is None or secs < 0:
        return "-"
    secs = int(secs)
    if secs < 60:
        return "%ds" % secs
    if secs < 3600:
        return "%dm" % (secs // 60)
    if secs < 86400:
        return "%dh%02dm" % (secs // 3600, secs % 3600 // 60)
    return "%dd%dh" % (secs // 86400, secs % 86400 // 3600)


def read_json_safe(path):
    """读 JSON；缺失/半截/非 dict → (None, ok_flag)。"""
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        if isinstance(doc, dict):
            return doc, True
    except (OSError, ValueError):
        pass
    return None, False


def current_tools(adir, pid_doc, now):
    """Observe the persisted v3 branch, never a live execution/wait verdict.

    The wrapper passes this exact file to pi. Restarts reuse it, so exclude calls
    predating startedAt; without any record from this generation it is unavailable.
    Native _buildIndex resumes at the last entry, then follows parentId. In-memory
    branch/session changes without a persisted entry cannot be observed here.
    """
    unavailable = "不可得"
    try:
        identity = ("pid", "procStart", "gen", "startedAt")
        started = ts_to_epoch(pid_doc.get("startedAt"))
        if (pid_doc.get("status") != "running"
                or type(pid_doc.get("pid")) is not int or pid_doc["pid"] <= 0
                or pid_doc.get("procStart") is None
                or not pid_doc.get("gen")
                or started is None or started > now):
            return unavailable
        path = os.path.join(adir, "session", "session.jsonl")
        if not stat.S_ISREG(os.stat(path).st_mode):
            return unavailable
        with open(path, "rb") as f:
            before = os.fstat(f.fileno())
            if before.st_size > TOOL_SESSION_BYTES:
                return unavailable
            data = f.read(TOOL_SESSION_BYTES + 1)
            after = os.fstat(f.fileno())
        if (len(data) > TOOL_SESSION_BYTES or not data.endswith(b"\n")
                or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)):
            return unavailable
        nodes = {}
        header = None
        leaf = None
        for line in io.BytesIO(data):
            if len(line) > TOOL_SESSION_LINE_BYTES:
                return unavailable
            if not line.strip():
                continue
            entry = json.loads(line)
            if not isinstance(entry, dict):
                return unavailable
            if header is None:
                header = entry
                stamp = ts_to_epoch(header.get("timestamp"))
                if (header.get("type") != "session" or header.get("version") != 3
                        or not header.get("id") or stamp is None or stamp > now
                        ):
                    return unavailable
                continue
            eid, parent = entry.get("id"), entry.get("parentId")
            stamp = ts_to_epoch(entry.get("timestamp"))
            if (entry.get("type") == "session" or not isinstance(eid, str) or not eid
                    or eid in nodes or "parentId" not in entry
                    or (parent is not None and parent not in nodes)
                    or stamp is None or stamp > now or len(nodes) >= TOOL_SESSION_ENTRIES):
                return unavailable
            # Retain no question, arguments, result body or assistant prose.
            calls, result = [], None
            if entry.get("type") == "message":
                msg = entry.get("message")
                if not isinstance(msg, dict):
                    return unavailable
                if msg.get("role") == "assistant":
                    content = msg.get("content")
                    if not isinstance(content, list):
                        return unavailable
                    for block in content:
                        if not isinstance(block, dict):
                            return unavailable
                        if block.get("type") == "toolCall" and stamp >= started:
                            cid, name = block.get("id"), block.get("name")
                            if (not isinstance(cid, str) or not cid or not isinstance(name, str)
                                    or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", name)):
                                return unavailable
                            calls.append((cid, name, stamp))
                elif msg.get("role") == "toolResult":
                    result = msg.get("toolCallId")
                    if not isinstance(result, str) or not result:
                        return unavailable
            nodes[eid] = (parent, stamp, calls, result)
            leaf = eid
        # Do not mistake an old process's file for this generation's observations.
        if leaf is None or nodes[leaf][1] < started:
            return unavailable
        branch = []
        while leaf is not None:
            node = nodes[leaf]
            branch.append(node)
            leaf = node[0]
        pending, seen_calls = {}, set()
        for _, _, calls, result in reversed(branch):
            for cid, name, stamp in calls:
                if cid in seen_calls:
                    return unavailable
                seen_calls.add(cid)
                pending[cid] = (name, stamp)
            pending.pop(result, None)
        # Ignore heartbeat-only changes; reject a concurrent generation/terminal change.
        latest = proto.execution_state(str(Path(adir).parents[2]), str(Path(adir).relative_to(Path(adir).parents[1])))
        if (latest is None or latest.get("final") is True or latest.get("status") != "running"
                or any(latest.get(k) != pid_doc.get(k) for k in identity)):
            return unavailable
        items = ["`%s` (%s)" % (name, fmt_dur(now - stamp))
                 for name, stamp in list(pending.values())[:TOOL_DISPLAY_LIMIT]]
        if len(pending) > TOOL_DISPLAY_LIMIT:
            items.append("另 %d 项" % (len(pending) - TOOL_DISPLAY_LIMIT))
        if not items:
            return "-"
        return ("%d 项：" % len(pending) if len(pending) > 1 else "") + "；".join(items)
    except Exception:
        # Observation is optional even for unexpected/native-format parse failures.
        return unavailable

