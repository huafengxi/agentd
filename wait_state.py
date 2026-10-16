"""Current RPC waits, not historical asks/session events.

pi-wrap owns wait-state.json. Identity is the runner's process incarnation
(pid, procStart), avoiding the spawn/pid.json publication race. report is a
read-only consumer; old producers without this snapshot simply have no hint.
"""
import copy
import math
import os
import threading
import time

import proto

DIALOG_METHODS = {"select", "confirm", "input", "editor"}
WAIT_FILE = "wait-state.json"


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


class CurrentWaits:
    def __init__(self, home, participant):
        self.path = os.path.join(home, WAIT_FILE)
        self.identity = {"task": participant, "pid": os.getpid(),
                         "procStart": proto.proc_starttime(os.getpid())}
        self.lock = threading.RLock()
        self.tools = []
        self.turns = 0
        self.waits = {}
        self._save()

    def _save(self):
        # Observability failure must not interrupt the task's RPC channel.
        try:
            proto.atomic_write_json(self.path, dict(self.identity, version=1,
                                                   waits=list(self.waits.values())))
        except OSError:
            pass

    def event(self, obj):
        with self.lock:
            old = copy.deepcopy(self.waits)
            t, call = obj.get("type"), obj.get("toolCallId")
            if t == "agent_start":
                self.turns += 1
            elif t == "tool_execution_start" and call:
                self.tools.append(call)
            elif t == "tool_execution_update" and obj.get("toolName") == "ask_dispatcher" and call in self.tools:
                details = (obj.get("partialResult") or {}).get("details") or {}
                w = details.get("agentdWait")
                if isinstance(w, dict) and w.get("askId") and w.get("question"):
                    self.waits["ask:" + call] = dict(w, kind="ask", toolCallId=call)
            elif t == "extension_ui_request" and obj.get("method") in DIALOG_METHODS and obj.get("id"):
                now = time.time()
                timeout = obj.get("timeout")
                self.waits["dialog:" + obj["id"]] = {
                    "kind": "dialog", "id": obj["id"], "request": dict(obj),
                    "question": " — ".join(str(obj[k]) for k in ("title", "message") if obj.get(k)),
                    "since": now, "deadline": now + timeout / 1000 if number(timeout) and timeout > 0 else None,
                    "toolCallId": self.tools[-1] if self.tools else None}
            elif t == "tool_execution_end":
                self.tools = [c for c in self.tools if c != call]
                self.waits = {k: w for k, w in self.waits.items() if w.get("toolCallId") != call}
            elif t == "agent_end":
                self.turns = max(0, self.turns - 1)
                if self.turns == 0:
                    self.tools.clear()
                    self.waits.clear()
            elif t == "agent_settled" and self.turns == 0 and not self.tools:
                self.waits.clear()
            if self.waits != old:
                self._save()

    def sent(self, obj):
        """Only called after a successful write to pi stdin."""
        with self.lock:
            t = obj.get("type")
            if t == "extension_ui_response":
                self.waits.pop("dialog:" + str(obj.get("id")), None)
            else:
                # A bare abort need not resolve ctx.ui or waitForReply promises.
                # sessiond sends explicit cancelled dialog responses first;
                # otherwise only the real tool end/deadline clears the wait.
                return
            self._save()

    def dialogs(self):
        """Live requests for socket attach, even if the raw ring has wrapped.

        Do not replay old/resolved dialogs or restart their timeout on attach.
        """
        with self.lock:
            now = time.time()
            out = []
            for w in self.waits.values():
                if w["kind"] != "dialog":
                    continue
                deadline = w.get("deadline")
                if number(deadline) and deadline <= now:
                    continue
                request = dict(w["request"])
                if number(deadline):
                    request["timeout"] = max(1, (deadline - now) * 1000)
                out.append(request)
            return out


def current_waits(root, participant, pid, now):
    """Read-only fail-closed snapshot; no pending-ask/keyword inference."""
    if not pid or pid.get("final") is True or pid.get("status") != "running":
        return []
    doc = proto.read_json(os.path.join(proto.agent_dir(root, participant), WAIT_FILE))
    if not isinstance(doc, dict) or doc.get("version") != 1 or doc.get("task") != participant:
        return []
    if not pid.get("procStart") or any(doc.get(k) != pid.get(k) for k in ("pid", "procStart")):
        return []
    waits = doc.get("waits")
    if not isinstance(waits, list):
        return []
    replies = None
    out = []
    for w in waits:
        if not isinstance(w, dict) or w.get("kind") not in ("ask", "dialog"):
            continue
        deadline = w.get("deadline")
        if deadline is not None and (not number(deadline) or deadline <= now):
            continue
        if w["kind"] == "ask":
            if not w.get("askId"):
                continue
            if replies is None:
                replies = {m.get("ref") for m in proto.list_messages(proto.inbox_path(root, participant))
                           if m.get("type") == "reply"}
            if w["askId"] in replies:
                continue
        out.append(w)
    return out
