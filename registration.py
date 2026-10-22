"""Validate and publish task declarations for every registration adapter."""
import json
import os
import re

import proto


SPEC_KEYS = frozenset({"command", "workdir", "creator", "restartPolicy", "name",
                       "reaper", "createdAt", "host", "createdByHost", "resources",
                       "needs", "profile", "resident"})


def register_task(root, name, spec, prompt=None):
    """Publish spec last; refuse invalid input and existing task directories.

    Adapters resolve caller-specific policy, routing defaults and prompt text.
    This function owns protocol validation and the only task publication point.
    """
    if not os.path.isdir(os.path.join(root, "agents")):
        raise ValueError("工作区 agents/ 必须已存在")
    if not isinstance(name, str) or not proto.is_valid_name_segment(name) or \
            name.startswith(".") or set("*?[]") & set(name):
        raise ValueError("非法目录名：%r" % name)
    if not isinstance(spec, dict) or set(spec) - SPEC_KEYS:
        raise ValueError("spec 必须为对象且只含任务协议字段")
    for key in ("command", "workdir", "creator", "createdAt", "host", "createdByHost"):
        if not isinstance(spec.get(key), str) or not spec[key].strip():
            raise ValueError("spec.%s 必须为非空字符串" % key)
    if not os.path.isabs(os.path.expanduser(spec["workdir"])):
        raise ValueError("spec.workdir 必须为绝对路径或 ~/ 路径")
    if "resident" in spec and type(spec["resident"]) is not bool:
        raise ValueError("spec.resident 必须为布尔值")
    if "name" in spec and not isinstance(spec["name"], str):
        raise ValueError("spec.name 必须为字符串")
    if "restartPolicy" in spec and spec["restartPolicy"] not in ("manual", "auto", "one-shot"):
        raise ValueError("restartPolicy 取值须为 manual|auto|one-shot")
    if "reaper" in spec and not proto.is_reaper_value(spec["reaper"]):
        raise ValueError("spec.reaper 非法")
    for key in ("resources", "needs"):
        if key in spec and (not isinstance(spec[key], list) or
                            any(not isinstance(v, str) for v in spec[key])):
            raise ValueError("spec.%s 必须为字符串数组" % key)
    if "profile" in spec:
        profile = spec["profile"]
        if not isinstance(profile, str) or not re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9._-]*", profile):
            raise ValueError("spec.profile 名非法")
        if not spec["command"].startswith("DISPATCH_PROFILE=%s " % profile):
            raise ValueError("spec.profile 与 command 前缀不一致")
    if prompt is not None and (not isinstance(prompt, str) or not prompt.strip()):
        raise ValueError("prompt 必须为非空字符串")
    # Reject unencodable strings before reserving a directory.
    if prompt is not None:
        prompt.encode("utf-8")
    json.dumps(spec, ensure_ascii=False).encode("utf-8")
    from execution import client
    client(root)
    adir = proto.task_dir(root, name)
    for candidate in (adir, proto.bot_dir(root, name)):
        if os.path.exists(candidate):
            raise ValueError("拒绝创建：%s 已存在" % candidate)
    os.makedirs(os.path.dirname(adir), exist_ok=True)
    try:
        os.mkdir(adir)
    except FileExistsError:
        raise ValueError("拒绝创建：%s 已存在" % adir) from None
    os.mkdir(os.path.join(adir, "inbox"))
    os.mkdir(os.path.join(adir, "session"))
    if prompt is not None:
        proto.atomic_write(os.path.join(adir, "prompt.md"), prompt)
    proto.atomic_write_json(os.path.join(adir, "spec.json"), spec)
    from execution import submit
    submit(root,'task/'+name,spec)
    return {"taskId": name, "agentDir": adir}
