import json
import os
import random
import re
import socket
import string
import time
TERMINAL_STATUSES = ('exited', 'killed', 'stale')
CRASH_STATUSES = ('exited', 'killed', 'stale')
EXITCODE_KILLED = 137
EXITCODE_STALE = 127
EXITCODE_CANCELED = 125

def now_ts() -> str:
    return time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + '.%03d' % (int(time.time() * 1000) % 1000)

def rand_suffix(n: int=4) -> str:
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

def auto_name() -> str:
    return rand_suffix(6)

def file_id() -> str:
    return now_ts() + '-' + rand_suffix()

def atomic_write(path: str, data: str) -> None:
    d = os.path.dirname(path) or '.'
    os.makedirs(d, exist_ok=True)
    tmp = os.path.join(d, '.tmp-%s-%s' % (os.getpid(), rand_suffix(6)))
    with open(tmp, 'w', encoding='utf-8') as f:
        os.fchown(f.fileno(), -1, os.getgid())
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, path)

def atomic_write_json(path: str, obj) -> None:
    atomic_write(path, json.dumps(obj, ensure_ascii=False, indent=1) + '\n')

def exclusive_write_json(path: str, obj) -> bool:
    d = os.path.dirname(path) or '.'
    os.makedirs(d, exist_ok=True)
    tmp = os.path.join(d, '.tmp-%s-%s' % (os.getpid(), rand_suffix(6)))
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            os.fchown(f.fileno(), -1, os.getgid())
            f.write(json.dumps(obj, ensure_ascii=False, indent=1) + '\n')
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError:
            return False
        return True
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass

def read_json(path: str):
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (OSError, ValueError):
        return None

def _darwin_process_facts():
    import importlib.util
    from pathlib import Path
    import sys
    configured = os.environ.get('PI_HOST_DIR')
    if configured is not None and (not configured.strip()):
        raise ValueError('PI_HOST_DIR is empty')
    root = Path(configured).expanduser().resolve() if configured is not None else Path(__file__).resolve().parent.parent / 'pi-wrap'
    source = root / 'local_process.py'
    key = '_agentd_native_process_facts'
    if key in sys.modules:
        if Path(sys.modules[key].__file__) != source:
            raise ValueError('generic P native source changed in this process')
        return sys.modules[key]
    spec = importlib.util.spec_from_file_location(key, source)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sys.modules[key] = mod
    return mod

def proc_starttime(pid):
    import sys
    if sys.platform == 'darwin':
        return _darwin_process_facts().starttime(pid)
    try:
        with open('/proc/%d/stat' % pid) as f:
            data = f.read()
    except OSError:
        return None
    try:
        rest = data.rsplit(')', 1)[1].split()
        return int(rest[22 - 3])
    except (IndexError, ValueError):
        return None

def pid_identity_ok(pid, expected_start):
    if not pid:
        return False
    if expected_start is None:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    live = proc_starttime(pid)
    if live is None:
        return False
    return live == int(expected_start)
TASK_DIR = 'task'
BOT_DIR = 'bot'
TOPIC_DIR = 'topic'
QUEUE_DIR = 'queue'
LAYOUT_DIRS = (TASK_DIR, BOT_DIR, TOPIC_DIR, QUEUE_DIR)
FAMILIES = (TASK_DIR, BOT_DIR, TOPIC_DIR, QUEUE_DIR)
NAME_RE = re.compile('^[A-Za-z0-9._-]+$')

def is_valid_name_segment(s) -> bool:
    return isinstance(s, str) and bool(NAME_RE.match(s)) and (s not in ('.', '..'))

def parse_participant_id(s):
    if not isinstance(s, str) or s.count('/') != 1:
        raise ValueError('非法参与方 id：%r——须为两段路径式 <family>/<name>，family ∈ {%s}，name ∈ [A-Za-z0-9._-]+（禁 ./../空段）' % (s, ', '.join(FAMILIES)))
    family, name = s.split('/')
    if family not in FAMILIES:
        raise ValueError('非法参与方 id：%r——未知族 %r（白名单 %s）' % (s, family, '/'.join(FAMILIES)))
    if not is_valid_name_segment(name):
        raise ValueError("非法参与方 id：%r——name 段须 ∈ [A-Za-z0-9._-]+，禁止空/'.'/'..'" % (s,))
    return (family, name)

def is_valid_participant_id(s) -> bool:
    try:
        parse_participant_id(s)
        return True
    except ValueError:
        return False
NO_REAPER = 'none'

def is_reaper_value(s) -> bool:
    return s == NO_REAPER or is_valid_participant_id(s)

def fs_safe_id(pid_: str) -> str:
    return pid_.replace('/', '.')

def task_dir(root: str, pid_: str) -> str:
    return os.path.join(root, 'agents', TASK_DIR, pid_)

def bot_dir(root: str, name: str) -> str:
    return os.path.join(root, 'agents', BOT_DIR, name)

def bot_inbox(root: str, name: str) -> str:
    return os.path.join(bot_dir(root, name), 'inbox')

def topic_dir(root: str, name: str) -> str:
    return os.path.join(root, 'agents', TOPIC_DIR, name)

def topic_inbox(root: str, name: str) -> str:
    return os.path.join(topic_dir(root, name), 'inbox')

def queue_dir(root: str, name: str) -> str:
    return os.path.join(root, 'agents', QUEUE_DIR, name)

def queue_inbox(root: str, name: str) -> str:
    return os.path.join(queue_dir(root, name), 'inbox')
POSITION_TOPIC = 'dispatcher'
POSITION_PID = QUEUE_DIR + '/' + POSITION_TOPIC
PROTECTED_SYSTEM_TOPICS = (POSITION_TOPIC,)
PROTECTED_SYSTEM_PATHS = tuple((TOPIC_DIR + '/' + t for t in PROTECTED_SYSTEM_TOPICS)) + (POSITION_PID,)

def position_inbox(root: str) -> str:
    return queue_inbox(root, POSITION_TOPIC)
RETIRED_MAILBOXES = {BOT_DIR + '/' + POSITION_TOPIC: POSITION_PID, TOPIC_DIR + '/' + POSITION_TOPIC: POSITION_PID, BOT_DIR + '/notify-user': QUEUE_DIR + '/notify-user', BOT_DIR + '/work-lead': QUEUE_DIR + '/work-lead', BOT_DIR + '/work-lead-watcher': QUEUE_DIR + '/work-lead', QUEUE_DIR + '/agentfw-lead': POSITION_PID, BOT_DIR + '/agentfw-lead': POSITION_PID, BOT_DIR + '/agentfw-lead-watcher': POSITION_PID}

def retired_move(pid_):
    return RETIRED_MAILBOXES.get(pid_)

def agent_dir(root: str, pid_: str) -> str:
    family, name = parse_participant_id(pid_)
    return os.path.join(root, 'agents', family, name)

def list_participants(root: str):
    base = os.path.join(root, 'agents')
    out = []
    for family in (TASK_DIR, BOT_DIR, QUEUE_DIR):
        fdir = os.path.join(base, family)
        if not os.path.isdir(fdir):
            continue
        out.extend((family + '/' + d for d in os.listdir(fdir) if os.path.isdir(os.path.join(fdir, d)) and (not d.startswith('.'))))
    return sorted(out)

def spec_path(root, pid_):
    return os.path.join(agent_dir(root, pid_), 'spec.json')

def inbox_path(root, pid_):
    return os.path.join(agent_dir(root, pid_), 'inbox')

def control_path(root, pid_):
    return os.path.join(agent_dir(root, pid_), 'control')

def task_sock_path(root: str, pid_: str) -> str:
    return os.path.join(root, 'run', 'agentd', pid_ + '.sock')
TASK_READY_KINDS = ('init-ok', 'recv-armed')

def task_ready_path(root: str, pid_: str, kind: str) -> str:
    if kind not in TASK_READY_KINDS:
        raise ValueError('task_ready_path: 非法 kind %r（允许 %s）' % (kind, '/'.join(TASK_READY_KINDS)))
    return os.path.join(root, 'run', 'agentd', '%s.%s' % (pid_, kind))

def agentd_lock_path(root: str, host: str) -> str:
    return os.path.join(root, 'agents', 'run', 'agentd.%s.lock' % host)

def gen_terminal(pid_doc) -> bool:
    return bool(pid_doc) and pid_doc.get('status') in TERMINAL_STATUSES

def life_terminal(pid_doc) -> bool:
    return bool(pid_doc) and pid_doc.get('final') is True and gen_terminal(pid_doc)

def ack_key(env, fn) -> str:
    raw = env.get('id') if isinstance(env, dict) else None
    raw = raw if isinstance(raw, str) else ''
    if raw and '/' not in raw and ('\\' not in raw) and ('..' not in raw):
        return raw
    return fn[:-4] if isinstance(fn, str) and fn.endswith('.msg') else fn or ''
MSG_TYPES = ('ask', 'inform', 'reply')
DELIVER_MODES = ('steer', 'followUp')
ASK_VIA_SEND_MESSAGE = 'send_message'
SYSTEM_SENDER = 'agentd'

def list_messages(inbox_dir):
    try:
        names = sorted((f for f in os.listdir(inbox_dir) if f.endswith('.msg')))
    except OSError:
        return []
    out = []
    for fn in names:
        doc = read_json(os.path.join(inbox_dir, fn))
        if not isinstance(doc, dict):
            continue
        doc['_file'] = os.path.join(inbox_dir, fn)
        doc['_name'] = fn
        out.append(doc)
    return out

def write_message(inbox_dir, envelope_no_id, retries=8):
    os.makedirs(inbox_dir, exist_ok=True)
    sender = envelope_no_id.get('from') or ''
    for _ in range(retries):
        mid = now_ts() + '-' + fs_safe_id(sender) + '-' + rand_suffix()
        path = os.path.join(inbox_dir, mid + '.msg')
        if os.path.exists(path):
            continue
        doc = dict(envelope_no_id)
        doc['id'] = mid
        atomic_write_json(path, doc)
        return (mid, path)
    raise RuntimeError('write_message：连续 %d 次文件名冲突，放弃' % retries)

def ask_scan_inboxes(root, task_pid):
    out = [position_inbox(root)]
    spec = read_json(spec_path(root, task_pid))
    reaper = (spec or {}).get('reaper')
    reaper = reaper.strip() if isinstance(reaper, str) else ''
    if reaper and is_valid_participant_id(reaper):
        cand = inbox_path(root, reaper)
        if cand not in out:
            out.append(cand)
    return out

def list_task_asks(root, task_pid):
    out, seen = ([], set())
    for ib in ask_scan_inboxes(root, task_pid):
        for m in list_messages(ib):
            if m.get('type') == 'ask' and m.get('from') == task_pid and (m.get('id') not in seen):
                seen.add(m.get('id'))
                out.append(m)
    out.sort(key=lambda m: str(m.get('id')))
    return out

def find_pending_ask(root, pid_):
    asks = list_task_asks(root, pid_)
    if not asks:
        return None
    answered = {r.get('ref') for r in list_messages(inbox_path(root, pid_)) if r.get('type') == 'reply'}
    for a in asks:
        if a.get('id') not in answered:
            return a
    return None

def ask_question_text(ask):
    body = (ask or {}).get('body')
    if isinstance(body, str):
        try:
            doc = json.loads(body)
        except ValueError:
            return body
        if isinstance(doc, dict) and isinstance(doc.get('question'), str):
            return doc['question']
        return body
    return '' if body is None else str(body)

def ask_summary(ask, limit=200):
    body = (ask or {}).get('body')
    if isinstance(body, str):
        try:
            doc = json.loads(body)
            if isinstance(doc, dict) and isinstance(doc.get('question'), str):
                return doc['question']
        except ValueError:
            pass
        t = ' '.join(body.split())
        return t[:limit] + '…' if len(t) > limit else t
    return '' if body is None else str(body)

def local_hosts(host_arg=None, aliases_arg=None):
    s = set()
    if host_arg:
        s.add(host_arg)
    for a in (aliases_arg or '').split(','):
        a = a.strip()
        if a:
            s.add(a)
    s.add(socket.gethostname())
    return s

def host_matches(spec, local_ids) -> bool:
    h = spec.get('host')
    if not h:
        return False
    return h in local_ids

def host_missing(spec) -> bool:
    return not (spec or {}).get('host')

def execution_state(root, participant):
    from execution import state_view
    return state_view(root, participant)

def participant_spec(root, participant):
    from execution import metadata
    return metadata(root, participant)
