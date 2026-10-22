"""Agent notification policy. Execution facts are read from serviced."""
import datetime
import fcntl
import json
import logging
import os
import sys
import time
import proto
from task_state import is_resident as spec_is_resident
log = logging.getLogger(__name__)
LOCK_REFRESH_INTERVAL = 5.0
REPLAY_GRACE_DEFAULT = 300.0
REPLAY_GUARD_LOG = 'historical notification'
NOTIFY_TO = proto.POSITION_PID
NOTIFY_MARK = 'notified.json'

def ts_epoch(ts):
    try:
        head, _, frac = ts.partition('.')
        return time.mktime(time.strptime(head, '%Y-%m-%d-%H-%M-%S')) + (float('.' + frac) if frac else 0.0)
    except (ValueError, OverflowError, TypeError, AttributeError):
        return None

def lock_path(root, host):
    return proto.agentd_lock_path(root, host)

def _lock_doc(host, started_at):
    return {'host': host, 'pid': os.getpid(), 'procStart': proto.proc_starttime(os.getpid()), 'startedAt': started_at, 'updatedAt': proto.now_ts()}

def refresh_lock(lockfile, host, started_at):
    proto.atomic_write_json(lockfile, _lock_doc(host, started_at))

def acquire_single_instance_lock(root, host):
    guard = os.path.join(root, 'run', 'locks', 'agentd.%s.lock' % host)
    os.makedirs(os.path.dirname(guard), exist_ok=True)
    handle = open(guard, 'a+')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        log.error('另一个 agentd runner 已在运行，本进程退出')
        sys.exit(0)
    lockfile = lock_path(root, host)
    refresh_lock(lockfile, host, proto.now_ts())
    log.info('single-instance lock acquired: %s (pid %d)', guard, os.getpid())
    return (handle, lockfile)

def setup_logging(log_file, level):
    h = logging.FileHandler(log_file, encoding='utf-8') if log_file else logging.StreamHandler()
    h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
    root = logging.getLogger()
    root.handlers[:] = [h]
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

class Notifications:

    def __init__(self, root, host, aliases, interval, lockfile=None, lock_host=None, replay_grace=REPLAY_GRACE_DEFAULT):
        self.root = os.path.abspath(root)
        self.local_ids = proto.local_hosts(host, aliases)
        self.interval = interval
        self.lockfile = lockfile
        self.lock_host = lock_host
        self._lock_refresh_at = time.monotonic() + LOCK_REFRESH_INTERVAL
        self.lock_started_at = proto.now_ts()
        if lockfile:
            doc = proto.read_json(lockfile)
            if isinstance(doc, dict) and doc.get('startedAt'):
                self.lock_started_at = doc['startedAt']
        self._nohost_warned = set()
        self.run_started_epoch = time.time()
        self.replay_grace = replay_grace
        self._seen_nonterminal = set()
        self._replay_logged = set()
        self.ensure_dispatcher_inbox()

    def adir(self, pid_):
        return proto.agent_dir(self.root, pid_)

    def read_spec(self, pid_):
        return proto.participant_spec(self.root, pid_)

    def read_execution(self, pid_):
        return proto.execution_state(self.root, pid_)

    def ensure_dispatcher_inbox(self):
        os.makedirs(proto.position_inbox(self.root), exist_ok=True)

    @staticmethod
    def _bare(tid):
        tid = str(tid)
        return tid

    def _pid_active(self, pid_, cache=None):
        if cache is not None and pid_ in cache:
            return cache[pid_]
        res = self._pid_active_uncached(pid_, cache)
        if cache is not None:
            cache[pid_] = res
        return res

    def _pid_active_uncached(self, pid_, cache=None):
        try:
            d = proto.agent_dir(self.root, pid_)
        except ValueError:
            return (False, 'id 文法非法')
        if not os.path.isdir(d):
            return (False, '目录不存在')
        doc = proto.execution_state(self.root, pid_)
        if isinstance(doc, dict) and doc.get('final') is not True:
            return (True, '')
        wdir = os.path.join(d, 'watcher')
        try:
            if any((not n.startswith('.') for n in os.listdir(wdir))):
                return (True, '')
        except OSError:
            pass
        if pid_ in self._subscribed_ids(cache):
            return (True, '')
        return (False, '目录在场但无消费者（serviced 执行缺失或已终态、无 watcher 订阅、无 spec.subscribes 声明者）')

    def _subscribed_ids(self, cache=None):
        if cache is not None and '*' in cache:
            return cache['*']
        out = set()
        for other in proto.list_participants(self.root):
            sp = self.read_spec(other)
            subs = (sp or {}).get('subscribes')
            if isinstance(subs, list):
                out |= set((x for x in subs if isinstance(x, str)))
        if cache is not None:
            cache['*'] = out
        return out

    def resolve_reaper(self, spec, cache=None):
        spec = spec or {}
        target = None
        raw = spec.get('reaper')
        if isinstance(raw, str) and raw.strip() == proto.NO_REAPER:
            return None
        if isinstance(raw, str) and raw.strip():
            cand = raw.strip()
            if proto.is_valid_participant_id(cand):
                target = cand
            else:
                log.warning('spec.reaper 文法非法（%r），回落职位信箱', raw)
        if target is None:
            return {'pid': NOTIFY_TO, 'inbox': proto.position_inbox(self.root), 'note': 'spec 缺 reaper 字段（或文法非法），回落职位信箱'}
        retired_note = None
        moved = proto.retired_move(target)
        if moved:
            retired_note = 'reaper %s 已退役（地址移族），按退役表改投 %s' % (target, moved)
            target = moved
        if target == NOTIFY_TO:
            return {'pid': NOTIFY_TO, 'inbox': proto.position_inbox(self.root), 'note': retired_note}
        alive, why = self._pid_active(target, cache)
        if alive:
            return {'pid': target, 'inbox': proto.inbox_path(self.root, target), 'note': retired_note}
        note = 'reaper %s 不存在，回落职位信箱' % target if why == '目录不存在' else 'reaper %s %s，回落职位信箱' % (target, why)
        if retired_note:
            note = retired_note + '；' + note
        return {'pid': NOTIFY_TO, 'inbox': proto.position_inbox(self.root), 'note': note}

    def _ask_inbox_env(self, spec):
        r = self.resolve_reaper(spec)
        if r is None:
            r = {'inbox': proto.position_inbox(self.root), 'note': 'spec.reaper=%s（无终态通知）⇒ ask 仍须有收件面，回落职位信箱' % proto.NO_REAPER}
        env = {'AGENTD_ASK_INBOX': r['inbox']}
        if r.get('note'):
            env['AGENTD_ASK_NOTE'] = str(r['note'])
        return env

    def notify_mark_path(self, pid_):
        return os.path.join(self.adir(pid_), NOTIFY_MARK)

    def _mark_doc(self, pid_):
        doc = proto.read_json(self.notify_mark_path(pid_))
        if not isinstance(doc, dict):
            return None
        raw = doc.get('to')
        if isinstance(raw, (list, tuple, set)):
            to = set((x for x in raw if isinstance(x, str) and x))
        else:
            to = set()
        out = dict(doc)
        out['to'] = to
        return out

    def _already_notified(self, pid_, to_pid):
        mark = self._mark_doc(pid_)
        return mark is not None and to_pid in mark['to']

    def stop_requests(self, pid_):
        return []

    def canceled(self, doc, stop_reqs):
        return doc.get('runtime', {}).get('phase') == 'canceled'

    def terminal_event(self, doc, pid_):
        if self.canceled(doc, []):
            return 'task_canceled'
        return 'task_done' if doc.get('exitcode') == 0 else 'task_failed'

    def replay_suppressed(self, pid_, doc):
        if pid_ in self._seen_nonterminal or self._bare(pid_) in self._seen_nonterminal:
            return False
        ended = ts_epoch(doc.get('endedAt'))
        age = None if ended is None else self.run_started_epoch - ended
        if age is not None and age <= self.replay_grace:
            return False
        if pid_ not in self._replay_logged:
            self._replay_logged.add(pid_)
            log.info('%s：抑制历史终态通知 %s（status=%s exitcode=%s endedAt=%s，早于本次启动 %s，超宽限窗 %.0fs；本次运行未见证其终态迁移）', REPLAY_GUARD_LOG, pid_, doc.get('status'), doc.get('exitcode'), doc.get('endedAt'), '不可解析' if age is None else '%.0fs' % age, self.replay_grace)
        return True

    def notify_tick(self):
        active = {}
        for pid_ in proto.list_participants(self.root):
            spec = self.read_spec(pid_)
            if spec is None or not proto.host_matches(spec, self.local_ids):
                continue
            doc = self.read_execution(pid_)
            if not proto.life_terminal(doc):
                continue
            mark = self._mark_doc(pid_)
            if mark is not None and mark.get('complete') is True:
                continue
            recipient = self.resolve_reaper(spec, active)
            if recipient is None:
                continue
            if self._already_notified(pid_, recipient['pid']):
                continue
            if self.replay_suppressed(pid_, doc):
                continue
            adir = self.adir(pid_)
            report = os.path.join(adir, 'report.md')
            event = self.terminal_event(doc, pid_)
            payload = {'event': event, 'taskId': pid_, 'dir': adir, 'status': doc.get('status'), 'exitcode': doc.get('exitcode'), 'name': spec.get('name') or spec.get('workdir') or ''}
            if os.path.exists(report):
                payload['report'] = report
            if recipient.get('note'):
                payload['note'] = recipient['note']
            stop_reqs = self.stop_requests(pid_)
            is_canceled = self.canceled(doc, stop_reqs)
            if event == 'task_done' and doc.get('exitcode') == 0 and ('report' not in payload) and (not is_canceled):
                payload['warn'] = 'no_report'
            for req in stop_reqs:
                if req.get('reason'):
                    payload['stopReason'] = req['reason']
                    break
            inbox = recipient['inbox']
            os.makedirs(inbox, exist_ok=True)
            for _ in range(8):
                mid = proto.now_ts() + '-agentd-' + proto.rand_suffix()
                mf = os.path.join(inbox, mid + '.msg')
                if not os.path.exists(mf):
                    break
            proto.atomic_write_json(mf, {'id': mid, 'from': 'agentd', 'ts': proto.now_ts(), 'type': 'inform', 'body': json.dumps(payload, ensure_ascii=False)})
            after = self._mark_doc(pid_)
            to = set(after['to']) if after else set()
            to.add(recipient['pid'])
            mark_doc = dict(after) if after else {}
            mark_doc['to'] = sorted(to)
            mark_doc['ts'] = proto.now_ts()
            mark_doc['event'] = event
            mark_doc['complete'] = True
            proto.atomic_write_json(self.notify_mark_path(pid_), mark_doc)
            log.info('notify %s -> %s%s (%s status=%s exitcode=%s)', pid_, recipient['pid'], '（%s）' % recipient['note'] if recipient.get('note') else '', event, doc.get('status'), doc.get('exitcode'))

    def run(self):
        log.info('root=%s hosts=%s interval=%s', self.root, sorted(self.local_ids), self.interval)
        while True:
            try:
                for pid_ in proto.list_participants(self.root):
                    try:
                        self.service(pid_)
                    except Exception:
                        log.exception('%s service error (retried next tick)', pid_)
                try:
                    self.notify_tick()
                except Exception:
                    log.exception('notify_tick error (retried next tick)')
                if self.lockfile and time.monotonic() >= self._lock_refresh_at:
                    try:
                        refresh_lock(self.lockfile, self.lock_host, self.lock_started_at)
                    except Exception:
                        log.exception('lock refresh error (retried next tick)')
                    self._lock_refresh_at = time.monotonic() + LOCK_REFRESH_INTERVAL
            except Exception:
                log.exception('loop error (retried next tick)')
            time.sleep(self.interval)
