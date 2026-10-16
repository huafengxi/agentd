# -*- type=script -*-
# Local script context (__file__) is supplied by the HTTP host.
# Run the adjacent report CLI with the host interpreter; render failures as Markdown.
# Optional test hooks: AGENTD_REPORT_PATH, AGENTD_REPORT_TIMEOUT (positive, at most 15s).
import logging
import math
import os
import sys
import subprocess

_TO_CAP = 15  # 超时上限（秒），锁死；环境钩仅可向下调
_STDERR_LIMIT = 2000


def _error_body(title, detail):
    return ('# agentd 报表生成失败（实时 API）\n\n> **%s**\n\n```\n%s\n```\n'
            % (title, detail))


def interp(store, **kw):
    path = os.getenv('AGENTD_REPORT_PATH') or os.path.join(os.path.dirname(os.path.realpath(__file__)), 'report.py')
    try:
        timeout = float(os.getenv('AGENTD_REPORT_TIMEOUT') or _TO_CAP)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('timeout must be finite and positive')
        timeout = min(_TO_CAP, timeout)
    except ValueError:
        return dict(type='text/plain'), _error_body('超时配置无效',
            'AGENTD_REPORT_TIMEOUT 须为正有限数（秒）')
    cmd = [sys.executable, path]
    try:
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=timeout)
    except subprocess.TimeoutExpired:
        body = _error_body('执行超时（上限 %gs）' % timeout, 'cmd: %s' % ' '.join(cmd))
    except OSError as e:
        body = _error_body('启动失败', '%r' % e)
    else:
        if r.returncode != 0:
            stderr = r.stderr.decode('utf-8', 'replace')
            if len(stderr) > _STDERR_LIMIT:
                stderr = stderr[:_STDERR_LIMIT] + '\n…(stderr 截断)'
            body = _error_body('非零退出: exitcode=%d' % r.returncode,
                               stderr or '(无 stderr)')
        else:
            body = r.stdout.decode('utf-8', 'replace')
    logging.info('agentd4web: %s -> %d bytes', ' '.join(cmd), len(body))
    return dict(type='text/plain'), body
