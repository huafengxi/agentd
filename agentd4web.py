# -*- type=script -*-
# agentd 全链路任务报表 · 8080 实时端点（：用户 08-30 拍板
# 实时报表 API 统一用 type=script rpc 脚本风格，参考样板
# w/ext/shell/rpc/sh.py；原 w/core/live_api.py 前置
# handler 方案已删除）。
#
# 现算同目录 report.py（相对自身定位）：无参、取 stdout 作为响应体原样返回。
# 超时上限锁死 15s（环境钩仅可向下调）；超时/非零退出/启动失败降级为可读错误
# markdown（HTTP 200，不裸抛 500）。认证由 w 全局 BasicAuth（core/wsgi.py）继承。
# 消费方 dash.itab：tasks /agentd/agentd4web.py?v=text/md&refresh=15
#
# 测试钩（仅用于验证错误/超时路径，生产不设）：
#   AGENTD_REPORT_PATH     覆盖 report.py 路径（如指向不存在的文件试错误路径）
#   AGENTD_REPORT_TIMEOUT  超时（秒，仅可向下调，上限锁死 15s）
import subprocess

_TO_CAP = 15  # 超时上限（秒），锁死；环境钩仅可向下调
_STDERR_LIMIT = 2000


def _self_dir():
    # type=script 经 exec 执行（无 __file__）；server 进程 chdir 到 web 根
    # （~/m），故回退 = web 根下的 agentd/ 目录
    try:
        return os.path.dirname(os.path.realpath(__file__))
    except NameError:
        return os.path.realpath('agentd')


def _error_body(title, detail):
    return ('# agentd 报表生成失败（实时 API）\n\n> **%s**\n\n```\n%s\n```\n'
            % (title, detail))


def interp(store, **kw):
    path = os.getenv('AGENTD_REPORT_PATH') or os.path.join(_self_dir(), 'report.py')
    timeout = min(_TO_CAP, float(os.getenv('AGENTD_REPORT_TIMEOUT') or _TO_CAP))
    cmd = ['python3', path]
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
