import argparse
import json
import os
import re
import socket
import sys
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from datetime import datetime, timedelta, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import proto
from registration import register_task

def parse_str_array(v, opt):
    try:
        arr = json.loads(v)
    except ValueError:
        die('%s 需为 JSON 字符串数组，如 \'["gpu"]\'：%r' % (opt, v))
    if not isinstance(arr, list) or not all((isinstance(x, str) for x in arr)):
        die('%s 元素必须全部为字符串：%r' % (opt, v))
    return arr

def local_canonical_host(root):
    hn = socket.gethostname()
    try:
        with open(os.path.join(root, 'env', 'host-id')) as f:
            for line in f:
                t = line.strip()
                if not t or t.startswith('#'):
                    continue
                parts = t.split()
                if parts[0] == hn and len(parts) > 1:
                    return (parts[1], False)
    except OSError:
        pass
    return (hn, True)

def reject_blank_value(raw, opt, harm, remedy):
    if raw is None:
        return None
    if not str(raw).strip():
        die('%s 的值为空/纯空白（%r）：%s。%s；本次未创建任何目录/文件。' % (opt, raw, harm, remedy), code=2)
    return raw

def reject_blank_root(raw):
    reject_blank_value(raw, '--root', harm='给了旗标却没给值 = 调用方 bug（典型 = 上游变量取值失败）。空值 ⛔ 静默降级——回落缺省根（本仓所在根 = 生产工作区根）会把探针/测试的对照臂直接写进生产 agents 树并被 scheduler enable+spawn，解析成 cwd下的路径则报错口径指错成因', remedy='要缺省根请**不传**该旗标')

def require_workspace_root(root):
    reject_blank_root(root)
    r = os.path.abspath(os.path.expanduser(root))
    if os.path.basename(os.path.realpath(r)) == 'agents':
        die('--root %r 不是工作区根：你传的是 agents 树本身（basename=agents）。--root 须为工作区根（如 <workspace-root>），不是 <workspace-root>/agents；本次未创建任何目录/文件。' % root, code=2)
    agents = os.path.join(r, 'agents')
    if not os.path.isdir(agents):
        die('--root %r 不是工作区根：%s 不在场。--root 须为工作区根（如 <workspace-root>），不是 <workspace-root>/agents；本次未创建任何目录/文件。' % (root, agents), code=2)
    if not os.path.exists(os.path.join(r, 'env', 'host-id')):
        print('agentctl: WARN env/host-id 不在场（root=%s）：--root 须为工作区根（工作区根含 agents/ 与 env/host-id），若不是请改正；登记照旧进行，登记机规范名回退本机 hostname' % r, file=sys.stderr)
    return r
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from platform_paths import workspace_root

def default_root():
    return workspace_root(__file__)

def portable_path(p):
    home = os.path.expanduser('~')
    if p == home:
        return '~'
    if p.startswith(home + os.sep):
        return '~' + p[len(home):]
    return p

def beijing_iso():
    return datetime.now(timezone(timedelta(hours=8))).replace(microsecond=0).isoformat()

def cmd_create(a):
    reject_blank_value(a.command, '--command', harm='spec.command 会被逐字写成空 ⇒ 拉起来的会话什么也不跑（∨ 根本起不来），而登记侧从 spec 读数上看不出它与正常登记的件有何差别', remedy='本旗标是必填项：要跑什么就给一条非空命令，⛔ 用空值占位')
    reject_blank_value(a.workdir, '--workdir', harm='空值会被 os.path.abspath(os.path.expanduser(v)) 解析成**调用方 cwd**（纯空白则解析成 <cwd>/空白 一类怪路径）⇒ runner 在一个调用方从没指定的目录里 spawn', remedy='本旗标是必填项：要给 cwd 就给一条非空路径，⛔ 用空值占位')
    reject_blank_value(a.creator, '--creator', harm='spec.creator 是审计面唯一记录「谁登记的」的字段 ⇒ 写成空白即查不出登记方；同一枚字段在 `bot register` 侧已硬拒 ⇒ 两侧口径不一会让事后审计对不上登记方', remedy='本旗标是必填项：给出真实的登记方身份（两段路径式 id ∨ 任务名），⛔ 用空值占位')
    reject_blank_value(a.name, '--name', harm='空串会被当「没给旗标」⇒ 改用随机 id（调用方拿不到自己要的名字，而任务书正文里内嵌的 taskId 与目录名就此对不上）；纯空白则建出一枚名为空白的参与方目录（扫描面与寻址面都认不出它）', remedy='要自动生成的随机 id 请**不传**该旗标（缺省档 = proto.auto_name()）')
    reject_blank_value(a.restart_policy, '--restart-policy', harm='空串与**不传**本旗标在 `if a.restart_policy:` 上同值 ⇒ spec 不写 restartPolicy 键、只落一行 WARN ⇒ 调用方「显式给了策略」的意图被静默丢弃，而登记侧读数与「没传」完全同值（缺省档那行 WARN 还会把成因说成调用方没声明）', remedy='要缺省（= 不写该键 ⇒ 崩溃不自愈）请**不传**本旗标；要给策略就给 manual ∕ auto ∕ one-shot 之一。choices 里那枚空串不是哨兵、只是 default 的产物，⛔ 拿它表达「不要该键」')
    for opt, raw, key in (('--host', a.host, 'host'), ('--created-by-host', a.created_by_host, 'createdByHost')):
        reject_blank_value(raw, opt, harm='空串会被 `or canonical` 回落链静默换成登记机规范名（与**不传**该旗标同值 ⇒ 调用方指定的路由/审计目标被丢弃，登记侧读数看不出差异）；纯空白非假值 ⇒ 被逐字写进 spec.%s ⇒ 执行节点无效（该件永久排队）+ 审计面失真' % key, remedy='要缺省（= 登记机规范名，解析链见 local_canonical_host）请**不传**该旗标；要指定他机就给一个非空的规范名')
    reaper = check_reaper(getattr(a, 'reaper', None))
    description = check_description(getattr(a, 'description', None))
    profile = check_profile(getattr(a, 'profile', None), a.root)
    command = apply_profile_prefix(a.command, profile)
    prompt_text = read_prompt_file(getattr(a, 'prompt_file', None))
    sched = [(key, parse_str_array(getattr(a, opt), '--' + opt)) for opt, key in (('resources', 'resources'), ('needs', 'needs')) if getattr(a, opt) is not None]
    if a.name:
        if '/' in a.name or a.name.startswith('.'):
            die('非法目录名：%r（作为路径组件合法即可，§2.2）' % a.name)
        if set('*?[]') & set(a.name):
            die('非法文件名：%r 含 glob 元字符 *?[]（属组闸门文件名禁用约定，@agentd#sync-channel）' % a.name)
        name = a.name
        adir = proto.task_dir(a.root, name)
    else:
        name = proto.auto_name()
        adir = proto.task_dir(a.root, name)
    for cand in (adir, proto.task_dir(a.root, name), proto.bot_dir(a.root, name)):
        if os.path.exists(cand):
            die('拒绝创建：%s 已存在' % cand, code=2)
    spec = {'command': command, 'workdir': portable_path(os.path.abspath(os.path.expanduser(a.workdir))), 'creator': a.creator}
    if getattr(a, 'resident', False):
        spec['resident'] = True
    if a.restart_policy:
        if a.restart_policy not in ('manual', 'auto', 'one-shot'):
            die('restartPolicy 取值须为 manual|auto|one-shot')
        spec['restartPolicy'] = a.restart_policy
    if description:
        spec['name'] = description
    if reaper:
        spec['reaper'] = reaper
    warn_unwritten_spec_keys(reaper, a.restart_policy)
    warn_missing_profile(command, a.root)
    spec['createdAt'] = beijing_iso()
    canonical, fallback = local_canonical_host(a.root)
    if fallback:
        print('warning: env/host-id 缺失或无本机（%s）映射条目，登记机规范名回退为本机 hostname：%s（请到 env/host-id 补一行「%s <规范名>」；登记正常进行）' % (socket.gethostname(), canonical, socket.gethostname()), file=sys.stderr)
    host = a.host or canonical
    created_by = a.created_by_host or canonical
    if not host or not created_by:
        hn = socket.gethostname()
        host = host or hn
        created_by = created_by or hn
        print('warning: host 解析链产出空值，最终兜底写 hostname：%r' % hn, file=sys.stderr)
    if not host:
        host = 'unknown'
    if not created_by:
        created_by = host
    spec['host'] = host
    spec['createdByHost'] = created_by
    for key, value in sched:
        spec[key] = value
    try:
        register_task(a.root, name, spec, prompt_text)
    except ValueError as error:
        die(str(error), code=2)
    print(name)

def cmd_register_json(a):
    try:
        request = json.load(sys.stdin)
        if not isinstance(request, dict) or set(request) != {'name', 'spec', 'prompt'}:
            raise ValueError('请求字段须为 name、spec、prompt')
        if not isinstance(request['prompt'], str) or not request['prompt'].strip():
            raise ValueError('prompt 必须为非空字符串')
        result = register_task(a.root, request['name'], request['spec'], request['prompt'])
    except (ValueError, TypeError, UnicodeError) as error:
        die(str(error), code=2)
    print(json.dumps(result, ensure_ascii=False))

def cmd_create_bot(a):
    check_name_segment(a.name, 'bot 名')
    adir = proto.bot_dir(a.root, a.name)
    for cand in (adir, proto.task_dir(a.root, a.name)):
        if os.path.exists(cand):
            die('拒绝创建：%s 已存在' % cand, code=2)
    os.makedirs(os.path.join(adir, 'inbox'), exist_ok=True)
    print(a.name)

def check_name_segment(name, what='名字'):
    if not name or not proto.is_valid_name_segment(name):
        die("非法 %s：%r（白名单 [A-Za-z0-9._-]+，禁止空/'.'/'..'，§2.1）" % (what, name))
    if set('*?[]') & set(name):
        die('非法 %s：%r 含 glob 元字符 *?[]（属组闸门文件名禁用约定，@agentd#sync-channel）' % (what, name))
    if name in proto.LAYOUT_DIRS:
        die('保留名：%r 是布局容器目录名' % name, code=2)
    if name.startswith('.') or '..' in name:
        die("非法 %s：%r——合法命名 = [A-Za-z0-9._-]+ 且不以 '.' 开头、不含连续点 '..'（'.' 开头被 agents/ 扫描面当隐藏条目跳过；名内 '..' 使 ack 命名空间段不安全 → topic 绑定面不启用；§2.1/§2.2）" % (what, name))
    return name

def parse_subscribes(raw):
    items = [x.strip() for x in (raw or '').split(',')]
    out = []
    for s in items:
        if not s:
            continue
        if not proto.is_valid_participant_id(s):
            die('非法订阅项：%r——须为两段路径式 id，name ∈ [A-Za-z0-9._-]+' % s)
        family, _name = proto.parse_participant_id(s)
        if family != proto.TOPIC_DIR:
            die('拒绝订阅 %r：只有 topic/ 族信箱可被绑定（通道 B 白名单，防抢收——task/bot 信箱收件归属 = 会话名的纯函数，§4.1）' % s)
        if s not in out:
            out.append(s)
    return out

def cmd_topic_init(a):
    check_name_segment(a.id, 'topic id')
    reject_blank_value(a.title, '--title', harm='空串会被 `a.title or a.id` 的假值回落并进「没传旗标」那一档（标题静默回落成 id）；纯空白非假值 ⇒ 被逐字写进策展文档 topic.md 的一级标题行（`#    ` 一类）⇒ 标题面失真，而寻址面与调度面都看不出差异', remedy='要缺省标题（= id）请**不传**本旗标；要给标题就给非空文本')
    tdir = proto.topic_dir(a.root, a.id)
    for cand in (tdir, proto.bot_dir(a.root, a.id), proto.task_dir(a.root, a.id)):
        if os.path.exists(cand):
            die('拒绝创建：%s 已存在（topic 不覆盖既有目录；删除走 agents-sync/gc.py 通道）' % cand, code=2)
    watchers = []
    for w in a.watcher or []:
        check_name_segment(w, 'watcher 会话裸名')
        if w not in watchers:
            watchers.append(w)
    os.makedirs(os.path.join(tdir, 'inbox'), exist_ok=True)
    wdir = os.path.join(tdir, 'watcher')
    os.makedirs(wdir, exist_ok=True)
    proto.atomic_write(os.path.join(tdir, 'topic.md'), topic_md_skeleton(a.id, a.title or a.id))
    for w in watchers:
        proto.atomic_write(os.path.join(wdir, w), '# 订阅声明（通道 A）：%s 会话 receiver 直订本主题信箱（agentctl topic init 登记）。删除本条目 = 即时退订。\n' % w)
    print('topic/%s' % a.id)
    print('  dir      %s' % tdir)
    print('  layout   topic.md + inbox/ + watcher/' + ('（订阅条目：%s）' % ', '.join(watchers) if watchers else ''))
    print('  下一步   策展 topic.md（单写者=owner/主持人）；登记主持人 bot 用 `agentctl bot register`；删除走 `agents-sync/gc.py add topic/%s/`' % a.id)

def topic_md_skeleton(tid, title):
    return '---\nwhen: "（占位：何时该来读本主题文档——一句话；入册后由 bots/kb_index.py 聚合进知识清单）"\n---\n\n# {title}\n\n> topic = `{tid}`（协作容器：本文策展 + `inbox/` 全量日志 + `watcher/` 订阅注册表）。\n> **单写者** = owner/主持人；日志由机制自动过账（`agentctl send` 投递即落盘），绝不手工双写。\n> 机制权威：`dispatch/docs/topics.md`（§5.1 主持人载体、§7 双件套）；消息与收件口径：`dispatch/DISPATCH.md` §4/§8。\n> 删除/归档本主题一律走 `agents-sync/gc.py add topic/{tid}/` 通道（铁律：不得直接 rm）。\n\n## 议题\n\n（要决什么、判据是什么、预算多少轮；议题漂移时显式改写并记原因与时间）\n\n## 已决\n\n（决策 + 时间 + 依据；检查点式追加）\n\n## 未决\n\n（待决项 / 待用户或调度员拍板 / 行动清单：谁、做什么、何时）\n'.format(title=title, tid=tid)

def check_reaper(raw, what='reaper'):
    if raw is None:
        return None
    reject_blank_value(raw, '--reaper', harm='空值会被假值判定并进「没给旗标」那一档 ⇒ spec 不写 reaper 键 ⇒ 终态通知回落职位信箱 %s 并带 note（调用方指定的收尾方被静默丢弃），而缺省档那行 WARN 说的是「未传 --reaper」⇒ 登记侧读数与报文都指错成因' % proto.POSITION_PID, remedy='要指定收尾方就给一个两段路径式 id（如 bot/<名> ∨ task/<id> ∨ topic/<id>）∨ 哨兵 %r（= 不发终态通知）；要走缺省回落请**不传**该旗标' % proto.NO_REAPER)
    s = str(raw).strip()
    if not proto.is_reaper_value(s):
        die('非法 %s：%r——须为两段路径式 <family>/<name>（family ∈ {%s}，name ∈ [A-Za-z0-9._-]+，禁 ./../空段、裸名）∨ 哨兵 %r（= 不发终态通知）；§2.1/§4.1' % (what, raw, ', '.join(proto.FAMILIES), proto.NO_REAPER))
    return s

def check_description(raw, what='description'):
    if raw is None:
        return None
    s = str(raw)
    reject_blank_value(raw, '--description', harm='空串 ∪ 纯空白都会被 `if not s.strip(): return None` 的假值回落并进「没传旗标」那一档 ⇒ spec 不写 name 键、调用方给的描述被静默丢弃，而登记侧读数与「没传」完全同值（缺省档也没有任何 WARN ∕ 报文提示）', remedy='要缺省（= 不写该键）请**不传**本旗标；要给描述就给非空文本')
    if '\x00' in s:
        die('非法 %s：含 NUL 字节，不可落 JSON 档案' % what)
    return s

def warn_unwritten_spec_keys(reaper, restart_policy):
    if not reaper:
        print('WARN: 本次调用未传 --reaper ⇒ spec 不写该键 ⇒ 终态通知回落职位信箱 %s 并带 note；要指定收尾方就带 --reaper <两段路径式 id>（如 bot/<名> ∨ task/<id> ∨ topic/<id>）∨ 哨兵 %r（= 不发终态通知）；传空值/纯空白会被拒（rc=2、零落盘）' % (proto.POSITION_PID, proto.NO_REAPER), file=sys.stderr)
    if not restart_policy:
        print('INFO: 未指定监督策略，按一次执行发布', file=sys.stderr)
PROFILE_ENV_KEY = 'DISPATCH_PROFILE'
PROFILES_REL = 'bots/profiles'
PROFILE_PREFIX_RE = re.compile('^\\s*%s=(\\S+)\\s' % PROFILE_ENV_KEY)

def list_profiles(root):
    try:
        entries = os.listdir(os.path.join(root, PROFILES_REL))
    except OSError:
        return []
    return sorted((f[:-len('.json')] for f in entries if f.endswith('.json') and f != '.json'))

def check_profile(raw, root):
    if raw is None:
        return None
    reject_blank_value(raw, '--profile', harm='空值会被当「没声明人格」⇒ spec.command 不拼 %s= 前缀，会话静默跑在最基线人格上（该装载的 caps 全没装载）；而缺省档那一行 WARN 照打 ⇒ 产物与报文与**不传**该旗标逐字同款，登记侧看不出调用方本意是要装载人格' % PROFILE_ENV_KEY, remedy='要装载人格就给一个命中白名单的 profile 名；要基线档请**不传**该旗标（缺省档 = 一行 WARN 且照建）')
    s = str(raw).strip()
    pdir = os.path.join(root, PROFILES_REL)
    avail = list_profiles(root)
    if not avail:
        die('无法校验 --profile %r：白名单枚举根 %s 不在场 ∨ 其中无 *.json（白名单是现场枚举、不写死名单；若这棵树不是工作区根请改正 --root）。本次未创建任何目录/文件。' % (raw, pdir), code=2)
    if s not in avail:
        die('非法 --profile：%r——白名单 = 现场枚举 %s/*.json 的基名，可选：%s。本次未创建任何目录/文件。' % (raw, pdir, ', '.join(avail)), code=2)
    return s

def profile_prefix_of(command):
    m = PROFILE_PREFIX_RE.match(command or '')
    return m.group(1) if m else None

def apply_profile_prefix(command, profile):
    if not profile:
        return command
    have = profile_prefix_of(command)
    if have is None:
        return '%s=%s %s' % (PROFILE_ENV_KEY, profile, command)
    if have != profile:
        die('--profile %r 与 --command 里既有的前缀 %s=%s 冲突（两侧值都在此，⛔ 静默覆盖）：人格声明只留一处——要么去掉 --profile、要么把前缀从 --command 里删掉。本次未创建任何目录/文件。' % (profile, PROFILE_ENV_KEY, have), code=2)
    return command

def warn_missing_profile(command, root):
    if profile_prefix_of(command) is not None:
        return
    avail = list_profiles(root)
    print('WARN: 未指定 --profile 且 --command 无 %s= 前缀 ⇒ spec.command 不带人格声明 ⇒ 该会话按运行时的缺省档回落（未设 %s 时回落哪个 profile 由人格解析层决定，本仓不复制该判据），能力/工具面即最基线档；需要更强 ∨ 更受限的工具面（如只读强制）必须显式 --profile <名>。登记照建。可选名单（现场枚举 <root>/%s/*.json）：%s' % (PROFILE_ENV_KEY, PROFILE_ENV_KEY, PROFILES_REL, ', '.join(avail) if avail else '（空：%s 不在场 ∨ 其中无 *.json）' % os.path.join(root, PROFILES_REL)), file=sys.stderr)
PROMPT_FILE_NAME = 'prompt.md'

def read_prompt_file(raw):
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        die('--prompt-file 的值为空/纯空白（%r）⇒ 拒绝创建：调用方以为给了任务书，实际会建出无 %s 的任务（会话起来无事可做却照样报完成）。正确做法 = 给一个可读的任务书路径 ∨ 不给本旗标（缺省档不写该文件）。本次未创建任何目录/文件。' % (raw, PROMPT_FILE_NAME), code=2)
    p = os.path.abspath(os.path.expanduser(s))
    try:
        with open(p, 'r', encoding='utf-8') as f:
            txt = f.read()
    except (OSError, UnicodeDecodeError) as e:
        die('--prompt-file %r 不可读（%r）⇒ 拒绝创建：登记与任务书必须同时落盘，⛔ 建了目录却没 prompt。本次未创建任何目录/文件。' % (raw, e), code=2)
    if not txt.strip():
        die('--prompt-file %r 内容为空（零字节 ∨ 纯空白）⇒ 拒绝创建：空任务书与没任务书是同一失效形态（会话起来无事可做却照样报完成）。本次未创建任何目录/文件。' % raw, code=2)
    return txt

def require_bot_spec_trio(a):
    for opt, raw, harm in (('--command', a.command, 'spec.command 会被逐字写成空白 ⇒ 拉起来的会话什么也不跑（∨ 根本起不来），而登记侧从 spec 读数上看不出它与正常登记的 bot 有何差别'), ('--workdir', a.workdir, '空值会被 os.path.abspath(os.path.expanduser(v)) 解析成**调用方 cwd**（纯空白则解析成 <cwd>/空白 一类怪路径）⇒ runner 在一个调用方从没指定的目录里 spawn'), ('--creator', a.creator, 'spec.creator 是审计面唯一记录「谁登记的」的字段 ⇒ 写成空白即查不出登记方')):
        reject_blank_value(raw, opt, harm=harm, remedy='本旗标是新建 spec 的必备项：要给就给非空值，⛔ 用空值占位。只要信箱用 `create-bot`；守护型 bot 走被追踪声明源 bots/daemon/<名字>/spec.json + `make bots.seed`；只想改订阅而 spec 应在场 = 先核对路径 --root')
    if not a.command or not a.workdir or (not a.creator):
        die('bot/%s 无 spec.json：新建进程型 bot spec 需 --command/--workdir/--creator 三件（只要信箱用 `create-bot`；守护型 bot 走被追踪声明源 bots/daemon/<名字>/spec.json + `make bots.seed`；只想改订阅而 spec 应在场 = 先核对路径 --root）' % a.name, code=2)

def cmd_bot_register(a):
    check_name_segment(a.name, 'bot 名')
    tdir_same = proto.task_dir(a.root, a.name)
    if os.path.exists(tdir_same):
        die('拒绝创建：%s 已存在（同名跨布局撞名，§2.2）' % tdir_same, code=2)
    subs = parse_subscribes(a.subscribes) if a.subscribes is not None else None
    reaper = check_reaper(getattr(a, 'reaper', None))
    description = check_description(getattr(a, 'description', None))
    reject_blank_value(a.restart_policy, '--restart-policy', harm='空串与**不传**本旗标在 `if a.restart_policy:` 上同值 ⇒ spec 不写 restartPolicy 键、只落一行 WARN ⇒ 调用方「显式给了策略」的意图被静默丢弃，而登记侧读数与「没传」完全同值（缺省档那行 WARN 还会把成因说成调用方没声明）', remedy='要缺省（= 不写该键 ⇒ 崩溃不自愈）请**不传**本旗标；要给策略就给 manual ∕ auto ∕ one-shot 之一。choices 里那枚空串不是哨兵、只是 default 的产物，⛔ 拿它表达「不要该键」')
    for opt, raw, key in (('--host', a.host, 'host'), ('--created-by-host', a.created_by_host, 'createdByHost')):
        reject_blank_value(raw, opt, harm='空串会被 `or canonical` 回落链静默换成登记机规范名（与**不传**该旗标同值 ⇒ 调用方指定的路由/审计目标被丢弃，登记侧读数看不出差异）；纯空白非假值 ⇒ 被逐字写进 spec.%s ⇒ 执行节点无效（该件永久排队）+ 审计面失真' % key, remedy='要缺省（= 登记机规范名，解析链见 local_canonical_host）请**不传**该旗标；要指定他机就给一个非空的规范名')
    bdir = proto.bot_dir(a.root, a.name)
    spath = os.path.join(bdir, 'spec.json')
    if not os.path.exists(spath):
        require_bot_spec_trio(a)
    if os.path.exists(spath) and getattr(a, 'resident', False):
        die('spec.json 已在场：--resident 仅用于新建声明（常驻属性不可变）', code=2)
    os.makedirs(os.path.join(bdir, 'inbox'), exist_ok=True)
    if os.path.exists(spath):
        if reaper or description:
            print('warning: spec.json 已在场 ⇒ --description/--reaper 不生效（本命令只改 subscribes；改这两字段 = 人工编辑 %s 后 `agentctl control restart bot/%s`）' % (spath, a.name), file=sys.stderr)
        if subs is None:
            die("bot/%s 的 spec.json 已在场：本命令只改 subscribes 字段（其余字段不可变），请传 --subscribes（清空传 --subscribes ''）" % a.name, code=2)
        doc = proto.read_json(spath)
        if not isinstance(doc, dict):
            die('拒绝改写 %s：不可读/半截/非 JSON 对象（人工核对后再登记）' % spath, code=2)
        if subs:
            doc['subscribes'] = subs
        else:
            doc.pop('subscribes', None)
        proto.atomic_write_json(spath, doc)
        print('bot/%s' % a.name)
        print('  spec     %s（只改 subscribes，其余字段保留）' % spath)
        print('  subscribes %s' % (json.dumps(subs, ensure_ascii=False) if subs else '（已清空 = 字段移除）'))
        return
    spec = {'command': a.command, 'workdir': portable_path(os.path.abspath(os.path.expanduser(a.workdir))), 'creator': a.creator}
    if getattr(a, 'resident', False):
        spec['resident'] = True
    if a.restart_policy:
        if a.restart_policy not in ('manual', 'auto', 'one-shot'):
            die('restartPolicy 取值须为 manual|auto|one-shot')
        spec['restartPolicy'] = a.restart_policy
    canonical, fallback = local_canonical_host(a.root)
    if fallback:
        print('warning: env/host-id 缺失或无本机（%s）映射条目，登记机规范名回退为本机 hostname：%s' % (socket.gethostname(), canonical), file=sys.stderr)
    spec['host'] = a.host or canonical or socket.gethostname() or 'unknown'
    spec['createdByHost'] = a.created_by_host or canonical or spec['host']
    if subs:
        spec['subscribes'] = subs
    if description:
        spec['name'] = description
    if reaper:
        spec['reaper'] = reaper
    warn_unwritten_spec_keys(reaper, a.restart_policy)
    proto.atomic_write_json(spath, spec)
    from execution import submit
    submit(a.root, 'bot/' + a.name, spec)
    print('bot/%s' % a.name)
    print('  spec     %s（新建）' % spath)
    print('  subscribes %s' % (json.dumps(subs, ensure_ascii=False) if subs else '（未声明）'))
    print('  name     %s' % (description if description else '（未声明）'))
    print('  reaper   %s' % (reaper if reaper else '（未声明 → 终态通知回落职位信箱 %s）' % proto.POSITION_PID))
    print('  serviced 执行声明已发布：bot/%s' % a.name)

def require_pid(s, what='参与方'):
    if not proto.is_valid_participant_id(s):
        die('%s id 非法：%r——须为两段路径式 <family>/<name>，family ∈ {%s}，name ∈ [A-Za-z0-9._-]+（禁 ./../空段、裸名）' % (what, s, ', '.join(proto.FAMILIES)))
    return s

def resolve_sender(explicit, what='发送方'):
    if explicit is not None:
        reject_blank_value(explicit, '--from', harm='空值会被当「没给旗标」⇒ 发件归属静默回落到环境 AGENT_SELF ∨ 职位信箱 %s（收件方按错误的发件方判归属，审计与权威归属双失真）' % proto.POSITION_PID, remedy='要声明发件方就给一个两段路径式 id（如 task/<id> ∨ bot/<名>）；要走缺省归属请**不传**该旗标（缺省档 = 环境 AGENT_SELF，再缺省回落职位信箱）')
        s = str(explicit).strip()
        if not proto.is_valid_participant_id(s):
            die('%s身份（--from）非法：%r——须为两段路径式 <family>/<name>，family ∈ {%s}（§2.2；不接受裸名）；缺省时取环境 AGENT_SELF，再缺省回落职位信箱 %s' % (what, explicit, ', '.join(proto.FAMILIES), proto.POSITION_PID))
        return s
    env_self = os.environ.get('AGENT_SELF')
    s = env_self.strip() if isinstance(env_self, str) else ''
    if s and proto.is_valid_participant_id(s):
        return s
    return proto.POSITION_PID

def require_not_retired(s, what='参与方'):
    moved = proto.retired_move(s)
    if moved:
        die('%s %s 已退役（地址移族）——请改用 %s。本次未写任何文件/目录。' % (what, s, moved), code=2)
    return s

def body_of(a):
    raw, fp = (getattr(a, 'body', None), getattr(a, 'body_file', None))
    if raw is not None and fp is not None:
        die('--body 与 --body-file 二选一（同时给 = 歧义，不猜优先级）')
    if fp is not None:
        try:
            raw = sys.stdin.read() if fp == '-' else open(fp, encoding='utf-8').read()
        except OSError as e:
            die('--body-file 读取失败：%s' % e)
    if raw is None:
        die('缺正文：--body <文本> ∨ --body-file <路径|->（`-` = stdin）')
    body = raw.strip()
    if not body:
        die('正文为空（trim 后）：拒绝投递，本次未写任何文件（§4.5 body 必填）')
    return body

def cmd_send(a):
    require_pid(a.to, '收件方')
    require_not_retired(a.to, '收件方')
    body = body_of(a)
    if a.type == 'reply' and (not (a.ref or '').strip()):
        die('type=reply 必须带 --ref（无主答复无效，§6.2）；纯空白同样无效——空白 ref 会写出关联不到任何 ask 的孤儿答复（提问方永远等不到）')
    if a.ref is not None and (not a.ref.strip()):
        die('非 reply 档传空白 --ref 同样无效（%r）：空串会被写点的假值判定吞掉（= 与不传同档、关联意图静默丢弃），纯空白则被逐字写进信封的 ref 键 ⇒ 收件方读到一枚关联不到任何 ask 的 ref（孤儿关联）；不需要关联就**不传**本旗标' % a.ref)
    sender = resolve_sender(a.sender, '发送方')
    env = {'from': sender, 'ts': proto.now_ts(), 'type': a.type, 'body': body}
    if a.ref:
        env['ref'] = a.ref
    if a.deliver:
        env['deliver'] = a.deliver
    if a.type == 'ask':
        env['via'] = proto.ASK_VIA_SEND_MESSAGE
    mid, _p = proto.write_message(proto.inbox_path(a.root, a.to), env)
    print(mid)

def cmd_ack(a):
    require_pid(a.participant)
    require_not_retired(a.participant)
    p = os.path.join(proto.inbox_path(a.root, a.participant), 'ack', a.msgid)
    if proto.exclusive_write_json(p, {'id': a.msgid, 'ts': proto.now_ts()}):
        print('acked')
        return
    print('already-acked')

def cmd_control(a):
    from execution import desired, facts, service_action
    require_pid(a.participant)
    if a.action == 'clear':
        from execution import clear_service_session
        print(json.dumps(clear_service_session(a.root,a.participant)));return
    if a.inject:
        die('Use inbox messages to inject context')
    declaration, _, _ = facts(a.root, a.participant)
    if declaration is None:
        die('No serviced execution object')
    if a.action != 'stop' and declaration['kind'] != 'Service':
        die('A Task is immutable; create a new participant to run again')
    if declaration['kind'] == 'Service':
        service_action(a.root, a.participant, 'stop')
        if a.action == 'clear':
            die('Clear sessions after stopping on their owning node')
        if a.action == 'restart':
            service_action(a.root, a.participant, 'start')
    else:
        desired(a.root, a.participant, 'cancel')
    print('requested: ' + a.action)

def require_live_participant(root, pid_, verb):
    spec = proto.read_json(proto.spec_path(root, pid_))
    if spec is None:
        die('%s 失败：参与方不存在（无 spec.json）：%s' % (verb, pid_))
    pid_doc = proto.execution_state(root, pid_)
    if proto.life_terminal(pid_doc):
        die('%s 失败：%s 已生命周期终态（final，status=%s）' % (verb, pid_, (pid_doc or {}).get('status', '?')), code=2)
    return spec

def cmd_answer(a):
    require_pid(a.participant, '目标任务')
    require_not_retired(a.participant, '目标任务')
    body = body_of(a)
    require_live_participant(a.root, a.participant, 'answer')
    ask = proto.find_pending_ask(a.root, a.participant)
    if ask is None:
        die('answer 失败：%s 没有未答的 ask（无需答复；反问由对方会话发起）' % a.participant)
    env = {'from': resolve_sender(a.sender, '答复方'), 'ts': proto.now_ts(), 'type': 'reply', 'ref': ask.get('id'), 'body': body}
    if ask.get('via'):
        env['via'] = ask['via']
    if a.deliver:
        env['deliver'] = a.deliver
    mid, _p = proto.write_message(proto.inbox_path(a.root, a.participant), env)
    q = ' '.join(str(proto.ask_summary(ask) or '').split())
    if len(q) > 120:
        q = q[:120] + '…'
    print(mid)
    print('  askId=%s' % ask.get('id'))
    print('  blocking=%s' % ('no' if ask.get('via') else 'yes'))
    print('  via=%s' % (ask.get('via') or '-'))
    print('  question=%s' % q)

def cmd_cancel(a):
    require_pid(a.participant)
    from execution import desired
    print(json.dumps(desired(a.root, a.participant, 'cancel'), ensure_ascii=False))

def cmd_status(a):
    require_pid(a.participant)
    doc = proto.execution_state(a.root, a.participant)
    if doc is None:
        print('participant=%s\nstate=no-execution' % a.participant)
        return
    gt = proto.gen_terminal(doc)
    lt = proto.life_terminal(doc)
    print('participant=%s' % a.participant)
    for k in ('gen', 'pid', 'procStart', 'status', 'exitcode', 'startedAt', 'endedAt', 'lastAliveAt', 'resumed', 'restarts', 'final'):
        if k in doc:
            print('%s=%s' % (k, doc[k]))
    print('代终态=%s\u3000生命周期终态=%s' % ('真' if gt else '假', '真' if lt else '假'))

def cmd_list(a):
    for pid_ in proto.list_participants(a.root):
        doc = proto.execution_state(a.root, pid_)
        if doc is None:
            s = 'queued' if os.path.exists(proto.spec_path(a.root, pid_)) else 'inbox-only'
        else:
            s = doc.get('status', '?')
            if doc.get('final'):
                s += ',final'
            s += ',gen%s' % doc.get('gen')
        print('%-40s %s' % (pid_, s))

def die(msg, code=1):
    print('agentctl: ' + msg, file=sys.stderr)
    sys.exit(code)

def cmd_submit(a):
    require_pid(a.participant)
    spec = proto.read_json(proto.spec_path(a.root, a.participant))
    if spec is None:
        die('spec.json 不可读')
    from execution import submit
    d = submit(a.root, a.participant, spec)
    if d is None:
        die('本工作区未配置 serviced 执行入口')
    print(json.dumps(d, ensure_ascii=False))

def main():
    ap = argparse.ArgumentParser(prog='agentctl')
    ap.add_argument('--root', default=None, help='工作区根（如 <workspace-root>），不是 <workspace-root>/agents：值为空/纯空白 ∨ basename 为 agents ∨ <root>/agents 不在场即拒绝（rc=2、不静默建目录）；<root>/env/host-id 不在场只告警（登记不失败）。缺省 = 本脚本所在仓的父目录（现场发现，不依赖 cwd/env）；要缺省根请**不传**本旗标 —— 传空值 ⛔ 回落缺省根（= 静默写生产树）')
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('register-json', help='从 stdin 接收 {name,spec,prompt}，校验并发布任务；stdout 为 JSON')
    p.set_defaults(fn=cmd_register_json)
    p = sub.add_parser('create', help='写 spec.json 创建参与方')
    p.add_argument('--name', help='自定义目录名；缺省自动生成（§2.2）。值为空/纯空白即拒（rc=2、零落盘）：要随机 id 请**不传**本旗标')
    p.add_argument('--command', required=True, help='启动命令（必填）。值为空/纯空白即拒（rc=2、零落盘）：空值会被逐字写进 spec.command ⇒ 会话什么也不跑而登记侧看不出来')
    p.add_argument('--workdir', required=True, help='--workdir；执行声明与状态以 serviced 为准')
    p.add_argument('--creator', required=True, help='登记方（必填；写入 spec.creator 审计字段）。值为空/纯空白即拒（rc=2、零落盘）：空白值会写进审计字段 ⇒ 查不出登记方')
    p.add_argument('--description', default=None, help='写入 spec `name` 字段（人类可读描述，§4.1）；缺省（= **不传**本旗标）不写该键。值为空/纯空白即拒（rc=2、零落盘）：两形态都会被假值回落并进「没传旗标」那一档 ⇒ 描述被静默丢弃而登记侧看不出差异')
    p.add_argument('--reaper', default=None, help='写入 spec `reaper` 字段（终态通知唯一收件面，§4.1）：两段路径式 <family>/<name>，family ∈ {%s}；∨ 哨兵 `%s`（= 无收件方、不发终态通知）；非法即拒。缺省（= **不传**本旗标）不写该键 → 运行时回落职位信箱 %s 带 note。值为空/纯空白即拒（rc=2、零落盘）：空值会被并进「没给旗标」那一档 ⇒ 指定的收尾方被静默丢弃，要缺省回落请**不传**本旗标' % (', '.join(proto.FAMILIES), proto.NO_REAPER, proto.POSITION_PID))
    p.add_argument('--resident', action='store_true', help='声明常驻监督（spec.resident=true；与 profile.form 无关）')
    p.add_argument('--restart-policy', choices=['', 'manual', 'auto', 'one-shot'], default=None, help='Service 监督策略：manual、auto 或 one-shot')
    p.add_argument('--profile', default=None, help='人格 profile 名（可选）。**白名单 = 现场枚举** <root>/%s/*.json 的基名（⛔ 不写死名单；枚举根按 --root 解析，口径同 env/host-id）；**非法即拒**（rc=2、stderr 打印可选名单、零落盘），只核名字命中清单、不核清单的 form 字段。**落地 = 把 `%s=<名> ` 前缀拼进 spec.command**（人格装载的唯一载入口；本 CLI ⛔ 不给 spec 加键、⛔ 不动调度语义）：与 --command 里既有的同键前缀**同值 ⇒ 幂等不重复拼**、**异值 ⇒ 拒**（打印两侧值，⛔ 静默覆盖）。缺省（且 command 无前缀）⇒ 打一行 WARN（会话按运行时缺省档回落、工具面为最基线档）但**照建**；**值为空/纯空白即拒**（rc=2、零落盘）：空值会静默降到基线人格，且产物与**不传**本旗标逐字同款 ⇒ 要基线档请**不传**本旗标' % (PROFILES_REL, PROFILE_ENV_KEY))
    p.add_argument('--host', help='执行节点规范名')
    p.add_argument('--created-by-host', help='登记机规范名（缺省 = 登记机）。值为空/纯空白即拒（rc=2、零落盘，口径同 --host：空白值会逐字写进 spec.createdByHost ⇒ 审计面失真）；要缺省请**不传**本旗标')
    p.add_argument('--resources', default=None, help='调度：互斥资源名数组（JSON，如 \'["gpu"]\'；缺省=["serial"]，[]=不占）')
    p.add_argument('--needs', default=None, help='调度：依赖的能力名数组（JSON；provider 成功后才放行）')
    p.add_argument('--prompt-file', default=None, help='任务书文件路径（可选）：create 自己把该文件内容**逐字**原子写进 `agents/task/<id>/%s`，且**写点在 `spec.json` 之前** ⇒ 登记返回 taskId 时任务书必已在场（消掉「create 与写 %s 分两次调用」的竞态：调度方看见 spec.json 即可能同秒放行、会话封装在 spawn 后数十毫秒读 prompt ⇒ 分两次调用会以 prompt 不可读死首代，而 one-shot 件首代失败即 final、restart 也救不回）。本 CLI **⛔ 渲染**任务书正文（正文由调用方渲染好写进该文件，收录判据 ①）。拒分支（rc=2 + **零落盘**、⛔ 留半成品目录）：旗标值为空/纯空白 ∨ 路径不可读 ∨ 内容为空（零字节/纯空白）∨ 非 UTF-8。**缺省不传 ⇒ 行为逐字不变**（不写该文件）' % (PROMPT_FILE_NAME, PROMPT_FILE_NAME))
    p.set_defaults(fn=cmd_create)
    p = sub.add_parser('create-bot', help='创建信箱型 bot（bot/<名字>/，只建 inbox/，无 spec）')
    p.add_argument('--name', required=True, help='人指定的 bot 名（§2.1 段白名单）')
    p.set_defaults(fn=cmd_create_bot)
    pb = sub.add_parser('bot', help='bot 登记（子命令：register）——删除仍走 agents-sync/gc.py 通道')
    bsub = pb.add_subparsers(dest='subcmd', required=True)
    p = bsub.add_parser('register', help='登记进程型 bot spec：--subscribes topic/<id>[,…]（通道 B）；spec 已在场时只改 subscribes（删除仍走 agents-sync/gc.py）')
    p.add_argument('--name', required=True, help='bot 裸名（§2.1 段白名单）')
    p.add_argument('--subscribes', default=None, help='逗号分隔的 topic/<id> 列表（只有 topic/ 族可绑定，防抢收）；空串 = 显式清空（删字段）')
    p.add_argument('--command', help='新建 spec 必备：启动命令（同 create）。值为空/纯空白即拒（rc=2、零落盘）：空白值会被逐字写进 spec.command ⇒ 会话什么也不跑而登记侧看不出来')
    p.add_argument('--workdir', help='--workdir；执行声明与状态以 serviced 为准')
    p.add_argument('--creator', help='新建 spec 必备：登记方（路径式 id 或任务名）。值为空/纯空白即拒（rc=2、零落盘）：空白值会写进审计字段 spec.creator ⇒ 查不出登记方')
    p.add_argument('--resident', action='store_true', help='声明常驻监督（spec.resident=true；与 profile.form 无关）')
    p.add_argument('--restart-policy', choices=['', 'manual', 'auto', 'one-shot'], default=None, help='Service 监督策略：manual、auto 或 one-shot')
    p.add_argument('--host', help='执行节点规范名')
    p.add_argument('--created-by-host', help='登记机规范名（缺省 = 登记机）。值为空/纯空白即拒（rc=2、零落盘，口径同 --host：空白值会逐字写进 spec.createdByHost ⇒ 审计面失真）；要缺省请**不传**本旗标')
    p.add_argument('--description', default=None, help='新建 spec 时写入 `name` 字段（人类可读描述，§4.1）；缺省（= **不传**本旗标）不写该键。值为空/纯空白即拒（rc=2、零落盘）：两形态都会被假值回落并进「没传旗标」那一档 ⇒ 描述被静默丢弃。spec 已在场时不生效（本命令只改 subscribes）')
    p.add_argument('--reaper', default=None, help='新建 spec 时写入 `reaper` 字段（终态通知唯一收件面，§4.1）：两段路径式 <family>/<name>，family ∈ {%s}；∨ 哨兵 `%s`（= 不发终态通知）；非法即拒。缺省（= **不传**本旗标）不写该键 → 运行时回落职位信箱 %s。值为空/纯空白即拒（rc=2、零落盘）：空值会被并进「没给旗标」那一档 ⇒ 指定的收尾方被静默丢弃，要缺省回落请**不传**本旗标。spec 已在场时不生效' % (', '.join(proto.FAMILIES), proto.NO_REAPER, proto.POSITION_PID))
    p.set_defaults(fn=cmd_bot_register)
    pt = sub.add_parser('topic', help='topic 协作容器（子命令：init）——只创建；删除/归档铁律走 agents-sync/gc.py add topic/<id>/ 通道，绝不直接 rm')
    tsub = pt.add_subparsers(dest='subcmd', required=True)
    p = tsub.add_parser('init', help='建 agents/topic/<id>/ 标准布局：topic.md 骨架（frontmatter when: 占位 + 议题/已决/未决）+ inbox/ + watcher/；已存在目录拒绝')
    p.add_argument('id', help='topic id（§2.1 段白名单）')
    p.add_argument('--title', help='topic.md 标题；缺省（= **不传**本旗标）= id。值为空/纯空白即拒（rc=2、零落盘）：空串会被 `a.title or a.id` 的假值回落静默换成 id、纯空白会被逐字写进 topic.md 的一级标题行；要缺省标题请**不传**本旗标')
    p.add_argument('--watcher', action='append', help='订阅登记（通道 A）：会话裸名，建 watcher/<裸名> 条目（存在即订阅、删条目即退订）；可重复')
    p.set_defaults(fn=cmd_topic_init)
    p = sub.add_parser('send', help='写 inbox 消息信封（协议动词：不做存活/意图校验）')
    p.add_argument('to')
    p.add_argument('--type', choices=list(proto.MSG_TYPES), default='inform', help='会话语用类型（§6.2）；缺省 inform。ask = **非阻塞**征询（信封自动带 via=%s，其 reply 由 answer 继承该标记才会被收件侧消费）；reply 必带 --ref' % proto.ASK_VIA_SEND_MESSAGE)
    p.add_argument('--body', help='正文（与 --body-file 二选一；trim 后空即拒）')
    p.add_argument('--body-file', dest='body_file', help='正文取自文件（长文/含引号换行走此道，不经 shell 断词）；`-` = stdin')
    p.add_argument('--ref', help='reply 必填：所答 ask 的 id。**任何 type 下**传了本旗标就 trim 判非空 ⇒ 值为空/纯空白即拒（rc=1、零落盘）：空白 ref 会写出关联不到任何 ask 的孤儿答复（提问方永远等不到），纯空白同样无效；非 reply 档不需要关联就**不传**本旗标')
    p.add_argument('--deliver', choices=list(proto.DELIVER_MODES), default=None, help='投递方式（§6.6，与 type 正交）：steer = 立即介入运行中会话的当前轮；缺省不写字段 = followUp（排队等当前轮结束）')
    p.add_argument('--from', dest='sender', help='发送方身份（两段路径式 id）；缺省取环境 AGENT_SELF，再缺省回落职位信箱 %s（不接受裸名）。值为空/纯空白即拒（rc=2、零落盘）：空值会静默回落下一档 ⇒ 发件归属被改写；要走缺省归属请**不传**本旗标' % proto.POSITION_PID)
    p.set_defaults(fn=cmd_send)
    p = sub.add_parser('answer', help='答复目标任务的未答 ask（写 reply 信封，继承其 via）')
    p.add_argument('participant', help='目标任务（task/<id>）')
    p.add_argument('--body', help='答复正文（与 --body-file 二选一）')
    p.add_argument('--body-file', dest='body_file', help='答复正文取自文件；`-` = stdin')
    p.add_argument('--deliver', choices=list(proto.DELIVER_MODES), default=None, help='投递方式（§6.6，与 type 正交；同 send --deliver）：steer = 立即介入收件方运行中的当前轮；缺省不写字段 = followUp（排队等当前轮结束）')
    p.add_argument('--from', dest='sender', help='答复方身份（同 send --from）')
    p.set_defaults(fn=cmd_answer)
    p = sub.add_parser('cancel', help='取消参与方（= control stop + 存活前置门）')
    p.add_argument('participant')
    p.add_argument('--reason', help='取消原因（入请求信封，审计面；缺省写「未给出原因」）')
    p.add_argument('--from', dest='sender', help='控制方身份（同 send --from）')
    p.set_defaults(fn=cmd_cancel)
    p = sub.add_parser('ack', help='写 inbox/ack/<id>')
    p.add_argument('participant')
    p.add_argument('msgid')
    p.set_defaults(fn=cmd_ack)
    p = sub.add_parser('control', help='写控制请求')
    p.add_argument('participant')
    p.add_argument('action', choices=['stop', 'restart', 'clear'])
    p.add_argument('--inject', help='仅 restart：进新一代启动上下文（§5.3）。值为空/纯空白即拒（rc=2、零落盘）：空白值非假值 ⇒ action≠restart 时会让报文指错成因（落到「仅对 restart 有意义」那条）、restart 时被逐字写进控制请求；不需要注入请**不传**本旗标')
    p.add_argument('--reason', help='--reason；执行声明与状态以 serviced 为准')
    p.add_argument('--from', dest='sender', help='控制方身份（两段路径式 id，如 bot/<名>、task/<id>）；缺省取环境 AGENT_SELF，再缺省回落职位信箱 %s（不接受裸名）。值为空/纯空白即拒（rc=2、零落盘，口径同 send --from）' % proto.POSITION_PID)
    p.set_defaults(fn=cmd_control)
    p = sub.add_parser('submit', help='发布或修复已登记参与方的执行声明')
    p.add_argument('participant')
    p.set_defaults(fn=cmd_submit)
    p = sub.add_parser('status', help='读取 serviced 执行状态')
    p.add_argument('participant')
    p.set_defaults(fn=cmd_status)
    p = sub.add_parser('list', help='列出参与方')
    p.set_defaults(fn=cmd_list)
    a = ap.parse_args()
    a.root = require_workspace_root(a.root if a.root is not None else default_root())
    a.fn(a)
if __name__ == '__main__':
    main()
