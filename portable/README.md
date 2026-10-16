# 最小本机任务核心：解包即运行

这是 agentd 的**本机文件任务核心**，不是整套 AI 平台。只含检查、agentctl 文件协议 CLI 和真实无模型 shell 任务的登记、显式放行、前台执行及终态报告。无安装复制、下载、pip install、全局软链、后台常驻或 shell/SSH/系统服务修改。

## 最短用法

自行提供 Python >= 3.10（标准库）、POSIX 系统（Linux/macOS）、POSIX 基础工具和 PATH 上的 bash。无需 conda、Node/pi、模型账号、私有基础设施或个人配置。

clone 本 agentd 仓，或把归档解压到自己的目录后，直接运行同一个脚本（示例路径替换为自己的源码目录）：

```sh
/path/to/agentd/portable/run.sh doctor
/path/to/agentd/portable/run.sh demo
/path/to/agentd/portable/run.sh ctl list
/path/to/agentd/portable/run.sh ctl --help
```

没有安装步骤，也没有配置文件。脚本按自己的位置发现源码根，不依赖 cwd；路径带空格时用引号。真实入口、相对入口和软链入口均按最终源码位置定位；不创建全局链接。构建器仍拒绝 allowlist 中的符号链接文件。

`doctor` 检查 Python 版本、POSIX、bash 及运行目录可写性，输出路径与本机标识，**不创建目录/任务或字节码**。可写性为只读预检，不是写入成功保证（ACL、配额、并发变化仍可能让实际动作失败）。`demo` 才创建运行目录和独立 task；实际运行 bash printf，前台仅监督这个新任务，不扫描/运行其他任务，不启动服务。成功须 exitcode=0、final=true 且 report.md 含真实 shell 成功标记。stdout 给出结果目录，可查 spec.json、enable.json、pid.json、report.md。`ctl` 是登记/通信入口，不自动执行自定义任务；它可创建运行目录。

## 约定与仅有的可选覆盖

| 项目 | 默认 | 可选覆盖 |
|---|---|---|
| 源码根 | run.sh 所在 portable 目录的父目录 | 自动发现，无配置项 |
| 运行根 | 源码根下 `workspace/` | `PLATFORM_WORKSPACE` |
| Python | PATH 上的 `python3` | `PLATFORM_PYTHON`（解释器名或路径，不是带参数的命令） |
| 本机任务路由标识 | `local`（本机单进程演示，不用于多机路由） | 无必填 host 配置 |
| 任务 HOME | 运行根下 `home/` | 随运行根确定 |
| 任务文件 | 运行根下 `agents/` | 随运行根确定 |

`PLATFORM_WORKSPACE` 复用平台现有公开变量；本入口的默认值特意在包内，不采用底层模块直接运行时的源码父目录默认值。空值视为未设置。**相对运行根路径以源码根为基准**，不是调用 cwd，也不展开 `~` 或 `$变量` 字面值。`PLATFORM_PYTHON` 中含 `/` 的相对路径同样以源码根为基准；不含 `/` 时在 PATH 查找。

```sh
# 只读源码或希望状态外置时：
PLATFORM_WORKSPACE='/absolute/my task state' /path/to/agentd/portable/run.sh demo
# 使用自选解释器（可选；不是必须的安装配置）：
PLATFORM_PYTHON='/path with spaces/python3' /path/to/agentd/portable/run.sh doctor
```

只读包若仍选择包内默认运行根，会清楚报不可写并提示 `PLATFORM_WORKSPACE`，不悄悄回退 HOME。请使用专属运行根，不要指向其他平台正在使用的状态树。无状态源码包可以搬迁后直接运行；**已有 task 中可能含绝对路径，已有状态不承诺可搬迁**，也没有迁移机制。

## 隔离与限制

启动脚本仅捕获 PATH、必要的运行根及解释器选择，随后以空白环境启动 Python；Python 使用 `-I -S -B`。CLI 为任务建立自己的 HOME，只保留 PATH、语言和必要的协议环境；宿主身份、SSH agent、供应商凭据、BASH_ENV、PYTHONPATH 和个人 pi 配置不继承。不会为了方便读取宿主 HOME 或 config.json；没有 `--config` 入口。

PATH、所选解释器、启动脚本所用 POSIX 工具及源码是调用方信任边界；不要用不可信可执行文件或命令。环境净化不是 OS 沙箱，不阻止任意可信命令主动访问系统文件，也不防启动脚本执行前的系统加载器注入或恶意并发文件替换。`ctl` 可登记任意命令，调用方必须自己信任它。

不包含模型调用、pi/Node 集成、角色/profile、知识库、同步、调度 daemon、服务管理、Web 或定时系统。shell demo 成功不说明这些能力可用。只验证本机 shell 最小链路。

## 打包方：显式 allowlist 构建

构建不是使用者安装步骤，也不会公开发布归档：

```sh
python3 -I -S -B portable/build.py --output /absolute/output/minimal.tar.gz
# 解到一个自己选择的新目录，随后直接运行其中的 portable/run.sh：
tar -xzf /absolute/output/minimal.tar.gz -C /absolute/new/unpack-directory
```

输出文件须不存在，包内前缀为 `agentd-minimal/`。allowlist 适配本仓根级 agentctl/proto/runner/registration/task_state/envscrub/helper 布局，不要求 agentd/ 子目录或统一平台仓。唯一成员清单为 [files.json](files.json)，只读取列出的普通文件，拒绝清单项符号链接，不递归收集工作区。归档中 run.sh 保留可执行权限；不带构建宿主用户名、组名、时间。清单不带个人 env、lore、work、bots 身份、pi 配置、凭据密文、运行数据或 .git。构建器不是隐私分类器；改清单和正文仍需审查。没有 install.py、install --prefix、example.json 或配置生成/复制路径。

## 回归与清理

在完整源码仓运行（测试脚本不打包）：

```sh
python3 -I -S -B portable/test_portable.py
```

回归只用独立临时根，打印并保留沙箱。覆盖零配置脚本、解包原地运行、空格路径/不同 cwd、外置状态、只读 doctor、无效配置诱饵、真实 shell 终态与环境隔离、缺依赖、无状态搬迁、allowlist 隐私诱饵与拒符号链接。诱饵/标志检查不是绝对隐私证明。

没有全局配置要恢复。确认自己启动的任务已退出后，可按自己的清理流程移除专属源码/状态目录；外置状态与源码分别管理。不要把共享工作区当作可整树清理的运行根。归档、解包目录和测试临时根也留给调用方检查后处理。
