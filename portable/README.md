# 最小本机任务平台（离线安装）

这是 agent-platform 的**本机文件任务核心**安装包，不是完整系统安装，也不是 AI agent 安装器。提供配置检查、agentctl 文件协议 CLI，以及真实无模型 shell 任务的登记、人工放行、前台执行和报告终态。无下载、无后台服务、无全局软件安装，不修改 shell、SSH 或系统配置。

## 公开依赖与范围

- 用户自行提供 Python >= 3.10（仅标准库）、POSIX 系统（Linux/macOS）和 PATH 上的 bash。用自己的 Python 路径替代下文 `python3`；无需 conda。
- 源码可从公开仓 `https://github.com/huafengxi/agent-platform` 获取；git 仅用于取源码，不是归档安装的运行依赖。本入口不联网或执行远程脚本。
- 不含 pi、Node、模型账号、角色/profile 装配、任务政策、知识库、同步、调度 daemon、服务管理、Web 和定时系统。不需要上述组件的配置或凭据。
- 真模型任务另需用户自己的 pi/Node、模型供应商凭据与显式隔离配置；本版没有该集成入口，不能用 shell demo 的成功推断模型可用。

## 从源码或归档安装

源码路径可位于任意目录。不要求特定工作区名称；不要在个人工作区上运行复制/清洗脚本。

```sh
# 在源码仓或解包后的 agent-platform-minimal 目录中：
python3 -I -S portable/install.py install --prefix /absolute/new/task-platform

# 从任何 cwd 检查和运行：
python3 -I -S /absolute/new/task-platform/portable/cli.py doctor
python3 -I -S /absolute/new/task-platform/portable/cli.py demo
python3 -I -S /absolute/new/task-platform/portable/cli.py ctl list
python3 -I -S /absolute/new/task-platform/portable/cli.py ctl --help
```

目标的父目录须存在；目标本身须不存在（包括空目录和符号链接），失败时不自动删除残留。安装不会启动任何进程。`config.json` 来自 [example.json](example.json)；workspace 是相对该配置文件的路径，须已有 agents 子目录；host 是用户选择的本机任务路由名。可用 `--config /path/to/config.json` 指定自己的配置。不要指向其它正在使用的平台运行树。

`demo` 只登记并执行包内固定 printf 命令，每次创建独立 task；它在当前进程监督一个 shell，不扫描或运行其他任务，不启动 runner/scheduler 服务。stdout 返回结果目录，含 spec.json、enable.json、pid.json 和 report.md。成功要求真实 exitcode=0、final=true 且报告含成功标记；报告明确标注无模型调用。超时或缺依赖报错并非成功。

CLI 丢弃继承环境，只给子进程 PATH、安装内 HOME、语言设置；Python 子命令使用 `-I -S`，不读取用户 site-packages、PYTHONPATH 或 pi 配置。PATH 是用户显式选择外部可执行文件的信任边界，请勿包含不可信程序。此隔离不是容器或任意命令安全沙箱；agentctl 可登记任意命令，只有调用方信任的命令才能交给 runner。`ctl` 是原有登记/通信 CLI，不自动运行自定义任务。

## 构建与内容边界

```sh
# 输出文件须不存在；留在本地：
python3 -I -S portable/install.py build --output /absolute/output/minimal.tar.gz
# 在新目录解包（普通 tar 工具；无需 pip/npm）：
tar -xzf /absolute/output/minimal.tar.gz -C /absolute/new/unpack-directory
```

唯一成员清单是 [files.json](files.json)。构建器逐项读取普通文件并拒绝符号链接，不递归复制目录；归档元数据不包含源码文件的用户名、组名和时间。安装也只复制同一清单；归档和源码使用同一个安装器。清单外不包含任何资产：身份材料、凭据/密文、运行树和历史日志、git 元数据/remote、家目录配置、个人 pi auth/models、岗位记忆或业务知识均不组装。构建器不是通用隐私分类器；修改清单/源码后仍需审查文件正文。

## 回归与清理

在完整源码仓中运行（测试脚本不作为运行依赖打包）：

```sh
python3 -I -S portable/test_portable.py
```

回归自行建立独立临时根，保留根路径供检查；不使用宿主 HOME 作为工作目录。覆盖归档精确成员、隐私标志抽查、缺依赖、异 cwd/空格路径、安装拒覆盖、符号链接拒收与 shell 任务真实终态。标志扫描只能发现选定风险，不能证明绝对无隐私。

卸载不需要系统命令或服务停止：确认没有该目录下自己启动的任务，核实安装路径后删除这个独立安装目录即可；没有全局链接或配置要恢复。归档、解包目录和测试打印的临时根可单独检查后清理。安装失败也只清理本次新建目标，勿用共享目录通配符。
