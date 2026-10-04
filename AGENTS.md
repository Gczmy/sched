# sched 开发与操作指南

本文件是 `sched` 项目的规则入口，适用于本目录及其子目录。配套仓库为同级的 `../dsh-node-sched`。

## 沟通

面向用户的叙述默认使用简体中文；代码、命令和技术标识保持英文。先给影响与结论，再给行动、待决策事项和必要证据；没有对应内容就省略。

使用简洁、连贯的段落，只有确实适合并列比较或按步骤执行时才使用列表。使用具体、简单的词，避免无意义术语、套话、重复总结和未经请求的对比。技术细节只保留对理解结论、判断风险或复现结果有帮助的部分。

## 指令优先级

遵守系统、平台和安全约束。用户当前明确指令优先于 Skill、历史记忆和默认偏好。项目目录中的 `AGENTS.md` 只在该项目范围内补充或覆盖全局规则，不将本项目规则自动扩展到其他仓库。

## 执行方式

用户表示要开始新工作或修复现有问题时，持续推进，直到目标完成。向用户提问之前，先完成上下文里已经授权、并且能把下一步变成可审查结果的工作；用户批准的应该是具体、可检查的结果。已有授权继续有效，不为同一动作重复请求确认。

用户的建议不适合目标时直接说明，不要迎合。不要因为假想风险，主动增加警告、免责声明、审批流程或安全／合规清单。

## 测试与验证

不要为可逆、影响小、只是复述实现的改动写测试。运行与本次改动相称的测试并完成必要检查；通过后，只有出现新改动、新失败或尚未解决的疑点时，才扩大或重复测试。

调度器的执行型测试依赖 Linux/POSIX 环境；远程测试、训练、批处理和 smoke test 必须在计算节点执行。纯文档改动可在本地静态检查路径、命令引用与差异，无需启动 daemon。验收脚本位于 `tests/run_*_accept.sh`，Python 回归位于 `tests/test_*.py`；文档引用检查见 `tests/run_docs_refs_accept.sh`。

收尾删除本次产生、之后用不上的临时文件。调度器管理的状态、日志和产物仍须遵守下文的 CLI 操作契约，不因清理临时文件而直接删除。

## 工具与并行

搜索文件或文本优先使用 `rg`、`rg --files`；独立的读取和查询尽量批量执行。网页控制台无 CLI/API 时，使用已登录的 Chrome 浏览器；飞书优先使用 `lark-cli`。

只有存在真正独立的工作流，且委派能节省时间或提升质量时才使用子 Agent。共享状态、连续决策和简单任务由当前 Agent 直接完成；委派任务必须有明确输入、输出和完成判据，最终结论由主 Agent 汇总并验证。

## 规则来源

全局规则维护在当前生效的 canonical `AGENTS.md`；本文件维护 `sched` 项目约定。`CLAUDE.md` 只作兼容入口，引用对应的 `AGENTS.md`，不复制规则正文。

项目事实、生产状态、历史决策和对外契约以项目级 `AGENTS.md` 及其指定的脚本、探针、决策记录和合同文件为准。代码用于核对当前实现；文档与实现不一致时明确指出差异。生产现状需通过 CLI 查询确认，不能把历史记录当作当前状态。

| 来源 | 用途 |
| --- | --- |
| `docs/integration-contract.md` | 持久实例身份、结构化请求回执与幂等网关投递 |
| `docs/reference.md` | 当前配置、CLI、JSON、状态机与写操作契约 |
| `docs/execution-boundary.md` | 通用执行层、客户程序、旧兼容守卫与各仓库独立发布边界 |
| `docs/recovery-policy.md` | 候选 checkpoint/smoke、新版本 FIFO、显存准入与前台守护 |
| `docs/recovery-acceptance.md` | 候选恢复故障/兼容矩阵与非生产 GPU 验收 |
| `docs/execution-api.md` | 公开 execution backend 注册、输入 FD 与可选 native 构建 |
| `docs/persistent-execution-owner.md` | 可选持久 owner 的身份、认证重连、冷配置保留期与已记录健康 |
| `docs/execution-rollout.md` | 独立发布、安装与 schema 回退边界 |
| `docs/release-preparation.md` | 固定来源的发布准备、原始产物校验、草稿续传与离线 evidence |
| `docs/releases/0.2.2.md` | 已发布 0.2.2 的固定来源、验收证据、ABI/libc 范围与安装约束 |
| `docs/releases/0.3.0.md` | 恢复能力、真实 CUDA 验收、正式 ABI 范围与 schema 9 回退 |
| `docs/releases/0.3.1.md` | SQLite 锁修复、版本查询与 schema 9 升级边界 |
| `docs/native-integration.md` | 旧实验接口的持久态保护与迁移限制 |
| `docs/native-deployment.md` | 已外移的旧实验部署绑定记录 |
| `docs/project-gpu-access.md` | 项目 GPU 开关的行为、实现与验收依据 |
| `docs/next-development.md` | 尚未实现的开发项，不能当成可用配置或 API |
| `docs/README.md` | 当前文档索引及历史记录的适用范围 |
| `docs/repository-hygiene.md` | 公开仓库中的示例、运行记录与隐私信息边界 |
| `../dsh-node-sched/docs/implementation-notes.md` | 配套插件的实现定案与历史原因 |

## 项目边界与代码入口

`sched`（Python 包名 `gsched`）是节点级 GPU/CPU 批量任务调度器，要求 Python >= 3.10，运行时零第三方依赖。调度器保持“无意识”：只提供标准接口，不含 Agent 逻辑。Agent 操作调度器的唯一入口是 `sched` CLI（`gsched.cli:main`）。

调度器的源码、构建、公共回归与发布独立维护。通用 `gsched.execution` 负责可执行文件和输入 FD 绑定、直接子进程 owner、真实 wait、取消与清理；可选 native 源码在本仓库。daemon 不导入客户模块、动态项目 backend 或由客户仓库提供的 `gsched` 扩展。客户协议、科学阶段、研究合同、研究 gate 与科学加载验证归客户仓库，不作为 sched 发布前置条件。

`linux_fd_owner` 由独立原始 owner 持有 child，允许 daemon 凭不可变绑定认证重连；恢复不能重新 start。该 backend 的 FD4 使用显式 owner identity 封装，不能自动替换 `linux_fd` 的 v1 identity。连接不确定时保留任务与资源；owner 丢失时仍禁止用 PID、日志或应用产物推断 wait。

新提交使用公开 `execution` 接口；旧 strict/native 输入仅供历史识别，不能授予执行权。历史 session、名称消费与未知尝试的兼容守卫必须保留，不能因清理专用代码重放或删除旧运行记录。源码与必要测试的边界检查使用 `python scripts/check_execution_boundary.py`。

`dsh-node-sched` 是 dsh 插件：`packages/node-sched` 负责 SSH 传输、查询、写操作转发与审计，`packages/node-sched-ui` 负责看板。调度和状态语义由 `sched` 实现，配套插件不重新实现调度逻辑。

| 代码 | 职责 |
| --- | --- |
| `gsched/cli.py` | 命令解析、主机守卫、JSON 输出、提交与 `request` |
| `gsched/state.py` | SQLite WAL、私有只读快照、事务、revision 与请求记录 |
| `gsched/daemon.py`、`gsched/dispatcher.py` | 生命周期、inbox 消费、恢复、依赖解锁与派发 |
| `gsched/allocator.py`、`gsched/executor.py` | GPU 探测与分配、任务启动和进程组管理 |
| `gsched/execution/`、`gsched/execution_policy.py` | 自包含通用执行实现、backend 冷注册与任务文件绑定 |
| `gsched/_legacy_execution.py` | 历史持久态识别与禁止重放的兼容守卫 |
| `gsched/schema.py`、`gsched/config.py`、`gsched/templates.py` | 批次与配置校验、runtime 解析、模板展开 |
| `gsched/fingerprint.py`、`gsched/artifacts.py`、`gsched/notify.py` | 指纹与产物校验、终态通知 |

跨仓库修改 CLI 或 JSON 契约时，同时核对配套仓库的 `packages/node-sched/lib/index.js`、`packages/node-sched-ui/src/ui-contracts.js` 及两边的契约测试。前端源码在 `packages/node-sched-ui/src/`，不要直接编辑生成的 `lib/client.js`。

## 调度器操作契约

- 只走 CLI，禁止直接修改 `state.db` 或 state 目录文件，不用手写 SQL 绕过 WAL 和并发协议。没有对应子命令时先提出接口需求。
- 脚本或 Agent 解析结构化结果时使用命令支持的 `--json`。`config get` 本身输出 JSON；`diag`、`log`、`verify` 等没有该选项，不虚构参数，也不把展示文本当稳定字段。`capabilities --json` 只报告查询本机的能力；`daemon check --json` 仍受计算节点守卫约束。
- `cancel`、`discard`、`clean`、`config set`、`gpu-free` 必须带 `--yes`。缺少该参数时返回码 `1` 表示未确认；参数要求不等于需要向已有授权的用户再问一次。
- 查询可在登录／网关节点执行；数据库查询由 CLI 获取私有只读 DB/WAL 快照，不能自行用 SQLite 打开共享源库。节点目录取自 `config.node`，不要用网关的 `hostname` 推导。
- `sched daemon start/stop/check/drain/resume` 必须在计算节点执行，包括 `request` 包装的调用；登录节点可用 `sched daemon status`。其他写操作受主机守卫约束，不把 `SCHED_ALLOW_FOREIGN_WRITE=1` 当作日常工作流。

### HPDC 登录与执行位置

远程 HPDC 登录统一使用 `ssh HPDC`。`HPDC_outside` 入口暂时关闭，不再按校内／校外选择入口或询问网络环境；配套仓库旧日志中的入口提示不改变此约定。

网关禁止运行计算任务。正常批次提交使用网关上的 `sched submit <batch.json>`，由文件 inbox 交给计算节点 daemon 收编；返回“已投递”后，用 `sched verify <batch-id>` 确认入库。`sched run` 是计算节点直接写入入口，不走网关 inbox，除 `--dry-run` 外不得在网关执行。

生产 daemon 通过实际部署使用的 screen 会话进入计算节点后管理。节点、会话、租约与有效期属于私有运行信息，不写入仓库；先用 `screen -ls` 确认会话，节点身份以实际主机和 `config.node` 为准。`sched daemon stop` 会取消运行任务。支持 drain 的版本需要无损重启时按顺序执行：

1. 执行 `ssh HPDC`。
2. 执行 `screen -ls`，再执行 `screen -d -r <session-id>`，使用实查会话。
3. 确认当前主机与 `sched config get` 的 `node` 一致，执行 `sched daemon drain --stop-when-idle`，等待 running 和未决启动标记清空、daemon 自然退出。
4. 完成维护并通过 `sched daemon check` 后，执行 `sched daemon resume`、`sched daemon start`。旧版本尚无 drain 时，先等运行任务自然结束再 stop；不可用 stop 模拟无损排空。

### 常用命令

任务引用使用 `<batch-id-or-name>:<task-id>`，完整 batch ID 优先匹配，名称解析为同名最新批次。自动化写操作使用完整 ID，避免同名批次歧义。

| 命令 | 用途与关键参数 |
| --- | --- |
| `sched --version`、`sched version [--json]` | 当前代码版本；JSON 包含 schema 兼容范围，不读取配置或 DB |
| `sched identity --json`、`sched request-status <request-id> --json` | 私有只读快照查询实例身份和原始请求事实，不迁移旧库 |
| `sched submit <batch.json> --request-id <id> --expect-instance <id> --expect-project P --json` | 同绑定复用原 batch ID；网关投递与数据库接受分别报告，未知不重投 |
| `sched submit <batch.json> --dry-run --json` | 校验、任务展开与 SKIP 预览；正式提交去掉预览参数 |
| `sched verify <batch-id>` | 确认批次已持久化；投递成功不代表已经入库 |
| `sched run` | 计算节点提交单任务；GPU 用 `--gpus 1`，CPU 用 `--cpu-only` |
| `sched status [batch] --json` | 当前态；支持 `--project`、`--limit`、`--cursor`、`--job-cursor` |
| `sched task <batch>:<task> --json` | 单任务与各版本详情 |
| `sched execution <batch>:<task> --json` | 通用执行尝试、身份绑定与原始退出／清理事实；owner_health 只表示已记录观察，不探测服务 |
| `sched execution list --json` | 跨任务筛选和实时分页；续页不能合并为完整当前态，具体契约见 execution-api |
| `sched capabilities --json`、`sched daemon check --json` | 本机能力／计算节点前置检查；check 另按 backend ID 检查文件摘要与项目 root，通过不替代启动校验 |
| `sched diag <batch>[:task]`、`sched log <batch>:<task>` | 失败诊断优先用 `diag`；日志支持 `-n N`、`-f` |
| `sched retry <batch>[:task]` | 同 spec 解锁失败终态重跑；省略任务为批次级 |
| `sched resubmit <batch>:<task>` | 同 spec 新版本入队；批次级使用 `--failed` 或 `--all`，可先 `--dry-run` |
| `sched cancel <batch>[:task] --yes` | 取消任务／批次；也支持 `--project P --yes` |
| `sched discard <batch> --yes`、`sched clean <batch> --yes` | 退役旧批次／清理最新产物和指纹并重排符合条件的 skip；约束见 reference |
| `sched history [batch] --json` | 历史各版本；支持 `--limit`、`--cursor`、`--status`、`--project` |
| `sched incidents [id] --json` | OOM／硬件事故快照；支持 `--job`、`--gpu` |
| `sched list-gpus`、`sched project list --json` | GPU 状态／项目 GPU 访问策略、配额与用量 |
| `sched gpu-set-mem <idx> <gib>`、`sched gpu-ok <idx>`、`sched gpu-ignore <idx>`、`sched gpu-free <idx> --yes` | 卡管理；未知 idx 报错，`gpu-set-mem` 是临时容量覆盖 |
| `sched config get`、`sched config set -f <patch.json> --yes`、`sched config reload` | 读取配置／深合并补丁并触发热更／请求重载 |
| `sched daemon status --json` | 只读健康查询，不打开 DB；区分调度健康、进程存活与查询节点，跨节点 PID 状态为 unknown |
| `sched daemon drain [--stop-when-idle]`、`sched daemon resume` | 暂停新派发／解除暂停；running 自然结束、pending 保留；排空状态跨重启保留 |
| `sched request <request-id> --expect-revision N ... -- <mutation>` | 计算节点持久化幂等写操作，前置条件见下文 |
| `sched markers`、`sched notify-inbox --json`、`sched notify-ack <file>`、`sched notify-test` | 批次终态／通知查询／确认／渠道验证 |

### 与 dsh-node-sched 的接口约定

`status`、`task`、`history` 的 JSON 当前使用 `schema_version: 1`。任务关联使用 `batch_id`，不要用显示名称关联。等待态使用 `status: "pending"` 和独立的 `wait_reason`。

项目禁用 GPU 时，排队 GPU 任务的 `wait_reason` 为 `project_gpu_disabled`。使用此功能前需同步更新配套插件；旧插件的严格校验不接受这个新值。

`status` 的批次分页与任务分页相互独立，分别检查 `truncated.batches`／`next_cursor` 和 `truncated.jobs`／`next_job_cursor`；`history` 使用自己的 `truncated` 与 `next_cursor`。不能将截断、过期或读取失败的结果当作完整当前态；看板写操作要求新鲜且完整的有效快照。

插件的查询通道与写入目标独立配置。切换看板 SSH 绑定不会切换 writer；writer 默认禁用，启用时必须显式配置目标并验证实际主机、`config.node` 与 `mutationExpectedNode` 一致。`screen` writer 还需要显式会话名，插件没有内置生产会话。

看板写操作经 `sched request` 转发。每次逻辑操作分配并持久化独立的 `request-id`；同一操作重放时必须沿用该 ID 与完全相同的命令、前置条件，不能在结果未知时换 ID 或自动换连接重发。批次绑定 `revision`，任务额外绑定 `version`，GPU 绑定其 `revision` 与完整有序的 `assignments`；具体参数见 `docs/reference.md`。

`request` 返回码 `64` 表示参数或请求绑定错误，`65` 表示前置条件冲突，`75` 表示先前结果未知、拒绝重放。它只包装已支持的写命令，不是绕过主机守卫的网关入口；网关正常提交仍使用 `sched submit`。

## 状态与资源语义

- 任务：`pending → running → done / failed / blocked / cancelled / timed_out / interrupted`；`pending → skip` 表示产物指纹命中，属于成功终态。
- 批次：依赖未满足为 `queued`，解锁后为 `active`；全部成功终态为 `done`，任一失败终态为 `blocked`；`blocked/queued` 可人工退役为 `discarded`。
- GPU：`free → assigned → releasing → free`。外部 compute PID 或 compute/topology/utilization 探测不确定时立即 `unmanaged`；只有进程列表完整且为空、利用率可读且大于 0 的信号连续 3 tick 才确认，确认期间即使显示 `free` 也禁止派发。`unmanaged` 连续 2 次干净采样自动恢复，`quarantined` 需 `gpu-ok` 解除。
- `projects[P].gpu_enabled` 必须为布尔值，省略为 `true`。热更新为 `false` 后拒绝新 GPU 提交与手动 GPU retry/resubmit，暂停派发已排队 GPU 任务；运行中任务正常结束，CPU-only 不受影响。恢复为 `true` 后原排队版本继续运行。
- `projects[P].gpu_quota` 省略或为 `0` 表示无限制；正整数限制并发 running GPU job 数，不是物理卡数。共享 job 各计 1，CPU-only 不计。禁止 GPU 使用 `gpu_enabled:false`，不能用零配额或空 affinity 代替。
- 派发顺序为项目 priority 降序、批次 priority 降序、同值 FIFO；不抢占，高优候选暂不可运行时低优候选可补位。`gpu_affinity_hard` 只限制本项目候选卡；独占隔离需要所有竞争项目使用互不重叠的硬亲和集合。

## 通知与检查点

处理调度器唤醒或继续跟进批次时，先运行 `sched notify-inbox --json` 查看未读事件；处理后将返回的 `_file` 传给 `sched notify-ack <file>`，不要手动 rename。已确认事件添加 `.acked` 后缀，由 daemon 在 7 天后清理。

启用 file 通知时，将以下补丁保存为 state 目录外的 JSON 文件，在计算节点执行 `sched config set -f <patch.json> --yes`，再用 `sched notify-test` 验证；首次初始化也可使用 `sched init`。

```json
{
  "notify": {
    "on": ["batch_done", "batch_blocked"],
    "file": {"enabled": true}
  }
}
```

state 根目录优先级为 `SCHED_STATE` > 已加载的 `config.state_dir` > `~/.sched`。批次终态 marker 位于 `<state>/<node>/markers/<batch-name>.done|.blocked`，通知位于 `<state>/<node>/notify_inbox/`；这些路径用于理解与诊断，操作仍走 CLI。

支持 `file`、`email` 和 `command` 渠道，`webhook` 尚未实现。email 的 SMTP 密码通过 `password_env` 指定的环境变量提供；command 将事件 JSON 送入脚本 stdin。唤醒脚本示例见 `scripts/notify_tmux_example.sh` 和 `scripts/notify_headless_example.sh`。
