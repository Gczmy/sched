# 通用 execution API

> 本文对应 0.2.2，写库 schema 7；正式提交和产物以 [v0.2.2 Release](https://github.com/Gczmy/sched/releases/tag/v0.2.2) 为准，生产部署独立安排。

普通任务继续使用默认 subprocess 执行。需要固定可执行文件和输入 FD 的任务，
通过管理员冷配置的 `linux_fd` backend 使用通用执行层。backend 注册值和其引用的
项目 root 不能热切换；任务不能传入 Python module 或注入 `gsched` namespace。
调度器不加载项目 adapter 代码。

## 管理员配置

以下配置使用虚构项目与路径，摘要必须替换为实际已审查文件的 SHA-256：

```json
{
  "execution_backends": {
    "text-worker": {
      "kind": "linux_fd",
      "executable": "/opt/example/bin/text-worker",
      "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
      "argv": ["text-worker", "--input-fd", "3"],
      "env": {},
      "projects": ["documents"],
      "input_slots": {"3": {"max_bytes": 1048576}}
    }
  }
}
```

`executable` 与 `sha256` 绑定实际可执行文件，`argv` 和 `env` 由管理员固定。
`projects` 限制可选择此 backend 的项目，`input_slots` 固定允许的 child FD 和
字节上限：允许 `3` 和 `5..63`，`4` 保留给 scheduler identity，最多 16 个 slot，
每个 `max_bytes` 为 `1..16777216`。配置只接受精确字段集合；注册的项目必须存在。
`env` 不继承宿主环境，不能覆盖 `SCHED_*` 或 `CUDA_VISIBLE_DEVICES`。
backend 缺失、文件漂移、输入不匹配或 native 能力缺失时明确拒绝，
不能回退到路径执行、shell 或默认 subprocess。

需要跨 daemon 重启保留原始 wait 时，管理员可显式选择 `kind:linux_fd_owner`。
它使用同样的 executable/argv/env/input 冷配置，新增独立原始 owner 和认证重连。
其 FD4 为 `sched_execution_owner_identity/v1`，封装原始 attempt identity 与实际
owner 身份；客户端必须明确支持此封装。绑定与崩溃窗口见
[persistent-execution-owner.md](persistent-execution-owner.md)。

0.2.1允许该 backend 的可选冷配置 `owner.prepare_timeout_sec`（1–300 秒，默认 30）
与 `owner.terminal_retention_sec`（60–604800 秒，默认 3600）。省略字段使用默认值；
其他 backend 不接受 `owner`，未知字段、布尔值和非有限数值拒绝。热更新不改变既有绑定。

## 任务输入

```json
{
  "id": "summarize",
  "cmd": ["text-worker", "--input-fd", "3"],
  "max_retry": 0,
  "resources": {"gpu": 0, "cpus": 1},
  "execution": {
    "backend": "text-worker",
    "inputs": {
      "3": {
        "path": "inputs/request.json",
        "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
      }
    }
  }
}
```

`cmd` 必须与注册的固定 `argv` 精确一致；显式设置 `max_retry:0`，任务不声明
`stages`、batch/task `env` 或 `runtime`，cwd 为注册项目 root。
输入路径是规范化项目相对 POSIX 路径，不能包含 `..`、反斜线或绝对路径；
输入 slot 必须与管理员注册集合一致。提交阶段校验声明与配置的精确绑定，
启动前逐级拒绝 symlink、读取有界 regular file、校验摘要，再封印副本并映射 FD。
可执行文件也按管理员摘要校验为 retained FD，不重新按任务路径执行。
输入内容是项目自己的数据，调度器不从中提取阶段、业务身份或研究授权。
后端退出事实、取消与 deadline 事实、完成和清理事实由通用 execution 层产生；
科学或其他业务结果由子进程和项目验收解释。

child FD4 是封印的 `sched_execution_identity/v1` JSON，含 `interface_version`、
`attempt_id`、job/batch/task/version、node、原 scheduler PID/start ticks 和 backend
绑定摘要。它标识本次通用执行尝试，不包含客户协议，也不自动授予研究角色。
应用程序可以自行验证或使用这些事实；调度器不接受应用程序回写它们。

## 生命周期 Python API

`gsched.execution` 导出 `ExecutionEnvelope`、`ExecutionObservation`、
`SubprocessBackend`、`LinuxFdBackend`、`Prepared`、`Owner` 和
`retained_owners()`；接口版本为 `sched-execution/v1`。
另导出 `PersistentLinuxFdBackend`、`PersistentOwner` 和 `OwnerUnavailable`。
持久 backend 的 `prepare` 额外接收不可变 `identity`，可传 `duration_seconds`；
`PersistentOwner(binding)` 仅重连原服务，不能创建替代服务或 child。
它不接受项目回调或协议字段。以下独立调用演示普通 child wait：

```python
import sys
from gsched.execution import ExecutionEnvelope, SubprocessBackend

envelope = ExecutionEnvelope((sys.executable, "-c", "print('hello')"), env={})
with SubprocessBackend().prepare(envelope) as prepared:
    owner = prepared.launch()
    observation = owner.wait(timeout=5)
    print(observation.status, observation.returncode)
    owner.close()
```

`prepare()` 在创建 child 前建立 owner；`Prepared.launch()` 只有一次尝试，
发生异常后也不能重放。native `prepare()` 接收 `executable_fd`、可选 `cwd_fd`
和 `{child_fd: caller_fd}` 的 `fd_bindings`，复制所需 FD，不接管调用方原 FD。
`ExecutionEnvelope` 的环境显式提供；native 不允许 pathname `cwd` 替代 dirfd。

`Owner.poll()`/`wait()` 返回实际原始 owner 观察，字段为 `status`、`pid`、
`returncode`、`rusage`、`launch_error`、`group_clean` 和接口版本。
child 的 `exited` 仅是进程事实；仍有同组后代时状态为 `cleanup_pending`。
`cancel()` 请求终止并由 poll 驱动期限后升级；`wait(timeout=...)` 的等待期限
本身不等于取消 child。owner 必须在原进程、原线程使用，未解决 owner 不能 close。
`retained_owners()` 只列出原进程保留对象，不能按 PID 重建权威。
持久服务独立驱动取消升级与 duration，不要求 daemon 持续 poll；代理连接失败
抛出 `OwnerUnavailable`。`lost:false` 为当前无法认证或通信，仍须保留资源重查；
`lost:true` 只表示绑定的服务进程已消失、身份被替换或主机重启，不能提供 child wait。

## CLI 查询

`sched submit <batch.json> --json` 正式提交返回单个 JSON 对象，包含
`schema_version:1`、`batch_id`、`tasks`、`project`、`delivery` 与 `persisted`。
计算节点事务提交为 `delivery:database,persisted:true`；网关文件投递为
`delivery:inbox,persisted:false`，仍须用 `sched verify` 确认收编。诊断输出在 stderr。

使用 `sched execution <batch-id>:<task-id> --json` 查询通用尝试，顶层包含
`schema_version:1`、`batch_id`、`batch_revision`、`task_id` 与 `attempts`。
调用方同时读取 `task` 时应核对 `batch_revision`，不合并不同 revision 的结果。
每项包含 attempt/job/version、backend 绑定、phase、identity、observation、
cancel reason 和时间；没有发生通用尝试的任务返回空数组。
持久尝试另有 `owner`，仅含 schema、owner ID、PID/start ticks、boot ID 和 attempt ID；
私有 endpoint 和认证 token 不输出。
0.2.1还返回 `owner_health`：`source:recorded`、`connection_status`、
`last_observed_at`、`cleanup_state`、`cleanup_attempts`、`retry_after`、
`last_cleanup_at`、`cleanup_error`、`acknowledged_at` 和 `acknowledgement`。
连接值为 `unknown/responsive/unreachable/lost`；确认状态为 `active/pending/acknowledged`，
旧库无记录时为 `unknown`，其余字段为 null。`retry_after` 是 Unix 秒，其他时间沿用
调度节点的 `YYYY-MM-DD HH:MM:SS` 记录格式，不含时区。错误为
`owner_unreachable/owner_rejected/close_timeout` 或 null；确认结果为
`closed/owner_lost` 或 null。查询不探测服务，记录可能过期；确认状态与下面 diagnostics
的进程组 `cleanup_state` 含义不同，确认元数据不改变原始 wait/rusage。
只读诊断另有 `diagnostics` 和 `legacy_sessions` 数组，按 job version 升序排列；
`--version N` 同时过滤这三个数组。新增字段不改变原始 `attempts` 或
`status/task/history` 的 schema 1 契约，也不执行 schema 升级。

`diagnostics` 每版本包含 job ID/version/status、`execution_kind`、`phase`、
attempt/session ID、原始 `cancel_reason`、`job_kill_reason`、`job_failure`、
`observation_status`、`wait_result_available`、`returncode`、`rusage`、
`launch_error`、`cleanup_state`、`uncertainty_reason` 与 `replay_blocked`。
kind 为 `generic`、`legacy`、`subprocess` 或无法读取 spec 时的 `unknown`；
generic 尚未保留尝试时 phase 为 `not_reserved`，旧入口无 session 时为 `retired`，
普通任务没有通用 phase。`replay_blocked` 标识已消费尝试、旧入口或无法读取 spec，
不能把它为 false 当作新的执行或 retry 授权，写操作仍执行自己的完整校验。

只有原始 observation 为 `exited/cleanup_pending` 且 returncode 为整数时，
`wait_result_available` 才为 true。`cleanup_state` 为 `confirmed/pending/unknown`，
只取自原始 `group_clean`，退出码为 0 不等于清理已完成。旧 session 的 wait 和
cleanup 为未知；job 的 rc、PID、日志及应用结果不能填补缺失事实。
`uncertainty_reason` 为 `legacy_wait_unavailable`、`owner_authority_lost`、
`owner_unreachable`、`record_invalid` 或 null；尚在正常执行的任务不因未退出就标为 authority lost。

`legacy_sessions` 只展示 session/job/version、phase、owner kind 与记录时间，
不输出旧项目路径、log 路径或客户协议。旧 schema 缺少的时间字段为 null，
其 null 不证明旧 monitor 从未启动。旧 session 和名称消费继续保留，查询不探测
进程、不恢复 owner、不删除记录、不重放任务。
phase 与业务成功分别解释；启动或 wait 权威不明时不能据此推断 `done`。
公共 `status/task/history` 继续按现有契约使用，详情通过此专用查询取得。

### 跨任务列表

```bash
sched execution list --project documents --limit 50 --json
sched execution list --batch <full-batch-id> --backend text-worker --phase unresolved --json
sched execution list --owner-status unreachable --json
```

列表查询支持 `--project`、`--batch`、`--backend`、`--phase`、`--owner-status`、
`--limit`（1–500，默认 50）和 `--cursor`；不接受 `--version`。
`--batch` 按现有规则解析完整 ID 或同名最新批次；续页须解析到同一个 ID。
backend 筛选实际 attempt 的 `backend_id`；phase 筛选已保存 attempt 或旧 session 的
phase（`prepared/launching/running/exited/not_started/unresolved/reserved/log_bound`）。
尚无 attempt 的 `not_reserved` 等诊断标签不是已保存 phase。
owner 状态取已记录的 `unknown/responsive/unreachable/lost`，无持久 owner 为
`not_applicable`。全部历史 job/version 均可浏览，包括普通任务与旧 session。

顶层为 `schema_version:1,query:execution_list`，包含 `database_schema`、Unix 秒
`observed_at`、`consistency:live`、`complete`、`filters`、`limit`、`items`、
`truncated` 与 `next_cursor`。每项带 batch ID/name/revision、project、task/job ID/version、
owner_status、`attempt`、`legacy_session` 和同一版本的 `diagnostics`；不存在的记录为 null。
公开 owner 不含 token/endpoint，不输出原始 task spec 或旧客户路径。

按 job 的固定插入顺序向后分页；cursor 绑定筛选、node/state 和第一页的上界 job，
拒绝参数漂移或上界身份丢失。续页不纳入后来插入的 job，重新从第一页查询才能看到。
每页使用自己的私有只读快照；期间已有记录的状态或筛选归属可能变化，页面不会重复
返回已走过的插入位置，但多页不是冻结快照。`complete:true` 仅用于一次查询返回全部
匹配项的第一页；续页即使 `truncated:false` 也保持 `complete:false`。
多页结果用于历史浏览，不能合并后作为完整当前态或写操作前置条件。
查询兼容 schema 1–7，不升级完整旧库，不连接、重建或清理 owner。

### 本机能力与前置检查

```bash
sched capabilities --json
sched daemon check --json
```

`capabilities` 不读取配置或 state，不启动 child 或连接 owner，只报告执行查询的本机。
顶层为 `schema_version:1,query:execution_capabilities`，带 sched/interface version、
query_host 和 observed_at。`backends` 为 `subprocess/linux_fd/linux_fd_owner`，每项含
`status:available|unavailable|unknown`、稳定的 `reason`、`declared` 和 `verified`。
只有 available 才填充 verified；声明的能力不能当作实际可用证据。
reason 为 null、`non_posix/non_linux/native_unavailable/native_interface_mismatch/`
`kernel_fd_exec_unavailable/owner_primitives_unavailable/probe_failed`。
native 检查编译接口及 execveat/close_range；持久 owner 另检查封印 FD、/proc 身份、
abstract UNIX socket、peer credentials 和 poll。不返回进程身份值或异常原文。
这是本机前置能力证据，不证明某个 executable、输入、配置或未来 child 已可执行。

`daemon check --json` 复用现有检查并保留计算节点限制。顶层为
`schema_version:1,query:daemon_check`，包含 sched_version、node、query_host、observed_at、
fake、passed、summary（ok/warn/fail 计数）与 checks。检查项保留 item/detail/level，
增加稳定 id、可空 subject 和 performed；展示文本不作为机器字段解释。
id 为 `configuration/execution_backend/user_identity/state_writable/node_state_writable/`
`gpu_probe/venv/project_git/file_limit/disk_free/terminal_tools`；subject 标识 backend kind、
venv 或 project。fake 模式跳过的检查不能作为实际验收证据。
`terminal_tools` 保留稳定 ID，仅说明后台 daemon 的启动方式，`performed:false`；
不探测终端托管工具，也不验证 Slurm 租约或资源约束是否有效。
有 fail 时返回 1，其余返回 0；主机守卫拒绝仍为 2。文本输出保持现有格式。
该命令包含原有的目录写入与资源检查，不属于纯只读查询；网关不得执行。

0.2.2 另按 backend ID 检查管理员配置的 executable 和每个允许项目的 root。
新增检查 ID 为 `execution_executable` 与 `execution_project_root`；`subject` 为 backend ID，
`project` 为项目名或 null，`reason` 为稳定错误码或 null。这些新增项保留既有
item/detail/level/performed 字段；其他检查的字段与 subject 含义保持原契约。
检查以有界流式读取验证普通文件、SHA-256、读取中变化和 ELF 标识，
按实际启动策略解析 root 并检查目录打开和搜索权限。文件执行位不作为原始文件的
必要条件，因为启动使用独立封印副本；检查不读取任务输入、不保留执行 FD、
不连接 owner、不打开数据库、不创建 attempt 或 child。fake 仅跳过原有资源探测，
不会跳过这些管理员文件检查。

reason 为 `executable_missing/symlink/not_directory/unreadable/io_error/not_regular/`
`too_large/changed/digest_mismatch/not_elf/probe_failed`（各项完整值均以
`executable_` 开头）；root 为 `project_root_missing/symlink/not_directory/unreadable/`
`io_error/unsearchable/probe_failed`；非 Linux 为 `non_linux`，performed 为 false。
错误不输出路径或异常原文。通过只证明本次读取时的部署条件，不验证 ELF 动态加载、
任务输入或未来启动；实际 prepare/start 继续执行原来的完整校验。

`request` 保留原写命令的 stdout、stderr 和返回码。`cancel` 及其 request 包装
没有 `--json` 选项；客户端保留原始输出，不能将展示文字作为稳定结果字段。
接受取消意图也不代表子进程已退出，最终事实仍由 task 与 execution 查询确认。

已消费 attempt 的 `retry` 明确拒绝；显式 `resubmit` 在旧任务完成清理并进入
终态后创建新 job/version 和新 attempt，保留旧观察。启动后的自动失败重试与
节点重启重排均禁用。daemon 原 owner 丢失时保留未解决尝试和资源；确认进程组
消失后记 `interrupted`，不推断真实退出码或成功。意图提交前的崩溃记 `not_started`。
`linux_fd_owner` 在原服务存活时可以恢复真实 wait；恢复发现 prepared 时废弃该准备，
不补发 start。0.2.1只读查询兼容写 schema 1–7，不迁移旧库或探测服务。

## 可选 Linux native 构建

默认安装和普通 subprocess 运行不需要编译器。可选 native 源码和构建定义都在
`sched` 仓库内，使用 Python 标准扩展构建工具，不从客户仓库复制源码：

```bash
SCHED_BUILD_NATIVE=1 python -m pip install -e '.[test]'
python -m pytest -q -rs tests
python tests/run_execution_accept.py
```

独立安装验收在无客户仓库的副本中构建并安装 wheel，再从无关目录调用安装后的 CLI。
需要开发工具 `setuptools>=61`、`wheel` 和 `pip`：

```bash
python tests/run_install_accept.py
python tests/run_install_accept.py --native
```

第二条同时检查 native 安装，以及编译 native 后再次默认构建不会夹带扩展。

公共 CI 分别检查默认安装和显式 native 安装，两者均无需另一仓库或合同路径。
发布、部署准备与 schema 回退边界见 [execution-rollout.md](execution-rollout.md)。
故障、取消、timeout、重启和未知启动结果的约束见
[execution-boundary.md](execution-boundary.md)。普通 daemon 不把外部程序返回的
业务 receipt 当成子进程 wait，也不把 backend 不可用当成重试普通命令的理由。

## 候选显式恢复策略

候选版本通过 [recovery-policy.md](recovery-policy.md) 的显式 smoke-bound retry policy
在旧尝试结算/进程组清理后创建新的 job/version/attempt；普通 max_retry 仍为 0，
原 attempt 不重放，未知结果继续保留。该 opt-in 接口与 schema 8 不属于已发布 0.2.2。
