# 通用 execution API

> 本文对应 execution isolation 分支的 v1 实现；尚未发布或切换生产实例。

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
`record_invalid` 或 null；尚在正常执行的任务不因未退出就标为 authority lost。

`legacy_sessions` 只展示 session/job/version、phase、owner kind 与记录时间，
不输出旧项目路径、log 路径或客户协议。旧 schema 缺少的时间字段为 null，
其 null 不证明旧 monitor 从未启动。旧 session 和名称消费继续保留，查询不探测
进程、不恢复 owner、不删除记录、不重放任务。
phase 与业务成功分别解释；启动或 wait 权威不明时不能据此推断 `done`。
公共 `status/task/history` 继续按现有契约使用，详情通过此专用查询取得。

`request` 保留原写命令的 stdout、stderr 和返回码。`cancel` 及其 request 包装
没有 `--json` 选项；客户端保留原始输出，不能将展示文字作为稳定结果字段。
接受取消意图也不代表子进程已退出，最终事实仍由 task 与 execution 查询确认。

已消费 attempt 的 `retry` 明确拒绝；显式 `resubmit` 在旧任务完成清理并进入
终态后创建新 job/version 和新 attempt，保留旧观察。启动后的自动失败重试与
节点重启重排均禁用。daemon 原 owner 丢失时保留未解决尝试和资源；确认进程组
消失后记 `interrupted`，不推断真实退出码或成功。意图提交前的崩溃记 `not_started`。

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
