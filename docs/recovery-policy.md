# 断点恢复与 smoke 门禁

本页描述候选版本中显式启用的通用恢复接口；0.2.2 不提供这些字段。
当前包含 checkpoint 和 smoke 门禁。持久 OOM 轮转、剩余显存阈值与 supervisor
仍是后续开发项，不能将下文协议视为已支持自动补跑。

## 任务声明

每个独立 group 对应一个单命令 task，显式设置 `max_retry:0`：

```json
{
  "id": "group-a",
  "cmd": ["python3", "worker.py"],
  "git": false,
  "max_retry": 0,
  "recovery": {
    "protocol": "sched-recovery/v1",
    "mode": "smoke",
    "code": {"worker.py": "<sha256-of-worker>"},
    "config": {"settings.json": "<sha256-of-config>"},
    "inputs": {}
  }
}
```

示例摘要必须替换为真实的 64 位小写 SHA-256。`code` 至少包含一个文件；
三个文件集合不允许重复路径，合计最多 64 个文件，每文件最多 256 MiB。
路径相对于 task cwd，要求规范 POSIX 相对路径，不接受符号链接。
大型输入可声明其不可变清单，客户程序负责验证清单所指数据，sched 不加载科学对象。
普通 Git 指纹仍覆盖已跟踪代码与 dirty 内容；`git:false` 的代码覆盖由声明决定。

正式任务使用相同 task ID、命令、cwd、环境、runtime、产物规则、代码、配置和输入，
将 `mode` 改为 `run`，增加 `smoke_job_id`，填入具体的成功 smoke job ID。
可从 `sched task <smoke-batch>:<task> --json` 的对应版本记录取得 ID。
不要用批次显示名称作为 smoke 授权。不同 group 可以在 smoke 批次各有同名 task。

提交和启动均校验文件摘要。持久 binding 覆盖 task ID、producer fingerprint 和
三个声明集合，不包含运行模式和 smoke job ID。代码、配置、输入、命令或环境漂移后
拒绝启动及同 spec resubmit；修改后重新 smoke，再提交绑定新 smoke 的正式批次。
正式任务只在指定 smoke job 的已记录状态为 `done`、真实记录 `rc=0`、mode 为 smoke、
binding 相同且有 `smoke_ok` 报告时放行。`skip`、日志、应用文件和未结算 report
不能代替实际成功退出。smoke 失败时正式任务保持 pending，用户修复后重新提交。

## 客户程序协议

调度器注入保留环境变量 `SCHED_RECOVERY_CONTEXT`；任务与 batch env 不得声明它。
普通任务和公开 `linux_fd` / `linux_fd_owner` backend 使用同一上下文，FD4 identity
保持原格式。配置 backend 仍固定管理员 argv/env，禁止继承任务环境，仍须 `max_retry:0`。

```python
from gsched.recovery import CheckpointStore

store = CheckpointStore.from_environment()
progress = store.load() or {"next": 0, "results": []}
# 客户程序完成一段工作并保存；payload 必须是可序列化的 JSON。
store.save(progress)
# 可捕获 OOM 后先保存已有进度，再报告并以非零状态退出。
store.report("oom")
```

smoke 程序必须验证正常运行、模拟可恢复 OOM、保存和加载后继续执行，最后报告
`store.report("smoke_ok")` 并以 0 退出。scheduler 只核对报告和退出事实，
客户程序仍负责 smoke 内容的充分性。可复现通用例子见
[recovery_worker.py](../tests/fixtures/recovery_worker.py) 和
[test_recovery_protocol.py](../tests/test_recovery_protocol.py)。

`save` 写临时文件、fsync、原子替换再 fsync 目录。单份 JSON 记录上限 1 MiB；
模型权重、数组等大文件由客户程序原子保存，JSON progress 可以记录其摘要和恢复位置，
sched 不解释这些内容。记录绑定 producer job 和不可变 binding，并校验 payload 摘要；
损坏或不同 binding 的断点拒绝加载，不能悄悄从头覆盖。

checkpoint 位于 scheduler 管理的独立 recovery 目录，每 task 保留最新一份，
每 job 有独立 report；新 job version 使用相同 task 的 checkpoint，保留失败历史。
它们不是最终成功产物，不参与 artifact SKIP，也不被 `clean` 或启动前产物清理删除。
本阶段默认保留，不自动过期；没有 CLI 删除接口时不要直接删除 state 目录。
恢复任务禁止同版本 retry 和 clean 后 skip 重排，显式 resubmit 创建新版本。
本阶段 daemon 重启不会将 interrupted recovery job 原地改回 pending。

应用 OOM 报告不能提供 wait 或进程组清理权威，也不能修改 scheduler 状态。
被 SIGKILL 或无法捕获 OOM 的程序只能恢复最近一次已落盘断点；应用应周期保存。

## 查询与验收

`sched recovery <batch-id-or-name>:<task-id> --json` 是独立的只读 schema 1 接口，
查询最新版本。输出包含 enabled、mode、binding_sha256、smoke_job_id、smoke_ready、
checkpoint 的 absent/verified/invalid 状态与 payload_sha256，以及当前 job 的 report。
不输出应用 payload 或私有 checkpoint 路径；smoke_ready 只表示门禁记录成立，
不表示资源、配额或派发许可已满足。已有 status/task/history 的 JSON schema 不变。

Linux 回归：

```bash
python -m pytest -q tests/test_recovery_protocol.py tests/test_execution_policy.py tests/test_execution_state.py
```

测试运行真实的通用 CPU 子进程，覆盖 smoke、OOM 后进度保存和新版本继续完成，
并注入原子替换失败、文件摘要变化、符号链接、断点损坏、假成功和环境伪造。
不需要 CUDA、客户仓库或生产节点。
