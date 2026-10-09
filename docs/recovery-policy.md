# 断点恢复与 smoke 门禁

本页描述自 0.3.0 发布、当前源码继续维护的显式通用恢复接口；0.2.2 不提供这些字段。
包含 checkpoint、smoke 门禁、显式新版本恢复 FIFO、剩余显存阈值、分级、持久无进展
策略和前台 daemon/supervisor。正式来源与验收见 [0.3.0 说明](releases/0.3.0.md)；
这些能力不授权与外部任务共卡，后文仍保留外部占用拒绝和原执行权威守卫。

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
daemon 重启不会将 interrupted recovery job 原地改回 pending；显式 retry policy 可以授权新版本。

应用 OOM 报告不能提供 wait 或进程组清理权威，也不能修改 scheduler 状态。
被 SIGKILL 或无法捕获 OOM 的程序只能恢复最近一次已落盘断点；应用应周期保存。

## 查询与验收

`sched recovery <batch-id-or-name>:<task-id> --json` 是独立的只读 schema 1 接口，
默认查询最新版本，`--version N` 查看历史结算和队列 lineage；checkpoint 始终表示 group
当前现存的最新断点，不能把它当作指定历史版本的快照。输出包含 enabled、mode、binding_sha256、smoke_job_id、smoke_ready、
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

## 显式自动恢复 FIFO

在 smoke 和正式任务的 recovery 声明中同时增加相同的 retry policy：

```json
{"retry": {"oom": true, "interrupted": true, "cooldown_sec": 30, "max_attempts": 0}}
```

省略 retry 时完全禁用自动恢复。显式空对象使用上述默认；两类开关必须为布尔，
至少启用一个。cooldown_sec 允许 0–86400 秒，max_attempts 为 0–100000 的整数，
0 表示不限次数，正数包含首个原始版本。策略参与 smoke binding，不能在运行后改变。
普通 max_retry 仍必须为 0，不承载此策略。

OOM 只有在原退出记录为非零、旧进程组清理完成且断点有效时才授权新尝试。
应用可提交结构化 oom report；没有 report 时可以使用真实失败退出后的 OOM 分类和
已验证的周期断点，但日志或 checkpoint 不能提供退出/清理权威。真实信号退出、
节点重启或确认 owner 丢失且原进程组消失，可按 interrupted 开关创建新版本；
未知连接或旧进程组仍存活时保留原任务与资源，不创建新版本，不推断 rc/rusage。
已认证的 not_started 准备可授权新版本，但不会补发原 start。首次落盘前的干净中断允许从初始状态开始；已经记录的断点丢失或损坏则拒绝，
不能将遗失进度当作从未保存。没有有效 checkpoint 的 OOM 不自动补跑。

结算、决策、新 task/job version 和队尾记录在同一事务内提交，并用唯一 predecessor
保证幂等；发布失败回滚全部新版本写入。旧 job 状态、失败和原始退出记录保留，
旧 execution attempt 永不重新 start。新版本排队时尚无 attempt，真正启动才创建新 ID。
queue lineage 与结算证据有保留/不可改写守卫。取消意图、状态/版本变化和批次退役
优先于创建 successor。自动排队不授予立即派发权；项目 GPU 禁用、配额、drain、
节点守卫与未决 launch marker 仍优先。

该批次全部普通 group 首轮结束后，按持久 seq 派发延后 FIFO；队首处于 cooldown
时后面的延后 group 不能越过。运行中的队首已完成 admission，可以在其他资源上派发
后续 group。再次 OOM 追加新版本到队尾，而不是重置旧 job 或保留最初 rowid 优先级。
等待顺序、轮次、首次排队时间和 checkpoint 摘要跨 daemon 重启保留。

达到次数上限、策略不允许或 checkpoint 无效时不创建 successor，batch 按既有失败
规则收敛；sched recovery 的 settlement 返回具体 denied reason。该命令还返回
queue 的 seq、predecessor_job_id、root_job_id、round、queued_at、not_before 和摘要，
不暴露私有路径或应用 payload。既有 status/task/history schema 和 wait_reason 枚举不变。

该恢复功能集在 schema 9 完成引入；当前候选写 25、完整只读 1–25，见
[reference](reference.md)。旧 schema 的 recovery 队列/结算返回 null，只读查询不迁移。
0.2.2 不识别 schema 9，不能将旧二进制接回新写库。

验收见 [test_recovery_queue.py](../tests/test_recovery_queue.py)：真实普通 subprocess、
linux_fd 和 linux_fd_owner 均验证 A OOM → B/C 完成 → A 新版本恢复；持久 owner
在 dispatcher 重建后仍返回原始 wait。另覆盖重复 OOM 队尾、事务注入失败、取消、
冷却、次数上限、首次落盘前中断、损坏/丢失断点、未知/未清理原尝试和 schema 迁移。

Smoke binding also covers normalized task resources, duration, probes and parallelism. A CPU smoke cannot authorize a differently configured GPU run. Candidate bindings created before this schema-8 change must be recreated with a new smoke; ordinary jobs are unaffected.


## 候选显存准入与无进展策略

项目配置 `projects[P].gpu_admission` 显式开启新准入，省略时保留原分配语义：

```json
{"gpu_admission": {"min_free_gib": 12, "allow_external_occupancy": false}}
```

空对象也是开启，默认门槛固定为 **12 GiB**，不随 GPU 总容量变化。30 GiB 只是可显式
覆盖的示例；`min_free_gib` 必须是 0–4096 范围内的有限正数。`allow_external_occupancy`
默认 false；设置 true 才允许在可完整归属的外部 compute 进程占卡时共用。
这不是独占保证，也不停止外部程序。显存门槛不是应用峰值估计；声明的 `vram_gib`
和缓存峰值较大时，仍需预留更大的预算。配置/CLI 的显存容量下调仍是上限，
不能用较大的物理总容量绕过；显式较小门槛可用于普通任务，恢复 tiers 仍独立取较大值。

nvidia-smi 的 topology、compute、memory.free 与 utilization 必须完整、可归属并保持
拓扑一致。未知进程组、无 compute 归属的利用率、不可读或过期采样均拒绝。
本项目未清理残留、未决 owner、releasing、quarantined 和人工 ignore 均不能被共用
策略绕过。unmanaged 只有重新确认实际存在可归属的外部进程时才有此显式准入；
空样本不会自动撤销 registry 的未决状态。

派发前采样，创建子进程前再采样并读取热配置；慢探测不持有 SQLite writer。
样本最多使用 5 秒。事务中的 gpu_jobs 预留覆盖同 tick 和既有启动，每个共享绑定
保留完整预算，即使 memory.free 已反映部分占用也再次保留，宁可延后而不透支。
独占 sentinel、共享总容量/任务数上限、项目配额、硬亲和、GPU 开关和 drain 仍有效。
外部程序可在采样后继续分配，因此准入不保证应用不会 OOM，恢复仍靠有效断点。

`recovery.retry` 增加三个参与 smoke binding 的字段：

```json
{"min_free_gib_by_round": [12, 24, 30], "no_progress_sec": 1800, "max_no_progress_sec": 0}
```

`min_free_gib_by_round` 默认 `[12]`；允许 1–16 个非递减、有限、12–4096 GiB 的值。
首轮取第 0 项，延后 queue round=1 取第 1 项，超出长度保持最后一项，不降低项目门槛。
可用显存未达该轮要求时继续等待资源改善；不修改应用精度、科学参数或输入。

`no_progress_sec` 默认 1800 秒，0 关闭通知周期；`max_no_progress_sec` 默认 0 表示不限
等待时间，正值仅停止等待中的新版本，不强杀运行中的尝试。两者为有限非负秒数，上限一年。
应用 checkpoint payload 摘要改变才算进展；相同 OOM 断点、daemon 重启、坏/丢失断点
都不能重置持久计时。到达停止条件，新版本 blocked/recovery_no_progress，旧退出事实保留。

通知需显式加入 `notify.on: ["recovery_no_progress"]`。新事件含 job/root ID、round、
last_progress_at、observed_at 与 stopped，使用持久 outbox，渠道失败后重试；command/email
可能至少一次投递，接收端用 event_id 去重。file 同一 event_id 使用固定文件名，已确认事件
不重新变成未读。`sched recovery --json` 的独立接口增加 watch；已有 status/task/history
schema 和 wait_reason 枚举保持不变。schema 8 查询不迁移且 watch 为 null，写入原子升级到 9。

验收见 [test_gpu_admission.py](../tests/test_gpu_admission.py)。实际 GPU 验收须在独立的
非生产计算环境执行；本次 fake GPU + 真实 CPU/native 子进程不能当作真实 CUDA 压力证据。


## 候选前台 daemon 与 supervisor

在配置指定的计算节点执行：

```bash
sched daemon foreground
sched daemon foreground --supervise --restart-delay-sec 3 --max-restarts 0
```

foreground 保持命令前台，拥有一个真实 daemon 子进程。`--supervise` 才开启意外退出后
重启；delay 范围 1–300 秒，max-restarts 为 0–100000，0 不限。正常退出不重启。
这是整个节点一个 dispatcher 的监督者，不是每块 GPU 再建一个 daemon，也不持有应用 wait。

监督者通过原 Popen 的 poll/wait 确认其子进程退出后才允许启动替代 daemon，不能用过期
心跳、卡顿或陌生 PID 判死。停止控制读失败或未知时，保留运行中的 child，禁止再重启。重复监督者由私有 mutex 拒绝；仍然运行、归属不明或其他
physical host 的 daemon lease 均不能被抢占。已验证本机 owner 确已死亡时可立即重新
核对并接管 exact lease；新鲜 ownerless 锁仍保留原启动宽限，ABA 变化仍拒绝。

`sched daemon stop` 先持久化当前 supervisor 的停止请求，因此 daemon 已死、正处于
重启 delay 时也不会再拉起。SIGINT/SIGTERM 发给前台命令时，监督者只通知它自己持有
且未 wait 的子进程优雅停止；不按心跳里的 PID 强杀。超时保留 child/lease/control，
报告未完成。stop 保持原取消语义；需要运行任务自然结束时使用 drain --stop-when-idle。
排空标记跨意外重启继续有效，正常排空退出不再启动；resume 仅解除标记，不创建 daemon。

应用由新的 dispatcher 按既有恢复契约处理：持久 owner 可认证重连并保留同一 attempt，
未知 wait/未清理进程组仍保留任务和资源；只有已有退出/清理结算才授予新的 task version。
事务发布失败留下的 pending settlement 在下次 tick 重试，不重新 start 原 attempt。
连续 tick 异常退出现在返回失败给监督者，正常 idle、drain、stop 与 lease 失去仍正常结束。
foreground 不接受 request 包装，避免长驻监督被当成普通幂等 mutation。

完整故障与兼容验收见 [recovery-acceptance.md](recovery-acceptance.md)。这些候选能力
尚未正式发布或部署；不能用本地验收推断生产状态。
