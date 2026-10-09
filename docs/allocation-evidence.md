# 候选 allocation 与分层执行证据

源码候选合同为 `sched-allocations-v1`，引入 schema 16。当前候选写 schema 25、
完整只读 1–25，兼容范围见 [reference](reference.md)；包版本为 0.5.0 候选，
不代表正式发布或生产已升级。

## 分配身份

最终派发事务在 active/latest running claim、依赖、指纹与 SKIP 判断之后，
外部启动/清理之前，提交随机 allocation ID 和同 job 单调 ordinal。
每次普通 retry 即使复用 job/version、重置 retries、在同一秒启动，也产生不同 ID。
SKIP 不生成 allocation；旧库迁移只加空表/NULL 指针，不补写历史身份或 wait。
allocation 是启动意图，不证明已经出生进程；启动失败/未知仍保留它，不能凭它重放。

不可变记录绑定 instance、完整 batch/task/version、job、spec SHA256、指纹，
以及 CPU/内存声明预留、实际 gpu_jobs 装箱预留和 backend 绑定摘要。
GPU UUID 只使用计算节点已记录、五秒以内的完整拓扑样本；缺失/过期为 unknown，
fake GPU 明确 simulated。索引/UUID/预留均不证明物理进程占用。
不保存 env、命令或 backend 认证 token/endpoint；不推断 worker 身份。
运行中的租约 monitor 为新分配保存 `lease_identity`，绑定原 lease/instance ID 和当时
记录的 allocation_state；没有 monitor 时为 null，历史记录不回填。租约持续验证见
[daemon lease](daemon-lease.md)。显式 CPU 模式还保存 `cpu_binding`，容量/存储准入
可保存各自观察；这些记录本身不证明 worker 的物理占用或硬隔离，仍保留
`hard_isolation:false`。正向内核隔离验收与生产切换独立记录。

jobs 的当前 allocation 指针参与 revision；回到 pending 时清空，历史记录不删。
重新启动前再分配。成组取消/依赖更新检查不可变历史，不把 retry 清空当前字段
当作从未启动；public backend 原始 not_started 必须精确链接原 allocation/attempt。

## 分层观察

append-only 事件包含序号、前一事件摘要、数据和来源时间；更新/删除由 DB 触发器拒绝。
它们保留来源事实，不替代已有执行 owner、真实 wait、取消、释放或结算规则。

| 层 | 保存的来源事实 | 不代表 |
| --- | --- | --- |
| resource | 初始声明/装箱预留、GPU 记账释放 | affinity/cgroup 硬隔离或物理卡已空闲 |
| execution | 原公开 attempt/identity 绑定 | 进程已启动或可重新 start |
| owner | 原持久 owner 的公开身份 | owner PID 就是 GPU/scientific worker |
| process | 原 owner 对直接 child 的观察；普通 Popen 对命令链 supervisor 的真实 wait | 科学验收、应用 worker 的身份或 sidecar RC 的权威 |
| monitor | scheduler 记录的 probe_ready/probe_failed/probe_invalid | 原始进程 rc=0 |
| scheduler | 当时状态/rc/失败分类/取消等变化 | 原 wait 或硬件故障根因 |
| artifact | 精确 allocation 的不可变首次 validation ID 引用 | 当前文件仍有效、科学 gate 或复验结算许可 |

普通 supervisor 的 wait 与 scheduler 采用的 RC 分开记录；应用 sidecar 覆盖展示 RC
不能改写原 wait。SIGKILL 路径保留原 Popen 已经 wait 的 pid/start-token/returncode，
上限 1024 个内存记录；只向精确原身份交付一次，不从 PID、日志或产物恢复。
缓存缺失/淘汰、daemon 重启、身份不匹配均为原 wait 不可用，不能制造 rc=0。
cache 本身不证明 group cleanup，也不改变原 poll_rc 的兼容返回值。

首次 validation 的 completion key 和证据包含新 allocation ID，避免同秒/同版本
retry 碰撞。旧 key/原 validation 不重写；仅产物复验还须原 allocation 指针匹配，
不能拿前次成功 wait 给后次运行结算。终态重新结算只追加来源事件，不覆盖原退出。

## 只读查询

```bash
sched allocations '<full-batch-id>:<task>' --version 1 --limit 20 --json
sched allocations '<full-batch-id>:<task>' --allocation-id '<32-hex-id>' --json
```

查询使用私有 DB/WAL 快照，不迁移、探测硬件、连接 owner、加载客户程序或读取任务文件。
`effect:none`、`settlement_authority:false`、`worker_identity_inferred:false`。
完整旧 schema 返回 migration_required，不补判历史；空新记录不表示从未启动。

摘要支持 1–100 条按 allocation_id 的实时 keyset 分页；cursor 与 allocation-id 互斥。
跨页不是完整当前快照。精确 ID 只读取同任务/版本记录；事件最多 1000 条、
相关首次验证最多检查 1000 条，分别报告 events_truncated/artifact_references_truncated。
输入/响应/读取证据受 4 MiB 限制，超限报错，不把截断视作完整历史。
完整产物证据继续使用 artifact-validations --validation-id；此查询只返回不可变引用。
原 status/task/history 字段与 wait_reason 不变，客户端须先协商独立合同。

## 验收边界

[CPU/CLI 验收](../tests/run_allocation_accept.py) 在 Linux 计算节点的独立 state
检查非零 wait、零 wait/产物失败、ready 监控/非零 supervisor wait、fake GPU
记账释放、普通 retry 的独立 allocation、只读与私有 daemon 重启不重复执行。
[回归](../tests/test_allocation.py) 使用合成 fixture 检查 owner service 与直接 child
区分、秘密排除、迁移不回填、旧 key 保留、同秒 retry、分页与 pid/start-token 绑定。
真实 CUDA/worker PID 归属与生产切换没有由这些测试取得证明。
