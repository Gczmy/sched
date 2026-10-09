# 候选 CPU 自动容量与声明预留

这是最初引入于 schema 17 的源码候选，尚未部署到生产。实际接收端须通过 `identity --json`
协商 `sched-cpu-capacity-v1`；包版本不代替能力检查。auto 本身不实现 per-job affinity/cgroup；
后续 schema 18 的 [显式 CPU 亲和](cpu-isolation.md) 独立启用，也不是 cgroup 硬隔离。

## 配置和计账

```json
{"cpus_total":"auto","cpus_auto_max":120}
```

`cpus_total` 为非负整数或精确字符串 `"auto"`，省略/null/0 保留原来的“不限制总
CPU 预留”语义：CPU-only 只受 `max_cpu_jobs` 并发限制，GPU 任务不受 CPU 总量限制。
正整数为显式声明预算，不自动缩小；大于已知 affinity/Slurm 边界时记录告警。
`cpus_auto_max` 默认 null，只能与 auto 同用，为 1..1048576 整数，作为额外上限。
这些 CPU 配置可热更；从 auto 切回固定/零值时同一补丁将 cpus_auto_max 设为 null。

auto 取下列可信计数的最小值，并报告来源差异，不乘任务数、不平均分配多节点总量：

- 当前 daemon `sched_getaffinity(0)` 的逻辑 CPU 数。
- 校验原 job/UID/节点/不可变绑定后的单节点 Slurm allocation CPU 数。
- 同一启动来源中的 `SLURM_CPUS_ON_NODE` 和 `SLURM_CPUS_PER_TASK`（若存在）。
- 可选 `cpus_auto_max`。

多节点 job 总 CPU 数不是本节点容量；没有可用的本节点声明时 auto 为 unavailable。
per-task 声明作为保守上限，不通过 NTASKS 放大。Slurm 字段语义见官方
[srun 环境变量说明](https://slurm.schedmd.com/srun.html)。缺失 affinity、非法 CPU 声明、
过期/不可读控制器观察、原 job 未确认、内核上下文变化或锁存 invalid，均暂停 auto
新派发，不回退到 0 或 max_cpu_jobs。无 Slurm 的 standalone 原始上下文可用 affinity。

[租约策略](daemon-lease.md) 默认 unknown=pause；只有 cgroup membership 无法核对，
而原 job/内核身份和 CPU 声明均可确认时，显式 allow/observe 才可能允许 auto。
这仍是有诊断告警的未知 cgroup，不是有效物理租约证明。即便 observe/allow，auto
也不会容忍已确认 invalid、未知控制器或未知内核身份。固定/零值仍沿租约策略控制，
不可用 auto 规则静默改变它们的兼容语义。

CPU/GPU 任务共享总预算；显式 resources.cpus 优先，CPU-only 默认 1，GPU 默认
gpu_job_cpus。运行中的新 allocation 使用不可变 cpu_reservation，不随后来的默认值
变化缩水。旧历史缺少 allocation 且 GPU 未声明 CPU 时，auto 不猜当时默认值，
报告 legacy_running_cpu_reservation_unknown 并停止新派发，保留原 running。
热缩容可造成 used 大于 total；运行任务不被暗杀，等待真实结束释放预留。

每 tick/派发入口更新容量，清理旧启动标记之前、慢检查之后的最终启动 CAS 之前读取
当前 CPU 热配置并重算运行预留。不在 SQLite writer 事务内调用 Slurm。外部 Slurm
采样仍按租约 interval 缓存，有采样/配置读取/启动间窗口，不是原子物理授权。

## 只读查询与兼容

```sh
sched cpu-capacity --json
sched status --json --include-cpu-capacity
```

独立 query 包含 schema_version=1、contract、instance_id、node、effect=none、configured、
mode、auto_max、effective_total、available、sources、observed_upper_bound、warnings、
unknown、allocation_state、lease_dispatch_allowed、invalid_latched、used、reservation_error、
observation、lease_id、unit、zero_means、hard_isolation=false、admission_granted=false。
available=false 时 effective_total 即便非 null 也只是已知候选边界，不能授权派发；
used=null 表示旧运行预留不可确认，不是零使用量。allocation 保存当时容量报告。

默认 status.cpu 保持严格 `{used:整数,total:整数}`，已知 auto 用解析后整数；auto
容量/用量不可确认时省略整个可选 cpu 字段，不发字符串或 0 冒充无限制。只有显式
`--include-cpu-capacity` 才增加嵌套扩展（要求 --json）；旧插件不应默认请求这项扩展。
配套插件确认接受可选 cpu 缺失及原 wait_reason=cpu，未新增等待枚举。admission-explain
同时展示容量、预算与未知原因；所有诊断仍不授予执行权。

查询只读私有 DB/WAL 快照和 daemon 已发布的容量观察，不初始化/迁移 state、不探测
网关 affinity/Slurm。观察最多 64 KiB，hash、instance/node、配置摘要、当前精确 owner
及 PID/start token 按已有健康规则绑定；过期 45 秒、未来时间、配置落后、owner
不匹配或观察损坏均为 unavailable。未运行 daemon 的固定/零值只展示配置声明，
observation.error 明确缺少观察；不伪造当前物理容量。auto 本身未新增 schema 17 之外的迁移。

## 验收边界

[纯逻辑/私有合成状态回归](../tests/test_cpu_capacity.py) 覆盖保守解析、unknown/invalid、
多节点、零值兼容、冻结运行预留、被动读取、配置落后/过期/PID 绑定与启动前门禁。
[Linux CPU/CLI 验收](../tests/run_cpu_capacity_accept.py) 使用独立临时 state，先记录
真实租约来源，再用 state 外的 fixture scontrol 改变回答，验证热缩容、CPU/GPU
共同预算、查询失败、失效锁存及零值回退。fake GPU 不占真实 GPU，不修改真实
Slurm job、生产 daemon 或配置；这些证据不替代真实 CUDA/生产部署验收。
