# sched 使用参考（权威版）

> 面向 AI 代理与用户的**功能与命令权威查阅文档**。改调度器行为时同步更新本文件。
> 版本基准：本次提交的 source/tests。新增诊断接口属于当前源码候选，不表示已发布或部署；使用前核对目标 CLI 的命名合同。

---

## 版本查询

`sched --version` 和 `sched version` 输出 `sched <version>`。
自动化使用 `sched version --json`，返回 `schema_version: 1`、`query: "version"`、
`sched_version`、`database_schema` 的 `write`、`read_min`、`read_max`，以及命名 `contracts`。
查询只报告当前加载代码的版本和 schema 兼容范围，不读取配置或状态数据库，
也不表示运行中的 daemon 已切换到该版本。数据库实际版本不由此查询推断。

## 集成身份、回执与幂等提交

公开 JSON 保持 `schema_version:1`；命名合同见 [integration-contract.md](integration-contract.md)。
`sched identity --json` 只读查询持久 `instance_id`、配置节点和查询主机。
没有状态或未迁移旧 schema 时返回 `available:false` 和原因，查询不会初始化数据库。
`task` 与单任务 `execution` 增加同一快照中的 `project`、`instance_id`；旧 schema 的身份为 null。

`sched request-status <request-id> --json` 返回 `not_found/unknown/delivered/done`，
原绑定摘要、结果码及可用的结构化结果。旧回执可有 null result；输出压缩不会删除新结构化事实。
不存在回执不证明未执行；unknown 保留原请求，不推断 wait 或触发重试。

当前源码候选支持 `request-status <id> --wait-sec N --expect-instance <instance> --json`
和 `request-status-many <id>... --json`（1..100 个不同 ID，支持同样的等待选项）。
N 为 0..60 的有限秒数；只读查询原 RID，不重投，超时返回 `wait_timed_out:true`。
回执追加来源、持久化、投递/批次确认和时间信息；`done` 是请求结算，不是训练完成。
多 ID 的数据库事实来自同一私有快照，ticket 回退不承诺文件系统原子快照。
读取失败返回非零，不当作 not_found。完整字段与证据保留规则见集成合同。

`request-validate` 使用与 `request` 相同的 envelope 和完整子命令，只校验格式，
不读写 state、不预占 RID、不检查当前态。`request --json` 是可选结构化结果，
不会改变原绑定；缺字段/命令语法错误在写入前返回 64，CAS 冲突仍为 65，未知仍为 75。
“本次调用未执行”不能用于否认该 RID 的历史 effect；文件、确认、主机与 CAS 执行时仍重验。

`sched submit <batch.json> --request-id <id> --expect-instance <instance-id> --expect-project <project> --json`
绑定有限规范 JSON、项目和可选预期身份，相同绑定保留原 batch ID；改变绑定返回 64。
网关只投递文件；`delivered`/`persisted:false` 不表示已入库，daemon 在同一事务中保存批次与终态回执。
结果不确定返回 75，不自动重投。不能与 `--dry-run` 或外层 `sched request` 嵌套。
`sched request` 的 `--expect-instance`、`--expect-project` 在写事务内校验；项目预期只适用于 batch/task。
没有新增参数的旧 request 绑定保持原样。当前原根健康候选写 schema 25，完整只读范围为 1–25；
已发布 0.4.0 写 schema 10；候选 writer 11–24 均不能回接 schema 25。原映射绑定表于 22 引入，
23 记录设备必需标志，24 守卫新 v2 MIG 能力证据，25 守卫原根健康事件，均不回填旧事实。包版本尚未变更，能力须查询实际部署的合同与 schema。

`sched allocations <batch>:<task> [--version N] [--limit 20] [--cursor ID] --json`
提供独立的 `sched-allocations-v1` 不可变分配摘要；`--allocation-id ID` 读取同任务/
版本的有界分层事件和首次 validation 引用。每次真实启动意图独立关联，即使普通
retry 复用版本也不复用 allocation。原 wait、监控声明、scheduler 分类与产物/资源
事实分别记录，不把 owner/supervisor PID 当作科学 worker。只读、不迁移、不探测
当前进程/文件、不重新结算；截断/旧库/超限边界见 [allocation-evidence](allocation-evidence.md)。

`sched artifact-validations <batch>:<task> [--version N] [--limit 20] [--cursor ID] --json`
只读查询首次验证摘要；`--validation-id ID` 读取同任务/版本的一条完整冻结证据，与 cursor
互斥。查询不读任务文件、不迁移、不重新结算；旧库明确 unavailable，不回填历史 wait。
实时 ID 续页不是完整快照；通过记录不等于取得科学验收或重新结算权，详见
[不可变产物验证](artifact-validation.md)。

`artifact-revalidate <full-batch-id>:<task> --validation-id ID --yes` 必须由完整 task CAS
和 instance 的 `request` 包装，仅追加复验事件；显式 `--settle` 才按原始 wait/规则/文件
证据重新结算。code=0 表示事件提交，须检查 effect.settled，不等于任务成功。
`artifact-revalidations <batch>:<task> --json` 只读查询事件，`--event-id ID` 读取一条完整证据；
有限系统退避、显式重开和全部守卫见 [仅产物复验](artifact-revalidation.md)。

## 候选任务 DAG 与受审计依赖更新（schema 15）

任务可使用 `depends_on:[{"task_id":"a","version":1}]` 指定同批次来源，
以及任务级 `depends_on_exact` 指定已有同实例来源（格式同批次 exact）。新批次
本地来源只接受存在的 task/version 1，允许前向引用，拒绝重复和环。接受事务在
所有任务插入后冻结来源 instance/batch/task/version、job/spec/fingerprint，以及
目标 job/spec；目标运行时缓存指纹仍按现有启动/clean 合同更新，不重绑来源。
随后检查包含任务边、精确批次门槛及旧动态名称门槛的混合图。
环检查最多 100,000 个 job/batch 节点及 100,000 条边，超限拒绝，不将部分检查当作通过。

派发、实际启动及 SKIP 前复用批次 exact 的来源核验：当前声明产物、公开执行的
原始 wait/cleanup、所有代际运行/未知 attempt/旧 session/launch marker 都影响准入。
任务未满足时为 `status --json` 的 `pending` / `wait_reason:dependency`；
task/history 保持其原始 waiting_dep 状态合同。不自动取消 running。
`continue_independent` 下 A→C、B→D 的 A 失败仍允许 B/D 执行，C 等待原 A v1。
默认 `freeze` 仍冻结整个批次；等待依赖的任务尚存时 opt-in 批次保持 active。
retry 的同版本可能改变来源当前态，但 resubmit/同名新批次从不替换固定选择；
resubmit 继承生效的冻结来源，不隐式改为 latest。
任务 SKIP 的生产者还必须具有相同任务/精确批次绑定；改变选择不会复用旧绑定
生产的指纹成功态。旧名称兼容缓存不因此改成精确选择，科学输入仍须反映在任务规范中。

`sched task-dependencies <batch>:<task> [--version N] --json` 使用独立合同
`sched-task-dependencies-v1`。返回当前冻结清单/摘要、当前与上一事件 ID，以及已记录
失败源/路径；`--event-id <id>` 返回该版本的一条不可变历史事件，不授予执行权。
默认 limit=100，允许 1..1000；cursor 是 0..10000 的实时偏移量。路径最多 32 层、
每个被访问 job 保留一条路径，不枚举所有路径；depth_truncated 单独表示深度截断。
游标容量不足时 truncated=true、next_cursor=null、cursor_limit_reached=true，不能当成完整结果。
批次级门槛须另查 batch-dependencies。续页、来源状态和目标 revision 独立变化，
不得合并成完整当前态。查询不读产物/marker，不迁移；记录成功仍需派发时重新检查。
现有 status/task/history 严格字段保持，status 中 depends_on 仍只列旧批次名称。

依赖更新只能通过完整 task/instance CAS request：

```sh
sched request <request-id> --json --expect-kind task --expect-id <batch-id>:c \
  --expect-status pending --expect-version 1 --expect-revision <revision> \
  --expect-instance <instance-id> -- dependency-update <batch-id>:c \
  --dependencies-json '<exact-source-list>' --yes
```

输入是最多 64 KiB 的**内联** exact JSON，不是文件路径；字节与命令一起绑定原 RID。
使用完整 batch ID 和精确来源 version；`[]` 显式解除任务自身依赖，但不解除批次门槛。
只更新当前未启动的 pending 版本：拒绝已有 start/pgid/retries/rc、当前 attempt、
任一代际运行/未知 attempt/旧 session/marker、活跃或不可知进程组、当前分配或待处理控制请求，以及旧执行元数据。
当前版本即使 retry 清空启动字段，已有 allocation 仍拒绝更新；已结束的旧版本
allocation 保留为历史，不单独阻止未启动新版本的更新，其他跨代际守卫仍生效。
CAS、来源冻结、环检查、事件、revision 与 request 回执同事务；失败回滚事件/变更，
保留拒绝回执。新事件链接旧事件，原 spec 和原始批次 exact 列不被覆盖。不训练、
不删产物、不创建新任务版本、不终止任务；同 RID replay 不增加事件，unknown 仍为 75。
direct 调用/缺少 instance 为 64，冲突为 65；网关拒绝，即使设置 foreign-write。
`--reopen` 只显式重开 continue_independent 的 blocked 批次，不重试任何失败任务。

schema 15 仅新增 append-only task_dependency_events、索引、保留触发器与 revision
触发器。迁移不回填/绑定旧任务、名称、wait 或科学事实，现代等待态和未知回执保留。
旧库 query 仍为只读、task_dag_supported=false；有声明但无接受绑定时不授予派发权。
能力取决于实际 schema/命名合同，不依赖尚未变更的包版本号。

## 候选精确事实与成组 pending-only 取消

`sched task-facts <full-batch-id> --tasks-json '[{"task_id":"a","version":1}]' --json`
使用独立合同 `sched-task-facts-v1`。只接受完整批次 ID、显式精确版本和 1..100 个
不同任务，不按名称/latest 解析；内联 JSON 最多 64 KiB。每次从同一私有只读快照
返回 batch revision/status、instance、job/spec/fingerprint/history 绑定、所有代际的
已记录启动/退出事实及拒绝原因；不读 marker、日志、产物或进程，也不迁移旧库。
`recorded_never_started` 只表示记录内未发现启动，不是取消许可；`cancel_ready:null`、
`launch_markers_checked:false` 明确尚未完成计算节点的文件核验。

每个任务最多 1000 代，整个选择最多 10000 条记录及 4 MiB 规范证据；超限返回非零，
不返回一个冒充完整历史的截断结果。旧 schema 缺少完整身份合同明确拒绝授予未启动证明。
历史失败、retry 清空当前 started_at/pgid/rc、未知请求、launch intent、旧 native 元数据、
恢复记录和不可变 validation 都不能仅凭当前 pending 忽略。公开执行的原始
not_started 身份/观察全部匹配且 owner 已关闭才可证明未出生；不从 PID 缺失推断。

取消必须把查询中 `tasks[].binding` 的完整数组原样冻结为内联 `--tasks-json`，
通过一次源批次 CAS：

```sh
sched request <rid> --json --expect-kind batch --expect-id <full-batch-id> \
  --expect-status <batch-status> --expect-revision <batch-revision> \
  --expect-instance <instance-id> -- cancel-pending <full-batch-id> \
  --tasks-json '<frozen-binding-list>' --yes
```

计算节点在同一写事务重验最新 pending/waiting_dep/waiting_quota、所有代际历史摘要和
启动文件；marker/profile/log 或旧 rc 文件存在、不可读或扫描不完整均拒绝，不删除文件。
原始 not_started 已验证的前置日志可保留，不当作 worker wait。任一成员冲突整组不变；
未知原 RID 为 75，不换 RID 重放。直接调用/缺 instance 为 64，冲突为 65，未确认是 1。
不终止 running、不发送信号、不释放其资源、不写控制请求、不删产物、不创建新版本。
一次 CAS 可因多个 job 更新累计增加 revision，不保证只加一；逐任务重复用旧 revision
不是成组操作。同 RID 查询/重放恢复原 durable 回执及清单摘要，不重复取消。

普通 `cancel` 仍可取消运行任务，不等同于这个严格接口。新增查询/回执沿用 schema 15，
没有新增持久表或迁移，status/task/history 严格字段保持。跨实例替代计划、目标 spec/RID、
reservation 和 lineage 由客户端保存，不承诺跨系统原子替换。

## 候选资源准入与逐卡装箱解释

`sched admission-explain <batch>:<task> [--version N] --json` 使用独立合同
`sched-admission-explain-v1`，不改 status/task/history 字段或 schema 15。
可在网关查询：私有 DB/WAL 快照提供当前预留和任务绑定，计算节点 daemon 的独立
带时间戳观测提供 GPU 容量、冻结/防抖、fresh VRAM、物理主机内存和未触页预留。
查询不初始化/迁移 state，不探测网关硬件、不连接 owner、不检查任务产物、不派发。

预算及选卡函数与实际派发共用，不从展示用 wait_reason 推导。返回全部同时不满足的
CPU 总量/CPU-only 并发、主机内存静态预算与物理余量、批次 max_parallel、项目 GPU
开关/并发 job 配额，以及逐卡硬亲和、quarantine/ignore、冻结/利用率防抖、独占占卡、
全局/卡级/项目级 pack 上限、声明/缓存峰值、容量/safety 和 fresh VRAM/外部占用原因。
共享选择仍按放入后归一化负载和亲和优先；小任务可补位，不抢占。优先级排序最多
读取 10000 候选，超出时 order.truncated=true、rank_before_gates=null，不伪造完整位置。

daemon 观测最多有效 90 秒，fresh VRAM 仍只有效 5 秒；未来时间、身份/配置不匹配、
缺失或不可读均明确 unknown。物理内存观测还绑定当时 running 的 job/spec/pgid；
运行集合变化后不把旧未触页余量当作当前余量。DB 与观测不是原子跨文件快照。
最多 512 卡、10000 个 running 记录及 4 MiB running spec、单目标 spec 1 MiB；超出
返回非零。查询只读：不删文件、不改 revision、分配、状态、回执或训练次数。

`resource_fit` 只表示这个有时效的资源快照能否容纳；相关观测未知时为 null。
`admission_granted:false` 始终不授予启动权。实际依赖产物、marker/执行身份、恢复 smoke、
最终 CAS/内核权限仍需派发时核验，unchecked_dispatch_gates 明确列出；旧失败/等待的
科学原因也不能由此补判成功。retry 退避、批次状态、当前代际和 drain 单独显示。

CPU/内存/显存预算是调度器声明预留，不是不可越界的 per-job cgroup 硬限制。
默认 off 时 cpus_total=0 仍只回退 CPU-only 并发，不限制 GPU 任务的 CPU 总量；
显式启用候选 [CPU 亲和](cpu-isolation.md) 后，有限 CPU-ID 池是独立 gate，
admission-explain 增加 cpu_isolation 并复用池/claim 判断，池满仍报告 CPU 拒绝。

`sched cpu-scopes [--scope-id ID] [--limit N] [--cursor ID] --json` 提供独立命名合同
`sched-cpu-scope-state-v1`。默认读取已记录 scope 摘要；精确 ID 返回完整有界事件链，
与 cursor 互斥。limit 默认 20、范围 1–100，每个 scope 至多 256 事件/单项 1 MiB。
记录绑定原 instance/job/version/allocation/lease/CPU 集合、唯一创建 intent 和原 inode；
查询不探测 cgroup/Slurm/进程、不修改 DB 或授予启动/wait 权限。`recorded_phase` 不是
当前内核健康，`cpu_release_recorded_ready` 也不替代实际清理和原执行守卫。
旧完整 schema 1–18 返回 migration_required，不迁移或回填；分页是实时 keyset，不是
完整快照。已有显式 cgroup/delegated_root 冷配置，没有 scope 写 CLI；默认 off 查询
为空，正向内核/设备验收仍未完成。详见 [scope 生命周期](cpu-scopes.md)。

`sched device-scopes [--scope-id ID] [--limit N] [--cursor ID] --json` 提供独立
`sched-device-scope-state-v1` 合同，只读原设备意图/程序绑定和有界链。schema 21 不回填
设备事实；schema 1–20 返回 migration_required，不升级或探测 BPF。安装未知保留原
CPU/GPU；原 CPU removed 引用和设备 released 记录均满足后才放行资源。此项没有
实际设备安装接入或写 CLI，默认 JSON/FD4 不变；详见 [设备记录](device-scopes.md)。

`sched device-inventory --json` 是计算节点显式只读设备探测，不读 DB 或迁移；网关
禁止执行，即使设置 foreign-write override。UUID/PCI/driver minor/节点核对成功才
报告完整映射，runtime_probed=true 但 admission/wait/physical boundary=false。返回
失败不等于空映射；MIG 未知不能用于整卡放行。显式冷配置接入见[设备 controller](device-controller.md)，不表示真实隔离验收已完成。
`device-inventory --with-mig-capability --json` 另协商 `sched-device-inventory-mig-v1`，
返回 v2 完整原 NVML 证据及外层 mig_support；仅原 GetMigMode 明确不支持才区分
N/A，其他错误仍 unknown。默认 v1 不增加字段或 NVML 探测；原证据/有限采样与
schema 24 兼容边界见 [MIG 能力](mig-capability.md)。

`sched scope-health --json` 协商独立 `sched-scope-health-state-v1`，被动读取原 daemon/
lease 的 root/CPU/NUMA/可选父 BPF 前置观察。30 秒诊断窗口、绑定/配置/租约变化或
过期明确 unknown，ready 也不证明实际 scope join/BPF/GPU 权限。admission-explain
可复用已记录 pool/claim 判断；不探测查询主机、不预占或授予启动权，默认健康 JSON
不变。schema 25 不增加表或回填，旧库返回 migration_required；详见 [原根健康](scope-health.md)。

`sched device-inventory-bindings [--scope-id ID] [--limit N] [--cursor ID] --json`
协商独立 `sched-device-inventory-binding-v1`。schema 22 冻结原完整映射、allocation/
CPU inode/device intent/lease 与时间摘要，不回填历史。默认摘要、精确 ID 完整映射；
单记录 256 KiB、包括关联原链的查询总预算 4 MiB，limit 1–100、默认 20，实时 keyset
不是完整快照。查询不采样硬件、迁移旧库或授予启动权；schema 1–21 明确 migration_required。
纯新鲜重验不换卡、不刷新旧 topology、不补造 BPF 安装；实际接入尚未完成。

精确原 owner 的租约检查最多有效 45 秒，缺失/过期/配置滞后为 unknown，不探测网关。
GPU quota=0 仍是无限制。
旧 free 卡容量未知时的兼容放行和共享降级独占明确警告，不因解释接口隐式改变策略。
后续存储源码候选在该查询嵌套独立 storage 报告，复用存储派发决策；相关必需证据
未知时 resource_fit 为 null。`sched storage-explain <batch>:<task> [--version N] --json`
使用独立 `sched-storage-explain-v1`：最多 30 秒的计算节点观察，不探测网关，
缺失/配置或运行集合变化明确 unknown。默认关闭的 `storage_admission` 可热更新，
检查输出与 state 文件系统的字节/inode、可知当前 UID quota，容量不足不清理科学文件。
严格 status/task/history 和 wait_reason 不变；全部配置/范围见 [storage-admission](storage-admission.md)。
持续租约验证已有独立候选；硬隔离的设备安装与正向验收仍未完成，不能把这些查询
当成租约有效或物理隔离的证明。

## 1. 心智模型

```
批次实例 (batch, 提交时生成唯一 id = 名字-时间戳)
  └── 任务 (task, id + version)
        └── 作业 (job, version 对应的执行记录)
```

- **普通 `sched submit` 生成新的批次实例**（即使同名）；带原始 `--request-id` 的幂等提交复用同一批次。同名旧实例不会被覆盖，
  会以 `[blocked]` 等状态留在列表里 —— 用 `sched discard` 退役，忽略即可。
- **项目归属与选卡**：提交必须带 `"project"`；`gpu_affinity_hard=true` 时，该项目任务只从其亲和卡中选卡。硬亲和不会反向为项目保留 GPU；需要项目间独占隔离时，所有竞争项目必须使用互不重叠的硬亲和集合。
- **共享装箱三要素**（想单卡多任务必读）：
  ```json
  "resources": {
      "gpu_share": true,      // ← 缺省 false=独占整卡！共卡必须显式声明
      "vram_gib": 0.5,        // ← 声明峰值显存 (共享任务必填)，装箱按此记账
      "profile_key": "..."    // ← 可选: 调度器学习的历史实测峰值, 自动取 max(声明, 实测)
  }
  ```
  ⚠️ 最常见错误：写了 vram_gib 但忘写 gpu_share → 任务独占整卡，单卡单任务。
- **指纹、stage checkpoint 与重跑**：指纹覆盖命令、真实工作目录、声明的合并环境（`task_default_env < batch.env < task.env`）、产物规则、git HEAD、tracked 文件的 staged/unstaged 内容与 runtime；任意 untracked 输出和未声明的宿主环境不参与 producer 指纹。stage 仅在产物有效且 job 私有 checkpoint sidecar 的 producer 指纹匹配时跳过；stage 命令返回 0 后必须先通过其完整产物规则才写 sidecar，任务最终收敛时会再次校验任务级与全部 stage 产物。`force_rerun:true` 绕过任务 SKIP 和 stage checkpoint。2026-09-07 指纹格式升级后，旧 producer/checkpoint 无法匹配，新提交或重开的任务会重新执行一次；已完成任务不会因此自动入队。
  tracked 代码未提交改动也会改变指纹；untracked 源码不会，因此生产代码应纳入版本控制。强制重跑见 R4/R5。
  对任务操作一律使用 `<batch-id-or-name>:<task>`；先精确匹配完整 batch id，否则选同名最新批次，裸 task 无效。

---

## 2. batch.json 字段参考

### 批次级

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | str | ✅ | 批次名（1..128 位安全 ASCII 标识符；id 自动追加时间戳）|
| `project` | str | ✅ | 项目名，必须在 config.projects 注册 |
| `mode` | str | ✗ | `mix`（缺省）；旧 `strict` 只识别历史持久态，不接受新提交 |
| `priority` | int | ✗ | 项目内批次优先级，默认 0；整数越大越先考虑 |
| `failure_policy` | str | ✗ | 源码候选：`freeze`（默认）或 `continue_independent`；网关与 daemon 均须支持 `sched-batch-policy-v1` |
| `cwd` | str | ✗ | 缺省为该批次的 project root；支持 `{ROOT}`/`{PROJECT:name}`/`{VENV:key}` 模板 |
| `env` | obj | ✗ | 字符串环境变量映射（最终覆盖，优先级高于自动注入）；变量名/值必须安全，shell bootstrap 与动态加载注入变量会被拒绝 |
| `depends_on` | [str] | ✗ | 上游批次名数组；每项同样须为安全标识符，上游 done 前挂起 |
| `depends_on_exact` | [obj] | ✗ | 候选：显式 instance/batch/task/version 清单；网关与 daemon 均须支持 `sched-batch-dependencies-v1` |
| `force_rerun` | bool | ✗ | true = 全部任务绕过任务 SKIP 与 stage checkpoint |
| `notify` | bool/obj | ✗ | 覆盖通知配置 |
| `sweep` | obj | ✗ | `{matrix:{参数:[值]},max_parallel:N}` 笛卡尔积展开 |

候选失败策略不改变 task 终态或原始执行事实。默认 `freeze` 在任一最新任务失败时
冻结后续派发，running 不自动取消。`continue_independent` 仅对显式启用的批次生效：
最新 pending/running/interrupted、旧 running、未决启动 marker 或非终态 execution attempt
尚存时保持 active，允许其余可派发任务继续；最终有失败仍 blocked，不把部分成功标为 done。
失败策略本身不创建任务 DAG；后续 schema 15 的任务依赖见前文。科学依赖/验收仍由客户控制。

`sched batch-policy <batch-ref> --json` 使用私有只读快照查询策略，不初始化/迁移 state；
schema 10 及以前返回 `freeze` 和 `source:legacy_default`。新策略不追加到现有严格
status/task/history JSON，使用独立合同 `sched-batch-policy-v1`。
写操作必须在计算节点通过完整 batch ID、状态和 revision 的 `sched request` CAS 执行：

```bash
sched request policy-01 --json --expect-kind batch --expect-id example-20261008000000000 --expect-status blocked --expect-revision 17 -- batch-policy example-20261008000000000 --failure-policy continue_independent --yes --reopen
```

省略 `--reopen` 只改变策略，不重开旧 blocked；`--reopen` 仅允许有最新 pending/等待/running
的 blocked 批次和 `continue_independent`，不重试失败任务、不生成新 version。done/discarded
和历史 strict 批次禁止修改；queued 可修改策略但不绕过依赖解锁。策略变化递增 revision（同值不递增），
同 RID 原绑定重放只返回原回执。失败策略不放宽主机、租约、原执行身份或未知启动守卫。

### 精确批次依赖（源码候选）

示例选择同一实例内一个来源版本；instance ID 必须从实际 `identity --json` 取得，
batch ID 从原提交回执取得，task/version 从结构化任务查询取得，不自动使用 latest：

```json
{"depends_on_exact":[{"instance_id":"0123456789abcdef0123456789abcdef","batch_id":"upstream-20261008000000000","tasks":[{"task_id":"task","version":1}]}]}
```

每个来源必须显式列出非空 tasks；最多 256 个来源、10,000 个不同 task/version，
冻结 JSON 最多 4 MiB。version 为 1..2^63-1 整数，不接受布尔、latest、名称选择器、
未知字段和重复项；batch ID 只按精确主键查找。跨实例及历史 strict 来源拒绝提交。
本地提交和网关 inbox 接受都在 submission gate/事务内重验来源与混合依赖环。
接受时另冻结 job ID、完整 task spec 摘要和 fingerprint；这些绑定不可原地改写。
提交前的来源预检和 dry-run 不代替接受时重验，网关无 DB 预览明确 sources_checked=false。

所有 exact 和旧名称条件是 AND。exact 只检查列出的来源 task/version，不把其他
任务的失败或整批 blocked 当作失败；同名新批次和同批次新版本不会替换所选版本。
所选版本必须当前为 done/skip，绑定匹配、声明产物仍有效，且该来源 task 所有代际
无 running/interrupted、未决 attempt/旧 native session/launch marker。public backend
还必须有一致的已记录原始 wait/cleanup；不从 PID、日志或当前产物补造 wait。
历史 native 标识不能获得新执行权。已记录普通 done/skip 使用既有调度成功语义，
不是科学 acceptance 或不可变文件发布；本功能不阻止外部写文件，也不冻结重试尝试。
显式 retry 可改变所选同一版本的当前态；原始失败记录仍由首次验证接口保留。

解锁、每 tick 的 active pending 复核及实际派发均检查 exact；失效时 pending
转 waiting_dep（查询为 pending/wait_reason=dependency），恢复后回 pending，无新版本。
running 不取消、不迁移。另一个候选本轮启动来源的新代际后，后续依赖候选会再次
检查，不能使用派发前的旧缓存。旧 blocked 批次仍只显式重开。

`batch-dependencies <batch-ref> --json` 使用独立合同 `sched-batch-dependencies-v1`，
私有只读快照返回 exact 冻结绑定及每项 recorded_status/binding_matches/recorded_clear；
旧名称标记 kind=legacy_name/dynamic_latest=true，显示此次快照解析到的 batch ID。
不读当前产物或 launch marker，不探测 owner；这些事实不构成派发许可。
默认最多 100 项，`--limit` 为 1..1000，`--cursor` 为非负偏移量；同时返回 total、
truncated/next_cursor、batch_revision/binding_sha256。续页为实时查询，不能合并成
完整当前态；来源状态可能变化，即使下游绑定摘要未变也不能证明各页属于同一快照。
旧 schema 查询 source=legacy_only，不迁移、不补绑。
schema 10 及以上写升级不归一化已有等待态或递增对应 revision；更旧 schema
仍按既有格式兼容规则处理历史等待别名，不制造原始 wait 或新的执行授权。

原 status/task/history 字段集不变；status.batches.depends_on **仅含旧名称条件**，
不能据其为空断言无依赖，完整依赖须协商新合同后查询。配套旧插件仍能解析 status，
但没有 exact 依赖展示，插件功能独立开发。这里仍是批次级门槛，不是任务 DAG；
后续 schema 15 的受审计任务依赖更新与失败路径见前文，批次原始 exact 列仍不可覆盖。

### 任务级

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `id` | str | ✅ | 批内唯一的 1..128 位安全 ASCII 标识符 |
| `cmd` | [str] | ✅* | 自由格式命令；`{VENV:x}`/`{ROOT}` 模板可出现在任意 token |
| `stages` | [obj] | ✅* | 多阶段替代 cmd：`[{cmd,artifacts}]` 顺序执行，失败即停；匹配 producer 指纹的私有 checkpoint 可跳过已成功 stage |
| `cwd` | str | ✗ | 任务级目录覆盖（任意目录，J 类）|
| `env` | obj | ✗ | 任务级安全字符串环境变量（最终覆盖）；与批次级采用同一注入变量拒绝规则 |
| `duration_min` | num | 推荐 | 有限正数分钟；超时看门狗 kill |
| `max_retry` | int | ✗ | 自动重试次数（缺省 1；0=失败即 blocked）|
| `depends_on` | [obj] | ✗ | 候选任务 DAG：本批次显式 task_id/version，初始只允许 version 1；不使用名称 latest |
| `depends_on_exact` | [obj] | ✗ | 候选任务 DAG：已有同实例 instance/batch/task/version 来源，接受时冻结 |
| `resources.gpu` | 0/1 | ✗ | 0=CPU-only；缺省占 1 GPU |
| `resources.gpu_share` | bool | ✗ | true=允许共享装箱（需全局 co_locate 开启）|
| `resources.vram_gib` | num | 共卡必填 | 峰值显存声明（GiB）；独占时用于容量校验 |
| `resources.profile_key` | str | ✗ | 历史实测峰值键，装箱取 max(声明, 实测) |
| `resources.cpus` | int | ✗ | 正整数 CPU 核数声明；缺省时 CPU-only=1，GPU job=`gpu_job_cpus`（默认 8）|
| `resources.host_mem_gib` | number | ✗ | 有限正数主机内存预留（GiB）；缺省 `host_mem_default_gib`，CPU/GPU 任务均计入 |
| `resources.disk_gib / disk_inodes` | number / int | ✗ | 候选非负存储声明预留，缺省 0；启用 storage_admission 时每输出文件系统记全额，不是硬 quota |
| `runtime` | obj | ✗ | `{conda_env:"名"}` ∥ `{venv_alias:"名"}` ∥ `{prefix:"路径"}` 必须且只能选一个；别名须注册、conda env/prefix 目录须在提交时存在；参与指纹 |
| `progress_regex` | str | ✗ | 从日志尾部提取进度，status 展示 |
| `artifacts` | obj | ✗ | `{key:{path,...}}`；规则：存在(缺省)/`"check":"json"`/`min_bytes:N`/`has_key:"键"`/`json_equals:{"键":值}`/`regex:"模式"`；内容校验有读取/执行上限，symlink 与特殊文件拒绝；命中→SKIP |
| `execution` | obj | ✗ | 通用冷 backend 与输入 FD 绑定；见 [execution-api.md](execution-api.md) |
| `probes` | obj | ✗ | 仅任务级 `{fail_on_log,ready_on_log}` 日志门控 |
| `_force_rerun` | 内部 | — | `force_rerun` 的落库字段，输入中不要提供 |

\* cmd 与 stages 二选一。

候选 `json_equals` 自动解析 JSON，以点分隔的对象键选择值，逐项比较有限 JSON 值，
布尔值不等于数字，JSON 数字 `1` 与 `1.0` 相等，null 不等于缺键；数组顺序保留、
对象键顺序无关。最多 64 项，规则组总上限仍为 64 KiB，内容读取上限仍为 1 MiB。
嵌套键名中的点没有转义语法。规则参与已有 producer 指纹。
网关与接收 daemon 都须确认 `sched-artifact-rules-v2`；旧版本会拒绝这个新规则。

`sched artifact-check <batch>:<task> --version N --json` 仅在配置的计算节点只读检查
该版本当前产物；规则通过返回 0、不通过返回 1，异机返回 2。它不初始化 DB、不修改
任务状态、不清理或重训，不是“复验结算”接口。没有声明时 checks 为空，passed 为 true，
不意味着产物来源或科学验收通过。初次检查的细节进入 daemon/task 日志；旧失败
不能靠本次检查还原。存在/min-size 检查不读取大文件，sha256 为 null。

标识符必须匹配 `[A-Za-z0-9][A-Za-z0-9._-]*`，且不能是 `.`/`..`。已移除或未实现的输入会拒绝：批次级 `gpus`、任务/阶段级 `retry_transform`、阶段级 `probes`；GPU 需求写 `resources.gpu`，probe 只写在任务级。

通用执行层的配置、输入与生命周期见 [execution-api.md](execution-api.md)。
普通任务缺省仍使用 subprocess；显式 `execution` 任务只使用管理员允许的 backend，
不可用时明确拒绝，不回退执行原命令。daemon 不解释客户的协议或科学字段。

旧 `mode: "strict"`、`native_exec_profiles` 与 `_native_exec_*` 仅供持久态迁移识别，
新提交不能借其恢复旧执行路径。旧 session 的名称和一次性尝试记录保留；失去原始
owner/wait 权威时不推断成功、不重放，也不因代码迁移直接删除其运行记录。
旧合同迁移限制见 [native-integration.md](native-integration.md)。
内部数据库 schema 与公开 CLI JSON 的 `schema_version:1` 分别演进。
`sched execution <batch>:<task> --json` 单独返回通用尝试、identity 与原始 observation，
并附逐版本只读诊断、旧 session 摘要；可用 `--version N` 过滤。
不改变既有 `status/task/history` 的字段契约；JSON 详见 execution API。

### config.json 相关（代理只读，调参报告用户）

`sched init` 要求显式填写 daemon 的计算节点名，新配置使用 `example` 项目和
`python` 解释器别名；已有配置不自动改名。使用 `{ROOT}` 的配置必须设置
`default_project`，不再回退到某个特定部署的项目名。

| 键 | 说明 |
|---|---|
| `projects[P].gpu_enabled` | 布尔值，省略为 `true`；`false` 禁止新 GPU 提交与手动 GPU 重跑，暂停排队 GPU 派发，运行中任务和 CPU-only 不受影响（热更新） |
| `projects[P].gpu_quota` | 项目并发 running GPU job 上限；省略或 `0` = 无限制，CPU-only 不计 |
| `projects[P].priority` | 项目优先级，默认 0；整数越大越先考虑（热更新）|
| `projects[P].gpu_affinity` | 该项目的亲和卡列表；软亲和可在必要时外借其他卡 |
| `projects[P].gpu_affinity_hard` | `true` 时只允许从非空 affinity 选卡；不反向保留这些卡 |
| `projects[P].colocate / max_jobs` | 项目级共享开关 / 每卡打包密度上限（热更新）|
| `gpus[i].max_jobs` | 异构卡每卡打包上限（热更新）|
| `gpus` | 卡号或 `{idx,mem_gib,max_jobs}` 数组；省略/空数组时由 daemon 探测 |
| `co_locate / co_locate_safety / co_locate_max_jobs / co_locate_freeze_pct` | 共享总开关与阈值；默认 `false / 0.7 / 3 / 85` |
| `cpus_total / gpu_job_cpus / max_cpu_jobs` | CPU 预留预算；默认 `0 / 8 / 2`。零值只限 CPU-only 并发；显式 `"auto"` 使用原租约/affinity 的保守容量；固定正值超已知边界告警 |
| `cpus_auto_max` | 候选 auto 的可选正整数上限 1..1048576，默认 null；非 auto 必须省略/null；可热更 |
| `cpu_isolation` | 候选显式冷配置，mode=off（默认）、affinity，或 schema 20 的 cgroup + 规范绝对 delegated_root；只用外部已委派原 lease cpuset，失败不降级。亲和与待完成内核/设备验收见 [CPU 亲和](cpu-isolation.md)、[CPU scope](cpu-scopes.md) |
| `host_mem_total_gib / host_mem_reserve_gib / host_mem_default_gib` | 主机内存准入；默认 `0 / 16 / 8` GiB。total=0 关闭；其余有限非负，default 必须大于 0；支持热更新 |
| `storage_admission` | 候选 opt-in 磁盘/inode/可知用户 quota 准入；默认 enabled=false，其余余量/unknown 策略见 [存储合同](storage-admission.md)，支持热更新 |
| `lease_validation` | 候选冷配置；默认 membership=cgroup、mode=auto、unknown_policy=pause、interval_sec=30；可显式 launch_ancestry 验证原 Slurm 启动祖先/affinity（非硬隔离），失效/未知停新派发，详见 [租约合同](daemon-lease.md) |
| `idle_timeout_min` | daemon 空闲自动退出分钟数；默认 360，`0` = 禁用 |
| `notify` | 省略时关闭；可配置 batch done/blocked 的 file/email/command 渠道 |
| `conda_envs_dirs` | runtime.conda_env 解析目录（热更新）|
| `task_default_env` | 部署级普通任务环境缺省值 `{k:v}`；batch/task env 可覆盖。固定 execution backend 使用其注册的显式环境。典型普通任务用途：`{"PYTHONNOUSERSITE":"1"}` 隔离 ~/.local 用户站点污染 |
| `execution_backends` | 管理员冷配置的通用可执行文件、argv、env、项目许可与 input slots；见 [execution-api.md](execution-api.md) |
| `native_exec_profiles` | 旧持久态识别用的冷配置记录；不能授予新 strict/native 执行权限 |

`gpu_quota` 的计数单位是 job，不是不同物理卡或显存：正整数 `N` 表示该项目最多
同时运行 `N` 个已经分配 GPU 的 job；独占和共享任务均每个 job 计 1，共享时多个
计数单位可能落在同一张物理卡上。`resources.gpu:0` 的 CPU-only job 不占该配额。
项目级禁止 GPU 使用 `gpu_enabled:false`，不能用 `gpu_quota:0` 或空 affinity 模拟。
显式非空 `gpus` 与启用项目的硬亲和无交集时，新 GPU 提交/dry-run、配置更新、
daemon 前置检查与热重载拒绝。普通旧配置读取仍允许，以便查询和修正；CPU-only 提交、
软亲和、已禁用 GPU 的项目不按此交集拒绝。省略/空池是自动探测，不当作零卡配置。
检查不会自动扩池或改亲和；GPU 池/容量仍是冷配置。
切回 `true` 后原排队版本继续运行，无需重新提交。配置写入与最终派发共用 submission
gate；开关读取失败时暂停 GPU 派发，CPU-only 使用最后有效配置。入口、重试和并发
行为详见 [`project-gpu-access.md`](project-gpu-access.md)。

ready 候选按 `(项目 priority 降序, 批次 priority 降序, job 入队 rowid 升序)`
逐一尝试，两个 priority 都默认为 `0` 且可为任意整数。项目 priority 是第一排序键；
低优项目的批次 priority 再大也不能越过高优项目。该机制不抢占、不预留容量，也没有
aging/fair-share：运行中的低优任务不会被驱逐；高优候选因 quota、CPU、GPU 或
`max_parallel` 暂时不可启动时，后续候选可以补位。

启用主机内存准入后，所有 running 版本的声明预留总和加上新任务必须不超过
`min(host_mem_total_gib, MemTotal-host_mem_reserve_gib)`。分配 CPU/GPU 之前，还检查
计算节点 `MemAvailable`，扣除运行任务尚未使用的预留和本 tick 新启动任务的预留，
保留系统余量。运行进程树采用 PSS，读取失败的部分按未使用预留保守处理，不累加 RSS。
采样或配置读取失败时暂停派发。预留不是 cgroup 硬限制；任务仍应保留运行期内存保护。
`status.cpu.used` 和 `host_memory.used_gib` 都是声明预留，不是实时 CPU/PSS 用量。
候选 CPU auto 的严格 status.cpu 已知时仍是两个整数；未知时省略，不用零值表示未知。
通过 `cpu-capacity --json` 或显式 `status --json --include-cpu-capacity` 查看配置、有效
容量、来源、租约和观测时效。运行 CPU 预留冻结到原 allocation，热改默认值不缩小
已启动任务的预留；详见 [CPU 自动容量](cpu-capacity.md)。这不是 per-job 硬限制。
节点可用内存来自 daemon 最近 90 秒内的采样，未知时为 null，不在网关采样替代。

安全维护使用 `sched daemon drain` 暂停新派发，已有任务自然结束，pending 保留。
`sched daemon drain --stop-when-idle` 还会在 running 和未决启动标记清空后退出。
排空请求跨 daemon 重启保留；`sched daemon resume` 解除，daemon 已退出时再
`sched daemon start`。这些写操作只在计算节点执行，也可由 `sched request` 包装。

维护请求使用 `--expect-revision 0`，不虚构 daemon revision。例如：

```bash
sched request drain-01 --expect-revision 0 -- daemon drain --stop-when-idle
sched request resume-01 --expect-revision 0 -- daemon resume
```

每次新的逻辑操作使用新的 request-id；重放同一操作必须保留完整命令与参数。
已完成的 drain 请求在 resume 后重放不会重新排空；已完成的 resume 请求也不会
解除后来发起的排空。`daemon drain` 只接受可选 `--stop-when-idle`，`daemon resume`
不接受附加参数。控制文件和所在目录同步后才报告成功；进程中断留下未完成请求时
返回 `75`，禁止自动换 ID 重试。resume 只解除排空，不启动已停止的 daemon。

`daemon status --json` 与 `status.daemon_health` 增加可选 `request_actions` 字段，
当前值为 `daemon-start`、`daemon-stop`、`daemon-drain`、
`daemon-drain-stop-when-idle`、`daemon-resume` 的字符串数组。这是执行查询的 CLI
所支持的请求操作，不证明其他主机上的 writer 或正在运行的 daemon 版本相同。
插件接入维护按钮时需同时核验查询结果和实际 writer 的能力、节点身份与快照时效；
字段缺失的旧 CLI 仍可展示健康状态，但不能据此启用新增维护操作。
`daemon stop` 仍会取消运行任务，不用于无损排空。

⚠️ config.json 为多项目共享配置，修改须经用户确认。

---

## 3. 常见场景配方（recipes）

### R1 单卡多任务（共享装箱）

```json
{"id": "t1", "duration_min": 30,
 "resources": {"gpu_share": true, "vram_gib": 0.5},
 "cmd": ["{VENV:k}", "train.py"]}
```
检查清单：全局 co_locate 开启 ✓ → 项目 colocate 未禁用 ✓ → `gpu_share:true` ✓ →
`vram_gib` 已声明且 ≤ safety×容量 ✓ → 单卡多任务生效。
验证：`sched list-gpus` 看 `packed=n/cap`。

### R2 独占整卡

不写 gpu_share 即可（或 `colocate:false` 项目级禁用）。声明 `vram_gib` 可避免被派到小卡。

### R3 改代码后重跑

```bash
git commit -m "fix"                      # 必须 commit（未提交改动也触发重跑）
sched resubmit <batch-ref>:<task>        # batch-ref: 完整 id 优先，否则同名最新实例
```

### R4 强制全部重跑（跳过 SKIP）

batch.json 加 `"force_rerun": true` 后重新 submit；或清指纹：
`sched clean <batch-ref> --yes`（删除每个最新 `skip` task spec 声明的产物）；
一次性 `strict` native 批次不允许 clean。
clean 会清除该批所有版本的指纹，但只把每个 task 的最新 `skip` 版本重新排队；
仅允许 `done/blocked` 终态批次，并且节点上不能有任何 running 任务或其他 `active`
批次（不同批次也可能声明同一路径，在尚无 producer/path ownership 元数据前按
fail-closed 处理）；依赖等待中的 `queued` 批次可以保留。
命令先提交指纹栅栏、再删除
这些 skip 产物，最后才公开可运行状态；最新 `done` 等未重排任务的产物不会删除。
若批次已 `done`，会原子恢复为 `active` 并确保 daemon 运行；daemon 在派发前根据
最新同名批次身份协调旧 `.done` marker。删除期间会持有全局提交栅栏；daemon 的
依赖解锁、终态 skip 产物复核和最终 skip/cleanup/Popen 派发也使用该栅栏，因此不会
在删除窗口启动或复用任务。若 phase 2 失败，`skip + fingerprint=NULL` 是持久
fail-closed 栅栏，不能解锁下游或重新发布 done。不存在的产物
视为已清理，路径策略、权限或 I/O 错误则 fail-closed，批次保持终态（此前已成功
删除的产物不回滚，修复后可重试）。历史版本不会复活。

### R5 重跑失败/全部任务

```bash
sched retry <batch-ref>                    # 解锁失败终态 -> pending 重跑（同 spec, 批次回 active）
sched resubmit <batch-ref> --failed        # 失败终态任务各生成新版本排队尾
sched resubmit <batch-ref> --all           # 全部任务重跑
sched resubmit <batch-ref> --failed --dry-run   # 预览将重跑的清单
sched resubmit <batch-ref>:<task>           # 单任务精确定位
```
选择建议：想按原 spec 重跑失败终态用 retry；想生成带当前代码/runtime 新指纹的新版本用 resubmit。单任务只能在该任务所有版本均为终态时 resubmit；`done` 或 `blocked` 批次都会自动重开为 `active`，终态 marker 由 daemon 在派发前按最新同名实例协调。
历史落库 spec 在 resubmit 时会剥离任务/阶段 `retry_transform` 与阶段级 `probes`；新 batch.json 直接拒绝这些字段。

### R6 退役被取代的旧批次

```bash
sched discard <batch-ref> --yes   # 仅 blocked/queued 且无 running；证据保留
```

### R7 批量取消项目队列

```bash
sched cancel --project <project> --yes   # 覆盖 queued/active/blocked 批次的排队或运行任务
```

### R8 失败排障流程

```bash
sched diag <batch-ref>:<task> # 一步到位: 状态+命令+git+日志尾+OOM事故快照判读
sched incidents               # 全部 OOM/硬件错误快照
sched log <batch-ref>:<task> -n 50
```

### R9 查进度

status 自动展示；长任务声明 `progress_regex` 更精确。

### R10 新环境准备检查清单（提交前必做）

```bash
# 1. 创建环境后, 用绝对路径安装 (防 pip 解析到别的 env)
/miniconda3/envs/new_env/bin/pip install pkg1 pkg2

# 2. 模拟 daemon 执行方式验证 (无 conda activate), 并确认加载位置
PYTHONNOUSERSITE=1 /miniconda3/envs/new_env/bin/python -c "
import pkg1, numpy
print('import ok')
print('numpy from:', numpy.__file__)"

# 3. 在计算节点上重复步骤 2 (共享 NFS 但解释器/LD 可能不同)

# 4. 全部通过后再 sched submit; 提交时若见
#    "检测到用户站点包" 提示, 说明 ~/.local 可能有遮蔽风险
```
说明：daemon 启动任务的 env 构建顺序 = 继承 → 剥离 conda 污染键 → 注入
runtime/B13 关键子集 → task_default_env 缺省值 → batch/task env 覆盖。
若部署配置了 PYTHONNOUSERSITE=1，用户站点(~/.local)自动隔离。

---

## 4. CLI 命令大全

下表的 `<batch-ref>` 先精确匹配完整 batch id，否则解析为同名最新批次。

| 命令 | 说明 | 注意 |
|---|---|---|
| `init [--config PATH]` | 交互生成 config.json | 默认写 bootstrap state 下的 config |
| `verify <batch-ref>` | 确认批次已持久化 | 网关 submit 返回“已投递”后确认入库；完整 batch ID 可查询消费拒绝原因 |
| `submit <file> [--dry-run [--json]]` | 提交批次 | 网关推荐入口；异机写 submit_inbox，daemon 下一 tick 收编；dry-run 只读 |
| `run --project P [--gpus 1\|--cpu-only] [--cpus N] [--duration MIN] [--cwd DIR] [--out PATH] [--venv NAME] [--dry-run] -- cmd...` | 单条命令提交 | 非 dry-run 仅计算节点；不用 inbox；批次 priority 固定 0 |
| `status [batch-ref] [--project P] [--detail] [--json] [--limit N] [--cursor TOKEN] [--job-cursor TOKEN]` | 最新版本当前态 | 缺省 200，钳制到 1..1000；`--cursor` 翻批次页，`--job-cursor` 独立翻任务页 |
| `task <batch-ref>:<task> [--json]` | 单任务全部版本详情 | `--json` 输出单一、版本化 JSON 文档 |
| `batch-policy <batch-ref> [--json]` | 候选失败策略只读查询 | 写操作只能经 batch CAS 的 request；`--failure-policy freeze\|continue_independent --yes`，可显式 `--reopen` |
| `batch-dependencies <batch-ref> [--json]` | 候选精确绑定/动态名称事实 | 私有只读快照，不检查产物或 marker；--limit 1..1000，实时 --cursor 非负偏移量 |
| `task-dependencies <batch>:<task> [--json]` | 候选任务 DAG/已记录阻塞路径 | --version/--event-id；有界路径不替代派发核验，批次门槛另查 |
| `dependency-update <batch>:<task> --dependencies-json '<list>' --yes` | 候选受审计依赖更新 | 仅计算节点 task/instance CAS request；未启动版本、事务环检查、不可变历史 |
| `task-facts <full-batch-id> --tasks-json '<task/version-list>' --json` | 候选有界精确代际事实 | 私有只读快照；不读启动文件，不授予取消权；超限报错 |
| `cancel-pending <full-batch-id> --tasks-json '<binding-list>' --yes` | 候选成组未启动任务取消 | 仅计算节点一次 batch/instance CAS request；任一成员冲突整组拒绝 |
| `admission-explain <batch>:<task> [--version N] --json` | 候选只读资源/逐卡装箱解释 | 共用派发预算/选卡函数；计算节点观测有时效；unknown 不授予派发权 |
| `storage-explain <batch>:<task> [--version N] --json` | 候选只读存储解释 | 共用存储决策；字节/inode/用户 quota 范围明确；不探测网关、不授予执行权 |
| `history [batch-ref] [--status S] [--project P] [--json] [--limit N] [--cursor TOKEN]` | 终态历史（保留各版本） | 缺省 50，钳制到 1..200；cursor 用于 JSON 稳定键集分页 |
| `markers` | 批次终态 marker 一行查看 | 纯文件查询，不打开数据库 |
| `log <batch-ref>:<task> [-f] [-n N]` | 任务日志 | |
| `diag <batch-ref>[:task]` | 失败诊断（首选）| |
| `incidents [id] [--limit N] [--job ID] [--gpu N] [--json]` | OOM/硬件事故快照 | `--json` 供看板/脚本 |
| `retry <batch-ref>[:task]` | 解锁失败终态重跑（同 spec）| blocked 批次原子回 active；marker 由 daemon 派发前协调；discarded 拒绝 |
| `resubmit <batch-ref>:<task>` / `<batch-ref> [--failed\|--all] [--dry-run]` | 新版本排队尾；支持批次级批量 | discarded/queued 守卫；done/blocked 自动回 active |
| `cancel <batch-ref>[:task] --yes` / `cancel --project P --yes` | 取消 | 后者遍历该项目 queued/active/blocked 批次 |
| `discard <batch-ref> --yes` | 退役 blocked/queued 批次 | 仅拒 running；证据保留 |
| `clean <batch-ref> --yes` | 清全部指纹+删最新 skip spec 产物 | 仅非 strict 终态批次；仅重排最新 skip；done 自动回 active |
| `list-gpus` | GPU 视图（packed=n/cap）| |
| `gpu-set-mem <idx> <GiB>` | 临时覆盖 state/list-gpus 中的 GPU 容量 | 重启时会被 config 或硬件探测覆盖 |
| `gpu-ok <idx>` / `gpu-ignore <idx>` | 解除 quarantine / 静默 unmanaged 告警 | 未知 idx 拒绝；不等同于强制释放 |
| `gpu-free <idx> --yes` | 强制 GPU 回 free | 破坏性操作；操作者须先确认无受管或外部任务；自动化应使用 request/CAS 绑定 assignments |
| `config get` / `config set -f patch --yes` / `config reload` | 配置读取/补丁写入/立即热更 | 冷键拒绝；多项目共享需谨慎 |
| `request <request-id> --expect-revision N [--expect-kind ...] ... -- <mutation>` | 持久化幂等 mutation | revision 必填；request-id 与完整命令/前置条件永久绑定；GPU 还必须绑定完整 assignments JSON |
| `daemon start/stop/status/check/drain/resume` | daemon 生命周期 | start/stop/check/drain/resume 仅计算节点；`--fake` 仅供 start/check 测试；显式 override 才可跨主机 |
| `notify-test` / `notify-inbox [--all] [--json]` / `notify-ack <file>` | 通知测试与检查点 | inbox 为纯文件查询；ack 是写操作 |
| `project list` | 项目 GPU 访问策略、配额/用量 | `--json` |

破坏性命令（cancel/gpu-free/discard/config set/clean）缺 `--yes` 时返回码 1 =
未确认而非失败。

`sched run` 当前只有单 GPU 与 CPU-only 两种受支持的资源形态。GPU 任务省略
`--gpus` 或显式写 `--gpus 1`；零 GPU 必须用 `--cpu-only`。拒绝其他 GPU 数量，
也拒绝同时指定 `--gpus` 与 `--cpu-only`。
它默认采用 config 中第一个 venv，工作目录为 `{ROOT}`（`default_project` 根目录）；
`--project` 只决定归属、quota、priority 与 affinity，不改变 `{ROOT}`。需要其他项目
目录时显式传 `--cwd '{PROJECT:P}'`。`run --dry-run` 与真实提交都校验项目成员，
并要求显式提供的 `--cpus` 与 `--duration` 为正整数。命令按 argv 保留引号，
使用非登录 Bash 保留所选 venv 的 `PATH`；需要 shell 管道时显式使用 `bash -c`。
与可无状态执行的 `submit --dry-run` 不同，当前 run 预览仍要求已有且可读的 state DB。

`config set` 将读取、深合并、校验和原子替换置于同一个 submission gate，
并发补丁按顺序读取前一个已提交配置，避免相互覆盖。`notify-ack` 只接受当前
节点 `notify_inbox` 内的普通 `.json`／`.json.acked` 文件；损坏事件在查询中标记
为不可读，不中断其余事件。

### daemon 进程与集群租约

`sched daemon start` 的默认后台启动通过 `Popen(start_new_session=True)` 在独立 POSIX session
启动 dispatcher：它会脱离当前 screen/TTY，但不调用 systemd、tmux 或 screen
托管进程，也不调用 Slurm/PBS 申请新资源。`sched daemon foreground` 在前台等待
dispatcher；加 `--supervise` 可在满足所有权条件时自动重启，不会申请或迁移租约。
新启动的 daemon、执行 owner 和任务继承其启动进程的 cgroup、cpuset 与设备权限，
因此必须从目标计算租约内启动。重连既有持久 owner 不会改变它及其任务的资源上下文，
不能视为迁移到新 daemon 的租约。screen/tmux 只是进入既有租约的操作通道，
并不是资源边界。`cpus_total` 与 `resources.cpus` 仅用于 sched 内部
并发记账；默认不创建子 cgroup、不设置 CPU affinity。候选显式 cpu_isolation.mode=affinity
在用户 exec 前设置并核对原 claim 的 CPU mask，应用仍可扩大 affinity，不是硬 cpuset。
资源继承只发生在启动时；只读 `sched cpu-isolation --json` 查询原活动 claim，完整合同见
[CPU 亲和](cpu-isolation.md)。该模式不创建子 cgroup。候选显式 cgroup 模式仅在
外部已授权委派内创建唯一子 cpuset，三种 backend 在 exec 前 join，原 inode/lease
绑定、清理与未完成正向/设备验收见 [CPU scope](cpu-scopes.md)；不修改父 Slurm 资源。
候选 `device_isolation.mode=nvidia` 必须同时显式启用 CPU cgroup，拒绝 fake；默认 off
不安装。原设备 intent/完整映射/installed binding 先提交，exec 前核对原 retained handle、
真实 attachment/映射/lease，再同事务提交设备与 CPU launch_intent。恢复只观察，未知
不重装或 CPU 降级；schema 23/冷配置、preflight 和验收边界见 [设备 controller](device-controller.md)。
候选 schema 17 保存白名单 Slurm/job/step、affinity/cgroup 启动来源与检查/退出事实；
默认 auto 对有 Slurm 来源的 daemon 持续校验，未知默认暂停、已确认失效锁存停止新派发，
不杀 running、不自动迁移新租约。通用 system cgroup 不能宣称 valid；旧 daemon 不补造来源，
硬杀/DB 不可写可能缺失退出记录。显式查询、策略和恢复边界见 [daemon-lease](daemon-lease.md)。

### 稳定 JSON 与跨主机读取

候选 `daemon-lease --json` 与显式 `daemon status --json --include-lease` 另协商
`sched-daemon-lease-v1`，通过私有 DB/WAL 快照查询已记录来源/检查/退出，不探测
Slurm、不迁移旧库、不授予派发权；默认 daemon status 仍不打开 DB，默认
status/task/history/wait_reason 不变。字段、分页与容量见 [租约合同](daemon-lease.md)。

- `project list --json` 输出 `{"schema_version":1,"projects":[...]}`；每项含 `name`、有效布尔值 `gpu_enabled`、整数 `gpu_quota`（省略或 null 归一为 0）、`gpu_access:"disabled"|"unlimited"|"limited"`、`gpu_used`、`priority`、`colocate`、`max_jobs`、`gpu_affinity`、`root`。禁用不清空原配额，`gpu_used` 仍显示运行中的 GPU job 数。
- `project_gpu_disabled` 仅用于排队 GPU 任务，优先于 quota/dependency 等待原因；任务状态仍为 `pending`。这扩展了 schema 1 的等待原因枚举，启用此功能前应同步更新 `dsh-node-sched`，旧插件的严格校验会拒绝新值。

- `status --json` 固定 `schema_version:1`，`limit` 缺省 200、钳制 1..1000；顶层可含 `host_memory:{used_gib,total_gib,reserve_gib,default_job_gib,available_gib}`（启用内存准入时），`daemon_health.draining` 表示持久排空；顶层含 `batches`、`jobs`、`gpus`、`cpu`、`daemon_health`、`truncated:{batches,jobs}`、批次分页 `next_cursor` 与独立任务分页 `next_job_cursor`。先按当前态优先、再按新旧顺序有界选择批次，任务只来自已返回批次且只含各 task 最新 version，因此每个 `jobs[].batch_id` 都能在 `batches` 中解析。等待态统一为 `"status":"pending"` 与独立 `"wait_reason":"project_gpu_disabled"|"quota"|"dependency"|"cpu"|"host_memory"|"gpu"|"parallel"|"draining"|"batch_blocked"|null`，不要解析人类视图的装饰文本。`batches[].revision` 是整数；`gpus[].revision` 是整数，`gpus[].assignments` 按 `job_id` 排序且每项为 `{"job_id":...,"vram_gib":...}`。只有 `truncated.batches=true` 时才用 `next_cursor`；只有 `truncated.jobs=true` 时才用 `next_job_cursor`。指定一个批次时也可只翻其任务页，不会丢失该批次行。
- `task --json` 固定 `schema_version:1`，输出 `batch_id`、`batch_name`、`batch_revision`、`task` 与 `jobs` 版本时间线；每个版本含状态、运行结果/时间、resources、规范化 spec 与 log 路径。stdout 只含这一份 JSON。
- `history --json` 固定 `schema_version:1`，`limit` 缺省 50、钳制 1..200；顶层含 `history`、`truncated` 与 `next_cursor`，每项含 `batch_id`、`batch_name`、`task`、`status`、`version` 及运行结果/时间。
- 配置的 `node` 之外执行查询时，CLI 不初始化、不迁移、也不写源 DB；无论源目录当时是否存在 WAL/SHM，都先复制出稳定的私有 DB（及存在的 WAL）快照，再以 `mode=ro` 打开。绝不对仍可变化的 live DB 使用 `immutable=1`。`daemon status` 也可跨主机只读；`daemon start/stop/check/drain/resume` 默认拒绝。
- `daemon status --json` 是不打开数据库的只读查询，输出 `schema_version:1` 与和 `status.daemon_health` 相同的健康字段：`node`、`query_host`、`pid`、`observed_at`（Unix 秒）、`process_state`（`running/stopped/unknown`）、`health_state`（`healthy/delayed/stalled/stopped/unknown`）、`heartbeat_age_s`、`tick_ok_age_s`、`frozen`、`draining`、`read_error`。年龄不可读或不存在时为 null；`read_error` 为 null、`health_file_unreadable` 或 `timestamp_in_future`。同物理节点且 lease 与进程启动标识一致才确认进程存活，跨节点不探测本机同号 PID。原进程退出/被复用或目标本机确认 lease、PID、心跳均不存在才确认 stopped。健康要求心跳 <60 秒且成功 tick ≤90 秒；tick >90 秒为 stalled，心跳新鲜不能掩盖 tick 停滞；已确认 stopped 与读错误优先。draining 是独立派发状态，不覆盖健康故障。这个查询结果只用于展示，不改变生命周期、租约或写操作校验。
- 看板 daemon 提示必须消费结构化健康数据；不解析中文展示文本。SSH/API 错误、格式错误、缓存过期或浏览器本地 TTL 到期均显示状态未知并禁用 daemon 操作；旧采样可保留供查看。只有新鲜且确认 stopped 的状态可启用 start，不能把心跳过期当成启动依据。旧 CLI 不支持该 JSON 命令时提示未知，需配套升级查询 CLI。
- 配置的 `node` 本机执行 `status/task/history/diag/log/list-gpus` 等数据库查询时，先以私有只读快照核验 schema 版本、必需对象/列与 WAL 文件头；完整、私有的 v1–v7 WAL 库均直接进入私有快照上的 `mode=ro + query_only` 查询，不对源库执行迁移或 writer 事务。0.2.1写 schema 为 7，保留不可变 execution owner binding，新增独立确认队列与已记录健康；旧库写操作会迁移。快照遇到 daemon 写突发时最多重试 8 次并做有界退避（累计 sleep 上限 1.585 秒）；首次建库、缺损 schema、非 WAL 库或权限漂移仍进入带有界锁重试的现有 writer 初始化路径。高于当前版本的库在本机与网关查询都 fail-closed。纯文件查询 `markers`、`notify-inbox`、`daemon status` 以及 `config get` 不检查或打开数据库。混版部署时，必须先由用户人工完成旧 daemon 切换，再运行 `init_db` 或任何可能触发迁移的写命令；只读查询不会代替这个部署步骤。
- `request` 只包装 `submit`、`cancel`、`retry`、`resubmit`、`gpu-free`、`gpu-ignore`、`gpu-ok`、`daemon start/stop/drain/resume`、`config set` 和候选 `batch-policy` 写操作；未列出的 mutation 有意 fail-closed。`gpu-set-mem` 是重启时会被 `config.gpus` 或硬件探测覆盖、且未纳入 revision/CAS 的临时 state/list-gpus 记录，不由 `request` 包装。每次 request 都必须提供非负 `--expect-revision`；无目标的 submit/daemon/config 使用 `0`。task/batch 绑定所属 batch 的 `revision`，其中目标必须使用完整 batch ID（不能用批次名）；GPU 绑定自己的 `revision`，且 GPU 必须额外传 `--expect-assignments-json`（与 status 返回的已排序数组完全一致）。被包装命令使用规范顺序：目标紧跟子命令，选项随后。revision 由 SQLite trigger 在批次状态/候选失败策略、task/job 代际与 job 状态变化，以及 GPU 状态/quarantine/ignore 确认/assignment、`gpu_jobs` membership/装箱值变化时递增，所以状态值绕一圈回到原值的 ABA 仍返回 65。task 示例：`sched request retry-42 --expect-kind task --expect-id batch-20260829-000000:train --expect-status failed --expect-version 1 --expect-revision 17 -- retry batch-20260829-000000:train`。GPU 示例：`sched request gpu-42 --expect-kind gpu --expect-id 0 --expect-status assigned --expect-quarantined 0 --expect-revision 9 --expect-assignments-json '[{"job_id":"batch-task-v1","vram_gib":1.5}]' -- gpu-free 0 --yes`。
- 对数据库 mutation，业务写入与 ledger 的 done/code/output 在一个外层事务中原子提交；嵌套 submit/retry/resubmit 的 `commit()` 被外层事务接管，daemon 唤醒只在提交后发生，marker 由 daemon 按数据库权威状态协调。`retry`/`resubmit` 的最终事务、`clean` 的发布重跑阶段以及 `cancel` 的任务分类与写入，都会在读取权威状态前取得 SQLite writer claim，防止 daemon 在状态校验与首个 job/task mutation 之间收敛或派发任务。daemon 的节点重启恢复、接管终态判定与重试发布也使用同一 writer 顺序；已落库的 cancel request 或 `kill_reason=cancelled` 永远优先于自动重试/恢复回队。`daemon start/stop/drain/resume` 与 `config set` 不绑定 SQLite 事务，进程中断留下 started 时返回 75，拒绝猜测外部结果。相同 request-id 和完全相同绑定重放已保存退出码/输出而不重复执行；绑定变化返回 64，前置条件冲突返回 65。stdout/stderr 捕获各自最多 2 MiB；旧 done 输出定期压缩为 tombstone（清空输出但永久保留 argv 绑定与退出码），因此 tombstone 重放保持退出码且不重复 mutation，但不再重放旧文本。
- 本地 `submit` 与网关 inbox payload 都可在 submission gate 外做预览校验和计算指纹，但最终写入前必须在同一 gate 内按最新同名代际重验依赖存在性、完整依赖图、批次 ID 与同名终态，并原子提交批次/任务/job（inbox 同时提交请求回执）。因此并发提交不能分别基于旧快照发布 `A → B`、`B → A` 环，也不会在 `clean` 两阶段之间或 `retry`/`resubmit` 事务中途插入第二个同名非终态批次；反向顺序会在重开旧批次前拒绝已有的同名非终态实例，`clean` 在删产物前和发布重跑前各重验一次。daemon 退出门禁生效时 payload 与 pending 请求保留供恢复后重试。
- state 根目录优先级：`SCHED_STATE` > 已加载的 `config.state_dir` > `~/.sched`。相对 `config.state_dir` 以 bootstrap config 所在目录为基准解析；`node` 必须是安全的单一路径分量。共享 state 上的登录节点查询仍定位 `config.node` 的节点目录。

---

## 5. 状态机速查

```
任务:  pending → running → done / failed / blocked / cancelled / timed_out / interrupted
       pending → skip (指纹命中, 与成功等价)
批次:  queued → active → done | blocked (等 retry/resubmit)
       blocked → active (retry/resubmit，或候选 batch-policy 显式 --reopen)
       done → active (resubmit，或 clean 确有最新 skip 重排)
       blocked/queued → discarded (退役终态, 不可 retry/resubmit)
GPU:   free → assigned → releasing → free; 外部占用 → unmanaged; 连续健康异常 → quarantined
```

空闲 GPU 的物理探测区分信号来源：发现 compute PID，或 compute/topology/utilization
任一探测不确定时，
仍在首次采样立即转 `unmanaged`（fail-closed）；只有“compute 列表完整且为空、但
`utilization.gpu > 0`”这一类易受驱动底噪影响的信号才做连续 3 次确认。确认期间该卡
保持 `free` 展示但从所有派发路径临时排除，任一干净采样立即清零并恢复派发；连续确认
成立后才持久转 `unmanaged`。因此 1–2% 的单次/短时空闲抖动不会制造状态和日志风暴，
也不存在把确认中的可疑卡分给新任务的窗口。已经处于 `unmanaged` 的卡连续 2 次
干净采样后自动恢复 `free`；`quarantined` 仍须 `sched gpu-ok` 人工解除。

daemon 只派发 `active` 批次中每个 task 的最新版本；终态/queued 批次和旧版本即使因
历史数据遗留为 pending，也不会在 daemon 重启后重新执行，且不会阻塞批次收敛、
依赖解锁或 idle 退出。候选扫描后仍会在最终 `pending → running` 抢占时原子重验
批次状态与最新版本。旧版本若仍为 `running`，或任一代际还有未决 launch marker，
仍会 fail-closed 阻止批次收敛、依赖解锁及 daemon idle 退出。
每次真实启动会在 `Popen` 前先原子发布并锁定版本化 launch intent；优先使用
no-replace rename，共享文件系统不支持该 capability 时退回同目录 hard-link
no-replace，并验证目标与锁定 FD 的 inode 一致。hard-link 的 link→unlink 崩溃窗
允许 intent 暂时有两个名字，但 identity marker 始终只接受单链接。wrapper 继承该锁，
在执行任何用户命令前把 intent 替换为强进程身份 marker。daemon 崩溃时，只有能够
非阻塞取得原 inode 锁并再次证明路径/nonce 未变化的 abandoned intent 才可删除；
锁仍由 launcher/wrapper 持有或 marker 内容未知时一律保留 running 与资源，禁止二次启动。
周期 tick 会重新接管所有 `running + pgid=NULL` 行；即使某轮已 claim intent 后数据库
收敛失败，下一轮也会消费 cancel 或安全回队并释放 GPU，不依赖 daemon 再次重启。
`blocked` 批次不会因历史 pending/waiting 行被 daemon 自动重开；只有成功提交的
`retry`/`resubmit` 能将 blocked 批次改回 `active`。`clean` 仅在 done 批次确有最新
`skip` job 被重新排队时重开该 done 批次。
依赖解锁在 submission gate 内的同一 SQLite writer 事务中读取上游最新代际并以
`status='queued'` CAS 发布 `active`；并发的上游 resubmit 与下游 discard
因此都有唯一串行化顺序，不会用陈旧成功快照解锁或复活已退役批次。上游 `done/skip`
都要现场复核该代所有声明产物仍有效，且 `skip` 还要求 fingerprint 非空，才算依赖
成功；共享路径被另一次 clean 删除后，原 producer 或其他 skip 别名都不会继续解锁
下游。尚未发布批次终态时发现名义 done/skip 的产物已失效，会把批次 fail-closed 为
`blocked`，避免留下无法恢复的 active 批次。
SQLite 状态是终态 marker 的权威来源；marker 属派生视图，daemon 只对本轮实际
可派发且确为最新同名实例的批次，在派发前清理旧 `.done/.blocked` marker，不扫描
无界历史，也不会误删较新同名终态批次的 marker。终态写入同样会在提交后持短
submission lock 重验最新同名实例与状态，并先删除相反 suffix，避免旧实例覆盖或
`.done/.blocked` 并存。终态通知的事件快照与 marker 所有权检查共享这一次
submission gate；批次已被 retry/resubmit 重开时不会把 `active` 误报为 blocked。

回滚到不认识 `intent-v1` 的旧 daemon 前，必须先用当前版本成功停止/收敛 daemon，
确认没有 `running + pgid=NULL`、没有 unresolved launch marker，也没有仍持锁的 wrapper；
条件不满足时禁止直接启动旧 daemon，否则旧恢复逻辑可能删除在途 intent 并重复执行任务。

## 6. 常见误区 TOP6

1. 忘写 `gpu_share: true` → 单卡单任务（R1 清单）
2. 改代码未 commit 就 resubmit → 正常现象是重跑；若 SKIP 见 R3/R4
3. 共享任务漏写 `vram_gib` → 校验直接拒绝（装箱必须有名数）
4. 同名重复 submit 会生成新实例；名称引用选择最新实例，要操作旧实例请使用完整 batch id
5. `min_bytes` 设得比真实结果大 → 有效结果被判无效；小 JSON 用 `check:"json"`
6. 把 `gpu_quota:0` 当成禁用 GPU → 实际是无限制；项目级禁用使用 `gpu_enabled:false`

## 7. 下一步开发（尚未实现）

尚未实现的能力记录在 [`next-development.md`](next-development.md)，不能作为当前
config/API 使用。项目级 GPU 开关 ND-01 已实现，行为与验收依据见
[`project-gpu-access.md`](project-gpu-access.md)。

## 候选断点恢复接口

显式 task.recovery、精确 smoke 门禁、新版本 OOM FIFO 和独立只读
`sched recovery <batch>:<task> [--version N] --json`
见 [recovery-policy.md](recovery-policy.md)。0.2.2 不支持该候选接口；现有 task/status/history
JSON 与 execution FD4 identity 不变，恢复任务仍使用 max_retry:0，禁止旧版本重放。

候选 `projects[P].gpu_admission` 显式开启固定默认 12 GiB 准入，外部占卡共用需另设
allow_external_occupancy:true。恢复分级和持久无进展策略见 [recovery-policy.md](recovery-policy.md)；
默认不限等待，schema 9 写库不能由 0.2.2 回接。

候选 `sched daemon foreground [--supervise] [--restart-delay-sec N] [--max-restarts N]`
只在计算节点前台执行，不由 request 包装。人工 stop 包含当前 supervisor 的持久停止请求；
正常 drain/idle 退出不重启，心跳过期不能证明 child 已死亡。见
[恢复策略](recovery-policy.md) 与 [故障验收](recovery-acceptance.md)。
