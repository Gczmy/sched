# 候选委派 CPU scope 与 scheduler 接入

公开通用接口 `sched-cpu-scope/v1` 位于 gsched.execution.scopes，schema 19 引入
不可变记录层；后续 schema 20 源码候选接通显式 cgroup 冷配置、实际派发与清理。
尚未发布或部署，正向内核隔离、故障恢复和 GPU 设备验收尚未完成，不能称为完整
硬隔离。Agent 的运维入口仍只有 sched CLI，不自行调用原语修改生产 cgroup。

```json
{"cpu_isolation": {"mode": "cgroup", "delegated_root": "/sys/fs/cgroup/example"}}
```

路径仅为格式示例，不能直接用于部署。必须由外部明确提供专用可写委派，不能指向
生产或 Slurm 父级。省略仍为 off，affinity 不创建 scope；切换模式或根路径需排空
并显式重启。daemon check 的 cpu_cgroup_delegation 只读取前置条件，失败拒绝启动，
通过也不授予后续启动权。sched 不创建/启用委派根、不修改父 controller。

## 严格委派与不可变绑定

DelegatedCpuScopes 只接受明确的规范绝对路径；逐级以目录 FD 和 O_NOFOLLOW 打开，
验证真实 cgroup-v2、private 单用户目录、当前 boot/mount namespace/UID、dev/inode
和原路径。父级必须已经是无直属进程的 domain，已经启用 cpuset，且有可用 CPU/
NUMA 节点。不会创建委派根、启用父 controller、移动 daemon、改 Slurm 父级、
写 cpu.max/memory.max/设备策略或提升权限。普通文件、symlink、共享可写目录、
无权限和未知上下文均拒绝，不降级为 affinity。

CPU scope intent 固定唯一 32 位 hex scope_id、调用方不可变 attempt/allocation
绑定摘要、原父级身份、有序 CPU/NUMA 集合；意图和 inode binding 都支持严格 JSON
往返。名字只从唯一 ID 派生，不接受应用路径。每 scope 最多 8192 CPU、4096 NUMA
节点，父 CPU 集合最多 65536，控制文本最多 64 KiB；重复/越界/歧义输入拒绝。

gsched.cpu_scope_controller 按以下顺序接通效果；每次外部操作都在 DB writer 之外：

1. 在创建前保存不可变 CpuScopeIntent 及 CPU claim，失败不能自动换 ID 重试。
2. create 只独占创建一个新 scope，并返回 CpuScopeBinding（原 dev/inode）。
   先持久化该 binding，再 configure；同名存在为未决，不接管或复用。
3. configure 仅写该子 scope 的 cpuset.mems/cpus，然后读取 effective 集合精确核对。
   内核将列表归一成范围时按集合比较；变空、缩小、扩大或 NUMA 不符均拒绝。
4. 保存真实配置观察后，调用一次 constraints 获得原 scope 的 cgroup.procs FD 和
   同一 CPU mask，再交给已有三种 execution backend。backend 在客户 exec 前加入
   scope，原 owner/child/wait 身份不变；scope 配置或 populated 不明不能启动。

configure 和启动 capability 在执行前即消耗，失败不能退回无约束执行或重新配置。
关闭 handle 只关闭保留 FD，不删除 scope。create 返回前的崩溃可能只留下 intent，
不能按名称猜 inode、删除或重建；scheduler 保留 unknown 和 CPU claim。

最终 allocation/CPU claim/scope intent 在同一 writer 事务提交。原委派还绑定
daemon 的精确 kernel context、cgroup-v2 mount、原 parent inode 和 CPU/NUMA 池；
每次派发及最终启动前重核。Slurm 来源必须确认 valid，且委派位于原 job/step 层级，
observe/unknown allow 不能代替该证明。非 Slurm 委派必须处于原 daemon 的祖先或
后代层级，不接受任意 sibling；未知、多 mount 映射或 mount-root 偏移不猜测。
有效池取原 affinity、保守容量与委派 CPU 的交集，最多 256 个活动 scope。容量或
kernel/root 身份漂移暂停新派发，不扩容旧 scope、不迁移到新租约或重启旧 child。

## 恢复、观察与清理

restore 要求原父级、boot/namespace/UID 和子目录 dev/inode 都一致。缺失目录、
替换 inode、改名或新内核上下文拒绝，不新建同名目录。恢复 handle 只能 observe/
remove，不能 configure 或再发启动 FD；它不重连执行 owner，不制造原 wait。
执行 owner 的认证重连仍由原 execution 接口负责。

observe 读取原 scope 的有效 CPU/NUMA、cgroup.events 的 populated（含后代）和
直属进程数量，不写控制文件、不授予启动或 wait 权限。配置匹配也不证明某 worker
已经加入 scope。只有原目录身份仍一致、populated=0 且直属进程为空才允许 rmdir；
有后代、读失败、目录被替换或删除失败都保留，不靠进程组已空推断 scope 已空。
只按原保留父 FD 和唯一子名字清理，不递归删除、不扫目录接管孤儿、不发信号。

这是可信委派者和可信客户使用的资源边界，不是恶意同 UID 沙箱。拥有父级/控制
文件写权限的应用可能改配或移动进程；外部管理者和不合作的同 UID 改名仍可能在
最终路径身份检查/rmdir 间竞争，不能将此模型宣称为对该攻击的原子隔离。scheduler
接入串行化自己的 create/configure/remove；跨 UID 安全边界须由管理员/broker
控制权限。cpuset 限制已加入该 scope 的 CPU，GPU 权限还需要单独设备 BPF；
后续显式设备 controller 已有源码接入，但默认不启用且尚未完成正向验收。
内核规则见 [cgroup-v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)。

## 持久记录层与释放守卫

schema 19 的 cpu_scopes/cpu_scope_events 以空表原子迁移，原 intent 和事件均禁止
UPDATE/DELETE；每 allocation 至多一个 scope，全局 scope_id 唯一，不补造旧任务。
reservation 绑定原 allocation 全文摘要、instance/job/version/lease 和完整 CPU claim，
不能把新租约、同名任务或 retry 后当前指针替代原绑定。调用方在同一个 allocation/
claim writer 中 reserve，再提交后执行外部操作；记录层不自行 commit 或操作文件。

每次外部效果先用 last_event_id CAS 消耗独立 intent，并提交：
reserved → create_intent → inode_bound → configure_intent → configured → launch_intent。
inode 必须精确包含原 intent；configured 只接受精确 CPU/NUMA、空 scope 的已记录
观察，不制造 worker 加入/wait 事实。取消、非最新/非 active 代际和已启动任务不能
消耗新的 create/configure/launch intent；重启不能重复消耗或重新绑定 inode。

schema 20 添加 cleanup_ready：原 execution 已确认无 child/clean group 后，仅在
writer 内请求清理；后续 tick 在 writer 外 restore 原 inode、观察后代是否为空，
再提交 cleanup_intent、执行 remove 并保存真实 removed 结果。未知时 CPU/GPU
预留和 running 保留，drain/idle/stop 不把未决 scope 当作已排空。

unknown 保留原事件链和 CPU claim。mkdir 后但 inode 未持久化时，不允许猜 inode/
按名字接管、abandon 或重建。已保存原 inode 的未知配置可转入 cleanup_intent，但
必须由后续控制器重新核对原 inode 和 scope 含后代为空；原 execution 清理守卫仍
必须通过。remove 的真实成功结果另写 removed，不能从目录缺失推断删除成功；
清理效果已经消耗但结果未知时不重新删除。reserved 尚未消耗 create 的意图才可
abandoned。只有 removed/abandoned 允许原 CPU release；状态终止、retry 清当前
指针、进程组已空或 scope 未知均不能绕过。空 scope 仍不提供原 returncode。

现有 release 已接记录守卫，launch_constraints 对有 scope 的 allocation 拒绝仅
affinity 降级；冷切换 off 时仍有未决 scope 会暂停新派发，不能用配置关闭绕过它。
mode=off|affinity 不会生成 scope；候选 cgroup 派发持有原 scope FD，普通与两种
configured backend 都不允许仅 affinity 降级。父/root 或 scope 配置漂移时停止新
派发，不杀 running；清理仍只按原 inode。创建/启动未决和删除结果未落库时不重放。
原 allocation 明确为 cgroup 但 scope intent 缺失时，同样拒绝仅 affinity 启动和
CPU/GPU 清理结算，不能将缺记录当作旧 affinity 任务。
记录函数只接受内部来源，不是 Agent 的新写入口。

普通 wait 在同一 daemon 内按原 allocation 保留至 scope 清理完成，不反复 poll 或
改读 RC 文件。daemon 重启失去普通原始 wait 时，原 group 与 scope 清理后记
interrupted（已取消/超时则保留该分类），不从 sidecar/产物补判成功或自动重跑。
linux_fd_owner 仍认证重连原 owner/child，不重新 start。配置匹配、空 scope 和
被动生命周期查询都不是原 wait，也不是 worker 已加入的证明。
已退出的原 execution owner 也保留至 scope 清理与任务结算提交后，不在仍 running
的资源待清理阶段提前 acknowledge/关闭。

只读 `sched cpu-scopes --json` 使用私有 DB/WAL 快照；`--scope-id ID` 读取精确
完整事件链，列表提供有界实时分页，不探测内核或改变 revision。命名合同
sched-cpu-scope-state-v1 与通用原语合同独立；所有 admission/wait/physical boundary
标记为 false。完整 schema 1–18 查询明确 migration_required，不做初始化或回填；
当前后续 MIG 能力候选的完整只读范围为 1–24；旧 writer 不得回接更高写库，回退只用升级前验证
恢复点，不能手改 user_version。admission-explain 的 cgroup fit 明确 unknown，
因为当前没有新鲜原委派观察供被动解释，不能把租约检查当作根目录可用性。

## 验收与剩余工作

[回归](../tests/test_cpu_scopes.py) 将 synthetic 模型与实际内核验收分开。模型验证
身份替换、context 漂移、busy/unknown 保留、配置失败消耗、无重复启动、只清理原
scope、FD 关闭和序列化边界；普通目录拒绝在 Linux 计算节点执行。

正向验收必须显式提供 `SCHED_TEST_CPU_SCOPE_ROOT`，指向专用、已委派的空 domain，
不是生产/Slurm 父目录。测试只在该目录创建唯一子 scope，用 subprocess/native
实际 join，验证扩大 sched_setaffinity 仍受 cpuset 限制、后代继承、原 wait、恢复
不再启动和空 scope 删除；不自动准备或改写父级。无委派时明确 skip，不计内核
隔离验收成功。native 另须明确构建，SCHED_REQUIRE_NATIVE=1 时不可用为失败。

记录层的 [纯事务回归](../tests/test_cpu_scope_state.py) 覆盖原绑定、CAS、不可变历史、
创建/配置/清理未知窗口、禁止 affinity 降级、释放保留、旧库和被动 CLI。模型事件
不是实际 cgroup 崩溃注入证据，不将它计入正向 kernel 验收。

[controller 模型](../tests/test_cpu_scope_controller.py) 覆盖已提交 intent 后的效果、
原租约层级、创建与删除未知窗口、取消/冷配置拒绝、busy 后代保留、配置漂移与原 wait
跨 tick 保留。[计算节点 CLI 验收](../tests/run_cpu_cgroup_accept.py) 默认只验证普通
目录委派拒绝且无 allocation/worker/降级；--positive 另需上述明确委派和 native，
实际验证三个 backend 的 join、mask 放宽受限、后代、并发/取消/原 wait/删除及停止后
重启不重放。没有委派时该正向项明确跳过，不计为通过。

后续 schema 23 的[设备 controller](device-controller.md) 接入显式安装、原 handle 启动与
恢复仅观察，默认 off 不安装；单独 CPU cgroup 不限制 GPU。
尚未完成：授权正向 cpuset/原 owner 故障恢复验收、真实设备 BPF/GPU 权限验收、原租约
真实终止矩阵、被动新鲜委派健康观察和授权生产 rollout。现有模型/拒绝测试及
[CPU 亲和](cpu-isolation.md) 不能代替这些交付。

另有独立 [设备策略原语](device-policy.md) 源码候选，提供原 scope 的有界白名单
安装/恢复核对，不自动接入本 controller；schema 20 的 scope 观察仍不证明设备隔离。
后续 [schema 21 设备记录](device-scopes.md) 绑定原 allocation/inode/策略，已记录
设备 intent 时现有 controller 拒绝 CPU-only 启动；原 CPU removed 与设备 released
记录分开，未知不借 cold off/任务终态释放。实际 opt-in 安装由上述 schema 23 接入，
但没有特权正向 BPF/GPU 验收证据。
