# 已记录原委派根健康（源码候选）

`sched scope-health --json` 协商独立 `sched-scope-health-state-v1`，读取私有 DB/WAL
快照和原 daemon owner 侧车。查询可在网关执行，不打开 cgroup、不查询 BPF/Slurm、
不初始化或迁移 DB，也不生成启动/安装/清理权限。默认 status/task/history、daemon
status JSON、scope 生命周期查询及 FD4 不增加字段。

## 原记录和新鲜度

显式 CPU cgroup controller 成功预检时，将首次原 parent 路径/inode/device/boot/mount/
UID、原 daemon/job/step 层级 authority、完整 CPU/NUMA 集合、冷 CPU/设备策略和可选
父 BPF program IDs/flags 记为 scope_origin。同一 lease 的原记录不可替换；随后
scope_check 绑定其完整摘要，记录采样开始时间、观察/失败原因。两种事件复用原
daemon_lease_events 的不可变、有限、前后摘要链，不写任务 revision 或生成资源 claim。

采样只在拥有原租约的计算节点 controller 中执行。kernel/hierarchy、原 root inode、
cpuset/NUMA 和可选 BPF 查询在记录前再次核对，前后漂移拒绝；只读核对 root 当前
写访问权限，不修改父 controller、创建 scope 或 load/attach BPF。不支持/读失败为
unknown，不改旧 scope、不杀 running、不换父级或自动绑定新租约。
启动、每轮 tick（包括 drain）及实际派发/启动检查更新记录；关闭 CPU cgroup 不采样。
原 BPF IDs/flags 变化不接受新策略为原策略，记录 invalid 并拒绝新派发。

每次 probe 必须在 5 秒内获得 writer 并提交；持久诊断使用 30 秒有效期，覆盖现有
10 秒 tick。查询返回 observation_age_s/expires_after_s；时钟倒退/过期、缺记录、
daemon 出生不同/已退出、owner 检查期间改变、冷配置改变或原 Slurm lease 未确认
有效都不会返回新鲜 ready。运行时启动仍只使用原 retained handle，内部 5 秒
admission_current 和实际重新采样不因该 30 秒诊断窗口而放宽。

## 结果语义

`recorded_origin`/`recorded_check` 是原始已记录事实，`status` 是本次被动解释：

| status | 意义 |
| --- | --- |
| ready | 原根前置条件的观察已提交、绑定仍匹配且未过期，不证明 join/安装成功 |
| unavailable | 有新鲜观察，但已记录活动 scope 未决，不能解释为可派发 |
| invalid | 原根/容量/可选父 BPF 证据与首次记录不同，不接受替代证据 |
| unknown | 观察失败/缺失/过期、身份/配置/租约不明，或查询无法确认原 owner |
| disabled | 当前原 daemon 未启用 CPU cgroup，不虚构一个委派根 |

available 只表示已绑定的新鲜观察可读，unknown 观察本身也可能 available=true；
不是可执行。runtime_probed、admission_granted、wait_authority_granted 和
physical_boundary_verified 始终 false。错误/损坏的原摘要或结构返回非零，不当作健康。
停止/更换 daemon 后仍保留不可变历史；精确 lease 事件链可用 daemon-lease 查询。

admission-explain 的 cgroup fit 可使用同一原 owner 的新鲜健康观察、独立租约检查和
CPU claims，复用真实 pool intersection/choose/活动 scope 上限判断。缺观察仍明确
unknown，内核事实不从查询主机补造；allowed=true 只表示该快照的资源解释，不预占
CPU、不提供新 start 权限，也不替代真实调度时的 preflight/设备映射/一次 CAS。

writer schema 25 守卫新增持久事件语义；不增加 SQL 表、不回填旧根或改写原 lease/
allocation/MIG 记录。完整只读范围 1–25，旧 schema 的 scope-health 返回
migration_required；writer 24 及更旧不能回接 25，回退只使用升级前验证恢复点。
包版本仍未变更，尚未发布或部署。

## 验收边界

[纯模型与私有 SQLite 回归](../tests/test_scope_health.py) 覆盖原根/容量/BPF 漂移、
双采样、未知/时钟/过期、替代 owner/exit、cold config、原 scope 未决、原摘要/层级、
旧 marker/无回填、gateway 无 probe 和资源解释不预占。根/BPF positive 使用模型，
不能计为实际委派或设备访问成功。
[CPU CLI](../tests/run_cpu_isolation_accept.py) 验证实际 private daemon 的 disabled
诊断；[普通目录拒绝](../tests/run_cpu_cgroup_accept.py) 验证没有生成原根记录，
正向 root ready 则另需明确专用委派/native 的 --positive 验收。
实际 cpuset/BPF/GPU 权限与 owner/daemon 故障矩阵、原租约真实结束和生产切换仍待
独立授权验收，不因新鲜 root 前置观察而宣称硬隔离完成。
