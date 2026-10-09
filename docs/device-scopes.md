# 设备 scope 不可变记录与恢复守卫（源码候选）

schema 21 新增空 `device_scopes` / `device_scope_events`、allocation 唯一索引与
不可变／保留触发器；不回填旧设备事实，不修改 instance、原 CPU/执行/资源记录。
通用原语见 [设备策略](device-policy.md)，CPU 生命周期见 [CPU scope](cpu-scopes.md)。
此项尚未连接 scheduler 的设备安装：没有 GPU 设备配置或安装写 CLI，不执行 BPF。

## 原绑定与提交边界

记录必须冻结原 instance/job/version/allocation 全文摘要、lease、CPU scope 原 inode、
scope 意图摘要、原 configured 事件和完整 DeviceIntent。只接受明确 cgroup allocation，
并核对全部原 CPU claim；CPU scope 尚未 configured、已启动、取消或非最新代际均拒绝
新设备效果。同一 allocation/scope 只能有一条策略，不能因恢复另建策略或替换 inode。

每个外部效果先在独立 writer 中 CAS 消耗意图并提交，之后才允许未来 controller 在
writer 外执行。正常记录链为 `reserved → install_intent → installed → launch_intent`；
installed 只接受相同完整 DeviceIntent、实际 program ID/tag 绑定和原语的严格观察。
观察的 admission/wait 必须 false；该记录不验证内核，也不是被动查询的启动权。
CPU launch_intent 要求设备 launch_intent 已在同一提交边界内写入。

当前 CPU controller 没有设备 handle 接入，遇设备记录会拒绝 CPU-only 启动，即使记录
已 installed 或 launch_intent；不会将数据库阶段当作真实策略已附加。后续实际安装
需重构 prepare，在设备策略 binding 提交与原 attachment 新鲜核对后才授予启动 FD。
持久 owner 仍须原身份重连，不能因为 installed 记录而重新 start。

安装效果已消耗但结果未知时只添加 unknown，不借查询发现的 program ID 补造 installed，
不重装、不卸载、不降级；取消、任务终态、retry 清指针或关闭配置均不能释放原预留。
尚未消耗 install 的 reserved 才能 abandoned；abandoned 不授权 CPU-only 继续启动。
事件及 allocation 资源引用保留，旧未知原因不可覆盖。

## 清理与释放

设备释放与原 execution wait/清理分开。必须先取得原执行 no-child/clean-group 守卫，
再由 CPU controller 在 writer 外验证原 inode 及含后代为空，实际 remove 并提交原
CPU removed 事件；空目录、目录缺失、进程消失或普通任务终态均不能替代该证据。

原 cleanup 请求随后在 writer 中将设备非终态（含 unknown）记录为 released，精确
引用原 CPU removed 事件，再允许 CPU/GPU 释放。此步骤不再次安装、删除或探测设备。
released 仅表示原 scope 删除已记录，不表示内核 program 已垃圾回收，也不产生原
returncode。CPU removed 但设备记录尚未完成时仍保留资源／阻止排空，cold off 不能
绕过；CPU-only 历史任务没有设备记录时行为不变。

## 只读合同与迁移

`sched device-scopes [--scope-id ID] [--limit N] [--cursor ID] --json`
协商 `sched-device-scope-state-v1`。默认摘要，精确 ID 返回完整有界链，与 cursor 互斥；
limit 默认 20、范围 1–100，使用实时 keyset，不是完整快照。每个 scope 最多 64 个
设备事件，单记录 256 KiB、设备链 1 MiB；包括关联 CPU/分配的查询证据预算 4 MiB，
超限拒绝并提示减小 limit，不将截断结果解释成当前完整状态。

只读取 CLI 私有 DB/WAL 快照，不初始化、迁移、探测 BPF/cgroup/GPU/进程或改变 revision。
所有 runtime_probed/admission_granted/wait_authority_granted/physical_boundary_verified
均 false；recorded_phase、程序绑定和 release_recorded_ready 均不是实时 kernel health。
策略字节码解析核对两种有界端序并保留原摘要，不依赖查询主机 ABI；异端序不授予安装权。
旧完整 schema 1–20 返回 migration_required，不猜设备绑定或升级；当前完整只读范围
为 1–21。schema 20 及更旧 writer 不得回接 schema 21，只能使用升级前验证恢复点回退，
不能删表/事件、降低 user_version 或手改 state。包版本尚未更改、未发布或部署。

## 验收与剩余工作

[纯事务回归](../tests/test_device_scope_state.py) 覆盖原绑定、CAS/回滚、唯一策略、
不可变历史、安装未知、取消／代际变化、原 CPU 删除引用、资源保留、禁止 CPU 降级、
schema 20 只读与无回填迁移、查询字节边界；均为 synthetic 证据，不算 BPF 验收。

后续[设备映射候选](device-inventory.md)提供 control/UVM/整卡原 UUID/minor/节点核对，
不改变这里的被动查询或实际安装状态。尚未完成：scheduler 实际设备安装、原 inventory
冻结/MIG/能力区分、持久 owner
设备故障矩阵、授权正向 CPU/BPF/真实 GPU 验收、被动新鲜根健康观察、原租约实际结束
及授权发布／生产切换。此记录层不缩小或替代这些交付。
