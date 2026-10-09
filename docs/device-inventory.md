# NVIDIA 设备映射（源码候选）

这是实际设备安装接入的前置项，不是已启用的 GPU 隔离。它生成精确原设备规则，
只读映射本身没有安装配置、BPF attach 或 daemon 调度改动；后续冻结绑定候选另写
schema 22。原策略与持久生命周期见
[设备原语](device-policy.md)和[设备记录](device-scopes.md)。

## 原映射与有限采样

GPU index 不是设备 minor；使用 UUID/PCI 核对同一张卡。NVIDIA 对编号稳定性的说明
见 [nvidia-smi 文档](https://docs.nvidia.com/deploy/nvidia-smi/index.html)。采样按顺序：

1. 有界 CSV 查询 index/UUID/PCI/MIG current/pending，最多 128 卡、64 KiB。
2. 按规范 PCI 路径读取 driver information 的 GPU UUID/Device Minor，核对原 UUID；
   不假设 `minor_number` 是可用 CSV 字段。字段依据见
   [NVIDIA information 解析](https://github.com/NVIDIA/nvidia-container-toolkit/blob/main/internal/info/proc/information_files.go)。
3. 从 `/proc/devices` 取得 primary/UVM 的实际 major，核对 control=primary:255、
   UVM=uvm:0 与每张卡的 primary:minor；重复编号/UUID/PCI/minor 或 driver major 均拒绝。
4. `/dev` 用原目录 FD 定位；只用 `O_PATH|O_NOFOLLOW` 验证直接字符节点的 inode/dev/rdev，
   不用读写方式打开这些节点。结束时重复核对节点、根、boot/mount namespace 与 driver 信息，
   再核对完整 CSV，变化、缺失、符号链接和读取失败均不返回完整映射。

完整采样预算 5 秒，utility 输出也有字节/截止时间限制。超时只 kill/wait 自己的 helper；
原 child 未回收时保留精确 Popen 句柄，拒绝再生成 helper，不按 PID 猜身份。模块没有
mknod/modprobe、sudo、设备策略安装或 Slurm 父级配置接口。utility/驱动仍是外部依赖，
其失败不会变成空的成功映射，也不能因此降级为 index=minor。

## 原 allocation 选择

纯选择函数只接受原 allocation 的 GPU index/UUID、非 simulated、recorded_sample 和
5 秒内的原 topology 时间。所选卡必须匹配完整映射；不存在、重复、UUID 漂移、过期
或 fake GPU 均拒绝。序列化记录完整核对后才生成有序精确白名单，不修改输入。

所有任务基线仅含固定字符设备 null/zero/full/random/urandom/tty 的 read/write；
不授予 mknod。CPU-only 不加入任何 NVIDIA 卡、control 或 UVM。GPU 任务才加入
原选中卡、nvidiactl 和 nvidia-uvm；不猜 UVM-tools、DRM、NVLink 或 nvidia-caps。
需要额外设备的工作负载须另行扩展精确协议和验收，不能自动放行整个 major。

v1 只支持已明确记录 MIG current/pending 均 Disabled 的整卡选择；Enabled、N/A、
未知或缺少字段不能作为未分区证据。MIG GI/CI/capabilities 映射尚未实现，不能用
整卡节点代替 MIG 权限；官方接口见
[MIG 设备说明](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/device-nodes-and-capabilities.html)。
映射完整不表示特权安装可用、设备 FD 无继承或 CUDA 可运行。
后续 [原 UUID MIG 能力候选](mig-capability.md) 显式可选 v2 查询，额外冻结两次
NVML GetMigMode 的身份/版本/原结果；只有明确不支持时才区分 N/A，其他未知仍拒绝。
新设备 controller 采样要求 v2，旧 v1 原记录保留，不补写或偷偷改变默认诊断。

## 显式只读诊断

`sched device-inventory --json` 协商 `sched-device-inventory-v1`，必须在 `config.node`
匹配的 Linux 计算节点执行，foreign-write override 不能放行网关探测。它不打开 DB、
初始化 state、迁移、改配置或派发任务，不更改默认 status/task/history；capabilities 仅
增加独立协商键，不因此自动探测硬件。
原 `device-scopes` 仍只读已记录事实，不因此探测 hardware。

成功报告完整 inventory 和摘要、`runtime_probed:true`；admission/wait/physical boundary
均 false。失败返回 1 和 stderr，不返回空清单或成功字段。报告是当次有限观察，
不是持久 claim 或可跨时间复用的执行许可。MIG 未知可以被诊断记录，但不能由纯
选择函数授予整卡策略。

## 原 allocation 冻结绑定（后续源码候选）

schema 22 新增空 `device_inventory_bindings` 表与不可变/保留触发器，不回填历史。
在设备 reserved 阶段的显式 writer 中、消耗 install_intent 前，冻结完整映射、原采样/
冻结时间、映射摘要，以及 instance/job/version/allocation/lease、原设备 intent 和
CPU scope inode 绑定摘要。纯规则选择必须等于原 DeviceIntent，boot/mount namespace
必须与原 CPU parent 相同；声明 GPU 数量与原预留不符、完整原 CPU/GPU claims 缺失、
取消、非当前代际、fake/未知拓扑、过期采样或未知 MIG 均拒绝。CPU-only 仍只授予固定基线设备。

未来 controller 的 `verify_current` 接口要求 writer 外提供新的有界采样，核对完整
原映射/节点 inode/driver/context 与原预留；新采样不能刷新不可变原 GPU topology
时间或换卡。installed 不代表可以重建启动权，unknown、launch_intent、取消/清理
或原 claims 丢失均拒绝新效果。此接口不自行采样、安装 BPF、重启客户进程或授予
admission/wait；实际保留 handle 与一次性 CAS 接入仍是后续工作。

`sched device-inventory-bindings [--scope-id ID] [--limit N] [--cursor ID] --json`
协商独立 `sched-device-inventory-binding-v1`。默认返回摘要，精确 ID 返回完整原映射；
limit 默认 20、范围 1–100，精确 ID 与 cursor 互斥。单记录 256 KiB、包括原设备/CPU/
allocation 链的总查询预算 4 MiB；超限拒绝，实时 keyset 分页不是完整当前快照。
查询只走私有 DB/WAL 快照，不采样硬件、迁移旧库或改变 revision；所有 runtime/
admission/wait/physical 标志均 false。原记录过期仍可以查询，不解释为当前健康。
完整 schema 1–21 返回 migration_required；当前完整只读范围 1–24。schema 21 及更旧
writer 不可回接新库，回退只使用升级前验证恢复点，不能手改 state 或降低 schema。

## 验收与未完成项

[回归](../tests/test_device_inventory.py) 覆盖 index/minor 不同、逐卡/CPU-only 规则、
原 UUID/driver/node/namespace 漂移、未知 MIG、字节/数量边界、网关拒绝和 helper 回收。
模型不算设备边界验收；真实 helper 仅执行独立 CPU 子进程。
[手工诊断验收](../tests/run_device_inventory_accept.py) 在现有计算租约内用新的私有
配置执行 CLI，不启动 daemon；实际不可用与拒绝路径通过分别报告，不把不可用算映射成功。
输出可能含私有 UUID、节点与路径，只保存到仓库外。

冻结绑定的[纯事务回归](../tests/test_device_inventory_state.py)检查原身份/策略/时效、
不变历史、故障拒绝、分页/字节边界、旧库不迁移与 21→22 无回填；不是内核验收。
后续 schema 23 [显式设备 controller](device-controller.md) 已接通安装/原 handle 启动/
恢复仅观察，schema 24 [MIG 能力候选](mig-capability.md) 区分原 GetMigMode 不支持/
未知并保留 v1；writer 23 及更旧不得回接 24。仍未实现 MIG 精确权限或完成正向
BPF/真实 GPU 验收；未发布或部署。
