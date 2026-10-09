# NVIDIA 设备映射（源码候选）

这是实际设备安装接入的前置项，不是已启用的 GPU 隔离。它生成精确原设备规则，
没有安装配置、BPF attach、schema 迁移或 daemon 调度改动。原策略与持久生命周期见
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

当前只支持已明确记录 MIG current/pending 均 Disabled 的整卡选择；Enabled、N/A、
未知或缺少字段不能作为未分区证据。MIG GI/CI/capabilities 映射尚未实现，不能用
整卡节点代替 MIG 权限；官方接口见
[MIG 设备说明](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/device-nodes-and-capabilities.html)。
映射完整不表示特权安装可用、设备 FD 无继承或 CUDA 可运行。

## 显式只读诊断

`sched device-inventory --json` 协商 `sched-device-inventory-v1`，必须在 `config.node`
匹配的 Linux 计算节点执行，foreign-write override 不能放行网关探测。它不打开 DB、
初始化 state、迁移、改配置或派发任务，不更改默认 status/task/history；capabilities 仅
增加独立协商键，不因此自动探测硬件。
原 `device-scopes` 仍只读已记录事实，不因此探测 hardware。

成功报告完整 inventory 和摘要、`runtime_probed:true`；admission/wait/physical boundary
均 false。失败返回 1 和 stderr，不返回空清单或成功字段。报告是当次有限观察，
不是持久 claim 或可跨时间复用的执行许可；未来 controller 必须冻结原记录并在安装/
启动前核对新鲜相同绑定。MIG 未知可以被诊断记录，但不能由纯选择函数授予整卡策略。

## 验收与未完成项

[回归](../tests/test_device_inventory.py) 覆盖 index/minor 不同、逐卡/CPU-only 规则、
原 UUID/driver/node/namespace 漂移、未知 MIG、字节/数量边界、网关拒绝和 helper 回收。
模型不算设备边界验收；真实 helper 仅执行独立 CPU 子进程。
[手工诊断验收](../tests/run_device_inventory_accept.py) 在现有计算租约内用新的私有
配置执行 CLI，不启动 daemon；实际不可用与拒绝路径通过分别报告，不把不可用算映射成功。
输出可能含私有 UUID、节点与路径，只保存到仓库外。

尚未接通 scheduler 的实际安装/启动/恢复，未冻结 inventory 到原 allocation，未实现
N/A 的可靠能力区分、MIG 精确权限或完成正向 BPF/真实 GPU 验收。原设备 intent/CAS、
未知不重装、原 scope 删除后释放资源等守卫仍须由后续 controller 接入；未发布或部署。
