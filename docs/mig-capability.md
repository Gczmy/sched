# 原 UUID MIG 能力区分（源码候选）

此项只解决 `N/A` 的解释与原分配设备规则选择，不启用 MIG、不申请 GPU、不安装
BPF，也不是实际权限隔离验收。没有 GPU 型号白名单或按代数猜测“不支持”。
默认 `device-inventory --json` 仍协商 v1、返回原 v1 映射，不执行新增 NVML helper。

## 查询与分类

`sched device-inventory --with-mig-capability --json` 显式协商
`sched-device-inventory-mig-v1`；inventory.interface_version 为
`sched-device-inventory/v2`，增加完整 mig_capabilities 原始证据，外层 mig_support
按原 gpu_id/UUID 返回 supported/not_supported/unknown。仍必须在 config.node 对应
Linux 计算节点执行，不读 DB/改配置/派发任务；所有 admission/wait/physical 标志 false。

原 UUID 列表来自完整 CSV，至多 128 卡。固定 `python -I -S -c` helper 用标准库
ctypes 加载已安装的 `libnvidia-ml.so.1`，只调用 init/shutdown 与 driver/library version、
handle-by-UUID、UUID 和 GetMigMode getters，不导入 vendor Python、客户模块或第三方
运行依赖。库/函数缺失、初始化、权限、driver mismatch、GPU lost 或身份不明为 unknown；
helper 与 CSV 共用 5 秒总预算、64 KiB 输出上限和同一个原 Popen 回收守卫。
超时/溢出/未回收 helper 拒绝完整采样，不生成新的 helper 或把失败当空映射。

GetMigMode 前后均核对同一原 handle 返回的完整 GPU UUID，记录函数阶段、原 return
code、两种 mode 与 driver/library version。只有身份和版本均明确、阶段确为
GetMigMode 的 `NVML_ERROR_NOT_SUPPORTED=3` 才分类 not_supported；其他 API 返回同样的
3 不算设备不支持。返回成功且 current/pending 为 0|1 才 supported，未知输出仍 unknown。
官方语义见 [NVIDIA MIG API](https://docs.nvidia.com/deploy/nvml-api/latest/api/group__nvmlMultiInstanceGPU.html)
与 [NVML return codes](https://docs.nvidia.com/deploy/nvml-api/latest/api/group__nvmlDeviceEnums.html)。

完整 capture 在同一个 CSV/设备节点/boot/mount bracket 前后各读取一次 NVML 证据；
版本、UUID、阶段/code/mode 任一变化均拒绝。显式 not_supported 必须与 CSV current/
pending 均 N/A 一致；supported 必须与 CSV 的 Disabled/Enabled 完全一致，不能用
CSV N/A 覆盖 NVML 结果或隐去冲突。

## 设备规则和持久兼容

v2 GPU 规则只接受 supported 且 current/pending 均 Disabled，或上述确认
not_supported 的整卡；仍只放行原 allocation 的新鲜 UUID/index/driver minor，不换卡、
刷新旧 topology 或猜 MIG GI/CI/capabilities。任何 unknown、Enabled 或待启用均拒绝
GPU 放行；CPU-only 仍仅固定基线设备，不加入 NVIDIA 节点。
v1 历史映射保留原语义：只有明确 Disabled/Disabled 可选，N/A 一直 unknown，
不从当前 NVML 补造历史事实。旧查询不会默默改为新协议或增加卡字段。

显式 device_isolation.mode=nvidia controller 的新采样要求 v2，并把完整原能力证据
与原 CPU inode/lease/allocation/device intent 一起冻结；安装和启动前的新鲜核对必须
与原记录完全相同，未知不重装或 CPU-only 降级。重启仍只观察原 installed binding，
不凭 NVML 重新 mint 启动 handle；设备恢复合同不变。

writer schema 24 是新持久证据版本守卫，不增加 SQL 表，不回填/改写 v1 原记录或
旧 allocation/instance。v2 冻结要求 marker>=24；后续原根健康候选完整只读范围 1–25；writer 23 及更旧
不能回接 24，回退只使用升级前验证恢复点，不降低 user_version 或手改 state。
被动 device-inventory-bindings 查询仍使用原协商协议及摘要/精确 ID 分页，不执行
NVML；完整原 inventory 自带 v1/v2 标识。包版本不变，尚未发布或部署。

## 验收边界

[回归](../tests/test_mig_capability.py) 覆盖原 GetMigMode 与其他错误的区别、双 UUID、
CSV/NVML 冲突、未回收/超时/溢出 helper、持久原证据与漂移、23→24 不改写 v1、
默认 v1/可选 CLI、不探测 DB 以及拒绝 legacy map 代替新设备安装证据。
vendor getter 用模型；真实 helper 边界用独立 CPU 子进程，不算真实 NVML 成功。
[计算节点只读验收](../tests/run_device_inventory_accept.py) 的
`--with-mig-capability` 才调用真实 NVML；观察失败、unknown、not_supported 和 supported
分别记录，不把“脚本正常报告 unavailable”算作硬件能力成功。
私有 UUID/节点/路径/驱动版本不写入公开仓库。

仍未实现 MIG GI/CI 权限映射；真实 enabled MIG、专用 cpuset/BPF 逐卡权限、后代/
owner/daemon 故障矩阵、实际委派下的新鲜根健康、原租约真实结束与授权发布/生产切换仍须
独立完成。只读明确“不支持 MIG”不证明 CUDA 可运行或设备隔离已生效。
被动根前置记录与解释已在后续 schema 25 [原根健康候选](scope-health.md) 实现，
不以模型或旧安装记录推断实际委派/GPU 健康。
