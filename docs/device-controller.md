# 显式设备安装与原 handle 启动（源码候选）

协商 `sched-device-cgroup-v1`；此候选接通 scheduler 安装与恢复观察，但尚无特权
BPF/真实 GPU 正向验收，未发布或部署，不表示现有生产任务已受设备隔离。
默认省略或 `device_isolation: {"mode":"off"}` 不探测、安装设备策略，也不改变旧
allocation/default status/task/history/FD4。单独 CPU cgroup 不启用设备隔离。

## 显式冷配置与原分配

`device_isolation: {"mode":"nvidia"}` 必须同时配置
`cpu_isolation: {"mode":"cgroup","delegated_root":"/sys/fs/cgroup/example"}`；示例路径
不是已准备的委派。仅接受 mode=off|nvidia，不支持额外键、自动启用或 fake topology。
配置为冷配置，热改后暂停新派发，不能将已绑定任务降级为 CPU-only。

源码 writer schema 23 在原不可变 allocation.cpu_binding 中仅为明确启用的任务
加入 `device_isolation:"nvidia"`，先于 CPU mkdir 和设备 intent。因此在 CPU 配置完成、
设备记录尚未创建的崩溃窗口也不能 CPU-only 启动。迁移只提高语义版本，不回填或重写
旧记录；后续 MIG 能力候选的完整只读范围 1–24，writer 23 及更旧不能打开 24。回退只用升级前验证恢复点，
不可删表、降 user_version 或手改 state；包版本仍未变更。

## 安装和启动顺序

1. 原 CPU 配置提交后，在独立 writer 中同时提交设备 intent 与
   [原完整映射绑定](device-inventory.md)。只根据原 GPU reservation 的 UUID/index/minor
   选择精确规则；CPU-only 排除 NVIDIA 节点，未知 MIG 或过期原 topology 拒绝。
2. writer 外重新采样完整映射，核对原租约、父委派与 native/query 前提；writer 内重新
   检查观察至多 5 秒、原 claims/当前代际/CPU configured 和原 topology 时效，CAS 提交
   install_intent 后才执行原 inode 上的一次 load/attach。没有替换、卸载或补装。
3. 回读原 program ID/tag 后提交 installed binding；再次核对映射，才从原 retained
   DeviceScope 取得一次性约束。普通启动、linux_fd 和 linux_fd_owner 共用原 cgroup FD
   的 exec 前 join。数据库 installed 不代替实际 attachment 观察。
4. exec 前再次检查原 CPU/device handle、完整映射、原租约和实际 attachment；同一
   writer 提交设备与 CPU launch_intent（配置 backend 另同事务绑定原 execution intent）。
   任一 CAS 冲突整组回滚，过期观察拒绝。关闭 FD 不卸载策略，也不重新授予约束。

`daemon check --json` 的 `device_cgroup_preflight` 只验证原 CPU 委派与 native/query
前提，不 load/attach，不检查真实设备访问；通过不保证安装权限、CUDA 或健康。
cpuset 写委派不等于 BPF 权限。失败保留一次性未知，不能另发程序或偷偷关闭设备模式。

## 恢复和清理

重启只用原 CPU inode、原 DeviceBinding 和完整冻结记录进行 attachment 观察，
不 install、不创建新启动 handle，也不从 query 补造丢失的 installed。连接/安装/观察
不确定时保留原 CPU/GPU claim，并停止新派发；持久 owner 仍须原身份认证重连与真实 wait。
同 UID 已打开/继承/传递的设备 FD 不会被撤销，这不是恶意逃逸沙箱。

只有原执行 no-child/clean-group 守卫和含后代为空的原 inode 才允许 CPU scope 实际
删除；设备 query 失败不阻止这条原空 scope 清理路径。随后设备 released 精确引用原
CPU removed，再释放资源。不 detach、不推断程序垃圾回收，也不补造 returncode。
冷配置 off、取消或普通任务终态均不能绕过旧未知记录。

## 验收边界

[模型与事务回归](../tests/test_device_scope_controller.py) 验证提交先于效果、结果丢失
不重装、原 minor 选择、双 CAS 回滚、冷配置/原 handle/映射漂移拒绝、恢复只观察、
未知资源保留及原空 scope 清理。它们不证明内核拒绝 GPU 访问。
[计算节点拒绝验收](../tests/run_device_cgroup_accept.py) 只用私有 ordinary directory
与 fake topology，明确拒绝 daemon/allocation/worker，不附加 BPF 或探测真实 GPU。

后续 [schema 24 MIG 能力](mig-capability.md) 接入 v2 原 UUID NVML GetMigMode，区分
明确不支持/未知；新安装必须 v2，旧 v1 不补写，未知不降级。
尚需：授权专用委派的 cpuset 与 BPF 正向矩阵、真实
逐卡权限及后代继承、取消/owner 断连/daemon 崩溃恢复、被动新鲜根健康、原租约实际
结束和授权独立发布/生产切换。不能把此源码接入或拒绝路径当作完整硬隔离交付。
