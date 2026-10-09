# 候选启动约束原语

这是 per-job affinity/cgroup 工作的通用第一层，不是已完成的 cgroup 硬隔离功能。
源码提供通用 `LaunchConstraints` 和 `sched-execution-constraints/v1`；该原语本身不
新增任务字段或迁移。后续 [scheduler 亲和层](cpu-isolation.md) 显式冷配置启用、
以 schema 18 持久化 CPU 分配/释放，并提供独立 CLI；Agent 仍只通过 sched CLI 操作。
另有 [委派 CPU scope 原语](cpu-scopes.md) 管理唯一 intent/inode、配置、被动恢复和
空 scope 清理；scheduler 持久事务/恢复接入与设备策略尚需后续实现。

## 通用 backend 接口

三种公开 backend 的 `prepare` 可显式传入 `constraints=LaunchConstraints(...)`：
SubprocessBackend、LinuxFdBackend、PersistentLinuxFdBackend。省略时维持原行为。

```python
from gsched.execution import LaunchConstraints

# CPU 编号由调用方根据实际已授权的 allocation 决定，不能使用示例编号。
constraints = LaunchConstraints(cpu_affinity=(24, 25))
# prepared = backend.prepare(envelope, constraints=constraints, ...)
```

cpu_affinity 必须是严格递增、无重复的 tuple，最多 8192 项，编号 0..1048575，
不接受 bool；准备时必须属于当前线程的实际 affinity。也可单独或同时传入
cgroup_procs_fd：调用方保有的、可写且位于真实 cgroup-v2 filesystem 的 cgroup.procs
FD。普通文件、同名伪文件、只读 FD 和非 Linux 明确拒绝，不能用路径字符串或环境
变量代替。两者都为空拒绝。FD 不能同时作为传给用户程序的 pass_fds/fd_bindings
或 subprocess stdout/stderr 来源。

调用方必须持有 source FD 至 prepare 返回。backend 自行复制/保留控制 FD，不接管
调用方原 FD；关闭/失败释放自己的副本。cgroup controller 配置、有效 cpuset、委派
来源、目录 inode/不可变 job 绑定及 scope 生命周期由上层负责。本原语不会创建 scope、
启用 controller、写 cpu.max/memory.max、改变父租约或安装设备 BPF。

child 在用户程序 exec 之前用保留 FD 加入 cgroup，再设置并重新读取 affinity；
实际 mask 必须精确一致，不能接受内核隐式缩小后的部分绑定。任一步失败都不执行
用户程序，不退回无约束执行。FD join 不证明该 scope 已配置资源上限。

native 在 fork 前分配 mask/复制 FD，child 只调用安全 syscall，不加载客户 callback，
仍按 retained executable FD 执行原始固定 argv。只有新 native 模块明确报告
constraints_interface_version 才接受可选约束；旧模块对默认调用仍兼容，但显式约束
报 native_constraints_unavailable，不降级。持久 owner 启动时继承保留控制 FD，原始
child 的绑定一次性固定；认证重连不能重发 start、替换 CPU/cgroup 或制造 wait。

纯 Python subprocess 路径不使用多线程进程中的 preexec_fn。一个隔离 -I/-S bootstrap
用固定最小环境读取封印描述符，先应用约束，再以原 argv/env exec；客户 loader/Python
startup 环境不会在应用约束之前进入 bootstrap。控制 FD 在最终 exec 前关闭，不泄漏
给用户程序。该路径需要 Linux memfd/sealing，缺失时明确拒绝，不采用临时文件降级。

bootstrap 本身是原 Owner 的直接 child，成功 exec 后原 PID/wait 不变。若约束或最终
exec 失败，subprocess 返回该实际 child 的原始 exit=127、launch_error errno 和真实
group cleanup；不能称作“从未创建 child”，不能补造客户程序的 wait。errno 通道 EOF
不作为已成功执行的证明。native 保持原有 launch_error/wait 语义；原生错误管道同样
不由 EOF 推断 exec 成功。prepare/launch 消耗规则、取消和未知 owner 保留不变。

## 不代表什么

affinity 可被有相应权限的程序再次扩大；它不是不可绕过的 cpuset 或设备权限。
cgroup cpuset 的层级上限才限制可用 CPU，GPU 设备限制在 cgroup-v2 中还需要设备
BPF。这里未提供这两种资源配置或多租户安全隔离。调用方的委派/UID/文件权限仍须
使客户无法修改不应修改的父级策略；同 UID 的委派写能力不能当成恶意程序沙箱。
规则依据见 Linux [affinity 手册](https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html)
和内核 [cgroup-v2 文档](https://docs.kernel.org/admin-guide/cgroup-v2.html)。

当前计算节点缺少用户 cgroup 写委派，专项只取得实际 CPU affinity/继承/错误/取消/
FD 生命周期及 native/持久 owner 重连证据，不能宣称取得真实 cgroup join/创建或 GPU
设备隔离验收。测试见 [launch constraints](../tests/test_launch_constraints.py)；执行型
测试只在 Linux 计算节点/CI 跑，native 必须明确构建，缺失不冒充通过。

## 后续交付，仍属原目标

1. scheduler 显式冷配置、能力检查及 CPU claim 已有亲和候选；未知 CPU 边界停新
   派发，默认关闭兼容。cgroup 委派可用性/控制面仍待实现。
2. cgroup 模式仍需事务前准备最小 scope，持久化 allocation/CPU 集合/inode/job 绑定后才能启动；
   预约不可冒充生效，真实 child/资源事件独立记录。
3. 运行、取消、timeout、失效租约、启动失败和 owner 重连共享精确 scope；有进程或
   cleanup 未知就保留资源，不凭 PID/日志补 wait，不迁移旧 scope、不自动重放。
4. 仅对已绑定的、确认空且 inode 未变的本次 scope 清理；scope unknown 禁止重复
   分配该 CPU 集合。明确 CPU 上限与设备访问各自的支持/不可用状态。
5. 在具备委派的隔离计算环境验证 cpuset 不可越界、资源释放/重启/失败；设备隔离与
   真实 CUDA 仍需单独授权，最后按阶段 12 完成发布/生产切换。
