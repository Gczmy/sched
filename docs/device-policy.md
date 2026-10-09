# 原 scope 设备策略原语（源码候选）

这是独立 execution 接口 `sched-device-policy/v1`，不是 scheduler 配置、CLI 写入口
或已交付 GPU 硬隔离。当前 daemon 不安装该策略；后续 schema 21
[设备记录与守卫](device-scopes.md) 不改变默认 status/task/history 查询。
原 scope 与 CPU 清理合同见 [cpu-scopes](cpu-scopes.md)。

## 有界策略和效果

`DeviceRule(kind, major, minor, access)` 只接受 `char`/`block`、精确 unsigned 32-bit
主/次设备号，以及 1–7 的权限组合：mknod=1、read=2、write=4。不存在通配符、路径
猜测、默认放行或自动 NVIDIA 设备识别。`DevicePolicy` 最多 256 条，按类型/主/次号
排序且同一设备唯一；空策略拒绝全部设备访问。请求的每个权限位都须被明确允许。

固定编译器生成至多 2060 条 eBPF 指令，无 map/helper/tail call、外部编译器或第三方
运行依赖。`DeviceIntent` 严格绑定原 CpuScopeBinding、完整策略与字节码 SHA256；
`DeviceBinding` 另冻结实际内核 program ID/tag。ID/tag 是身份核对，不是完整字节码
读取或原 wait 权威，也不能代替事前持久化的策略摘要。

原字节码端序由 digest 冻结；只读解析核对 little/big 两种有界编码，保留匹配的
原摘要，不随查询主机端序重编译解释。v1 序列化字段不变；异端序 intent 可以查询，
但安装在任何 native 效果前拒绝，不能借此改写策略、重复安装或 CPU 降级。

调用方须在效果前持久化 intent，在取得启动约束前持久化返回 binding。
`DeviceScope` 只接受已配置且尚未启动的原 CpuScope；包装后立即禁止绕过设备验证
直接启动。install 先消耗一次性权限，再验证原 scope 配置/空后代，查询无直接策略，
load 自己的程序，重新核对，attach，核对唯一原 ID/tag 和 MULTI 标志。
load/query/attach/回读任一步失败均保留未知，不重试、不降级、不隐式清理。

native 仅在父进程执行 syscall；不进入 fork 子进程路径。安装固定使用
`BPF_F_ALLOW_MULTI`，不用可能替换旧程序的 NONE/OVERRIDE/REPLACE，不改父级策略。
遇已有直接策略、祖先禁止附加、权限不足或 query 超过 64 条均拒绝。
附加后仅关闭 program FD；直接 attachment 持有引用，不借 link FD 生命周期
自动卸载。没有 detach、bpffs pin、父 controller 准备或 sudo 接口。
删除仍只由原 CPU scope 的含后代为空和原 inode 清理合同管理。

保留仓库 MIT 许可，程序无 GPL-only helper。load/query 所需权限由内核检查，
不把普通 cpuset 写委派误认为 BPF 权限；例如 Linux v6.8 的 query 要求
CAP_NET_ADMIN，load 另检查 BPF 能力，见 [内核 syscall 实现](https://github.com/torvalds/linux/blob/v6.8/kernel/bpf/syscall.c)。

`DeviceScope.constraints()` 与被包装的 `CpuScope.constraints()` 均先验证原
attachment，再授予一次启动约束；未安装、漂移和恢复实例不能获新启动权。
恢复须使用原 CPU inode 与 DeviceBinding，只观察，不重新 install 或 start。
单独 `CpuScope.observe().device_isolation="not_configured"` 是旧 CPU 通道的占位值，
不查询 BPF；设备事实只来自独立 DeviceScope.observe，不将占位值解释成内核无策略。
设备观察明确 admission/wait 均 false；关闭 scope FD 不卸载策略。

## 边界和剩余开发

cgroup v2 的设备检查使用 CGROUP_DEVICE BPF，不是 devices.allow 文件；其内核合同
见 [Linux cgroup v2](https://docs.kernel.org/admin-guide/cgroup-v2.html)。附加标志与设备
context 见 [Linux v6.8 UAPI](https://github.com/torvalds/linux/blob/v6.8/include/uapi/linux/bpf.h)。
本接口不撤销已经打开/继承/传递的设备 FD，也不是对同 UID 可信 delegate 的恶意
逃逸沙箱。实际设备访问受已有祖先限制；放行不保证 CUDA 成功。

设备 intent/生命周期已有 [独立记录层候选](device-scopes.md)，但尚未接入实际安装；
尚未实现控制/UVM/MIG/逐卡 UUID 与设备号映射、
必要 CPU 设备白名单、持久 owner 故障恢复接入和被动健康观察。因此不能声称现有
cpu_isolation.mode=cgroup 限制 GPU，不能从 CUDA_VISIBLE_DEVICES 或 GPU claim 推断
设备隔离。正式 GPU 验收与生产切换需要另行授权。

[回归](../tests/test_device_policy.py) 的解释器/故障模型验证有界策略、unsigned
设备号、合并权限、原 inode/策略漂移、已有策略不替换和恢复不重装，均非内核证据。
native 参数拒绝在 Linux 计算节点运行；显式 `SCHED_TEST_DEVICE_LOAD_DENIAL=1`
仅允许非 root、无 CAP_SYS_ADMIN/CAP_BPF 的 load 拒绝检查，不执行 attach。
当前未取得特权正向 BPF/设备访问/后代继承证据；不能把拒绝或模型算作隔离验收。
