# 学生账号兼容路线

适用于没有可写 cpuset 委派和 BPF load/attach 权限的计算环境。
当前发布基线为 [v0.6.1](releases/0.6.1.md)，固定来源 `50e7d98`、writer schema 25。
发布、目标实例的安装验收和实时健康分开记录；目标状态须通过 CLI 重新查询。
此路线不修改 Slurm 父级、不申请租约、不启用设备 BPF。

## 独立的证据层

| 事实 | 判断依据与边界 |
| --- | --- |
| 原租约有效性 | 原 job/step、UID、节点、启动时间、重启次数和 CPU 声明；unknown 暂停，invalid 锁存 |
| 启动来源 | 冻结 `/proc` 父链、原 shell/stepd 的 PID/start ticks/boot/UID/context，并以精确 listpids 关联；不接受仅环境变量 |
| 当前 daemon Slurm 归属 | 启动祖先模式仍报告 `current_daemon_slurm_membership_verified:false`，不声称后台 daemon 被重新接管 |
| CPU 约束 | 原 affinity 池与不可变 per-allocation claim；worker/后代继承，应用可主动放宽 |
| 硬隔离 | `hard_isolation:false`；没有 cpuset/BPF 正向验收，不授予设备隔离能力 |

默认 `membership:cgroup` 保持不变。兼容模式须显式冷配置，禁止 observe/unknown allow：

```json
{
  "lease_validation": {
    "membership": "launch_ancestry",
    "mode": "auto",
    "unknown_policy": "pause",
    "interval_sec": 30
  },
  "cpus_total": 120,
  "cpu_isolation": {"mode": "affinity"},
  "device_isolation": {"mode": "off"}
}
```

这是合同示例，不能直接覆盖生产配置。`cpus_total=120` 为声明预算；有限原 CPU 池
仍按可信 Slurm/affinity 解析，不能扩大 mask、假造 120 个 CPU 或自动租用资源。
未知/超时/过期/身份漂移停止新派发，running 保留真实 wait/取消/超时/收尾路径。
原租约确认失效后，后来同节点的新租约不能自动解除锁存，恢复须在指定目标租约显式重启。
详见[租约合同](daemon-lease.md)、[CPU 亲和](cpu-isolation.md)与[CPU 容量](cpu-capacity.md)。

## 交付与验收

现有[启动祖先专项](../tests/run_launch_ancestry_accept.py) 已有真实既有租约来源及
控制器夹具 unknown/invalid/restart 证据，配置使用 CPU auto，上限一。
现有[CPU 亲和专项](../tests/run_cpu_isolation_accept.py) 已有实际 mask/后代、取消、
原 wait 和持久 owner 重连证据，主要故障使用控制器夹具。

联合入口为[启动祖先与 affinity 验收](../tests/run_launch_affinity_accept.py)：

```sh
PYTHONPATH=. python3 tests/run_launch_affinity_accept.py --require-native --faults
```

须从指定原租约 shell 执行，并为该解释器独立构建 native。入口仅将测试自身及其
私有 daemon 限为两个已有 CPU；固定预算仍为 120，不改变原 Slurm anchor 的 mask。
退出前通过 CLI 确认私有 daemon 停止，保留受管 state/logs。[联合合成回归](../tests/test_compatibility_affinity.py)
覆盖预算/有限池、未知/过期、漂移与原 claim 保留；不是该入口已通过的实机证明。
`--faults` 使用私有控制器故障、实际 helper 超时、私有 daemon SIGSTOP/SIGCONT 的
过期观察、私有 daemon mask 漂移和 SIGKILL/原 owner 认证重连。fixture job 状态/身份
变化仍不是原真实租约结束；不修改原 Slurm shell/stepd 或生产进程。
恢复点管理的新命令和原始 v0.5.0 边界见[升级恢复点](upgrade-snapshot.md)。

| 已交付项目 | 验收范围 |
| --- | --- |
| 兼容模式 + 固定 120 + affinity | 实际来源与有限 CPU 池、非重叠 claim、worker/后代 mask、池耗尽补位、取消和原资源释放 |
| 联合故障矩阵 | 控制器失败/超时/过期、原身份/mask 漂移、invalid 锁存，running 收尾、显式私有重启与不重复执行 |
| 恢复点管理 CLI | 有界查询、预览、显式受管清理与保留期；仅处理符合条件的已关闭点，保留关闭/必要审计，不复活回退权 |
| 新版本准备 | 0.6.0 联合验收与 0.6.1 恢复点兼容修复均已发布，固定来源/CI/原始资产见各版本说明 |

计算节点验收使用用户指定的既有租约、独立配置/state、低并发 CPU 和 fake GPU。
故障效果只施加于测试拥有的进程或 state 外的控制器输入；不修改真实 Slurm job、
生产 daemon/config 或共享 cgroup。真实租约结束另用授权测试租约或自然到期观察；
fixture CANCELLED 不算真实结束证据。原 owner 重连必须保留 attempt/身份/mask/wait，不重新 start。
没有特权环境时正向硬隔离专项保持未完成；不以缺条件 skip 或模型结果记成功。

2026-10-09 已在用户指定的既有计算租约完成上述联合入口，固定源码为
`6420b3a9ccd5f08050e9e61c74d9666ead21e93b`，使用系统 CPython 3.12.3 独立构建 native。
真实原 job/step/anchor、有限池/非重叠 claim、worker/后代 mask、补位/取消/释放、
两种 backend 原 wait、全部私有故障与原 owner 崩溃重连均通过，退出码 0；
收尾通过 CLI 确认私有 daemon stopped、无活动 CPU claim，验收后逐文件核对源码未改变。
私有来源、节点、租约标识、state 和日志另存，不写入公开仓库。

实机验收发现并修复了 claim 启动前漏读原 anchor 的问题：现在每次选择 CPU 前
重新观察冻结的原 anchor，缺失或漂移仍拒绝，即使缓存决策为 valid。
原始内核上下文不带该观察；合成回归另覆盖真实调用路径，不能预填 anchor 掩盖缺口。
之后修复普通任务父进程与 wrapper 同身份发布竞争时的已解除链接 inode 读取：
仅重开路径一次，仍拒绝外来身份、硬链接和连续竞争。最终源码
`a85435e42de35717a74a152cf86db5e10c984989` 已在同一指定既有租约中重新完成
七组完整联合验收，退出码 0；20 项启动身份回归和 17 项 native 恢复队列回归均通过。
私有 daemon stopped、活动 claim 与 pending/running 均为零，273 个来源文件核对一致。

[PR #22](https://github.com/Gczmy/sched/pull/22) 已合并至
`8f412cc2c2d4828d97dd370df61e0cb72a210c02`，与最终验收来源的 Git tree 完全一致。
[main CI](https://github.com/Gczmy/sched/actions/runs/37981724339) 14/14 通过，
0.6.0 的七项原始发布产物已完成准备和离线复核；正式发布事实见
[0.6.0 说明](releases/0.6.0.md)。后续权限准备和较大实例修复已随
[0.6.1](releases/0.6.1.md) 发布；具体安装、迁移、关闭窗口与健康证据保存在私有交付记录。
历史安装验收不代替当前健康查询。真实租约结束测试和正向硬隔离仍未完成，
前者只使用单独获授权的非生产租约或自然到期观察，后者须有实际委派权限。
