# 候选存储准入

当前源码提供 opt-in 磁盘空间、inode 与可知的本地用户 quota 准入，
以及独立 `sched-storage-explain-v1` 查询。默认关闭，不改变旧任务的派发策略。
它是保守声明预留，不是硬磁盘配额，不承诺外部写入后的持续容量，也不代替科学验收。
未正式发布、未切换生产；固定来源与验收进度见 [阶段清单](feedback-development.md)。

## 配置与任务声明

将补丁放在 state 之外，使用计算节点 CLI `sched config set -f <patch.json> --yes`：

```json
{
  "storage_admission": {
    "enabled": true,
    "reserve_gib": 1,
    "reserve_inodes": 1024,
    "control_reserve_gib": 0.25,
    "control_reserve_inodes": 256,
    "require_user_quota": false
  }
}
```

以上为默认值，仅 `enabled` 默认为 `false`。全部支持热更新；布尔值不得使用整数，
GiB 必须有限非负且不超过 2^30；inode 必须为非负整数且不超过 2^63−1，拒绝未知配置字段。
任务可声明 `resources.disk_gib`、`resources.disk_inodes`，省略为 0，采用同样的数值边界；
GiB 向上转换为字节。0 是无额外声明预留，不表示零磁盘权限，也不关闭全局余量检查。
即使关闭功能，提交仍校验新声明的类型；不静默接受负数、NaN、字符串或布尔数值。

任务输出目标为冻结 cwd、当前配置项目 root，以及任务/阶段声明产物的父目录；
控制面目标为实际 state 节点目录。不扫描产物内容或删除任何文件。
最多 128 个不同目录、合计 60 KiB 路径；不存在的目录采样最近的已有祖先。
未声明的任意外部路径、未来 mount/路径移动不在这个合同内。

同一 `st_dev` 文件系统合并一次：任务声明的全额预留加同文件系统 running 的全额预留，
再加任务/控制面余量的最大值。不同输出文件系统各保守记完整任务预留，不假定数据分布；
独立 state 文件系统只要求控制面余量与已有同文件系统预留，不重复计候选任务输出量。
可用字节/inode 取用户可用 `f_bavail/f_favail`，只读或不可读/未知 inode 不放行。
不扣除“估计已写入量”：running 声明可能已经消费空间，因此这个规则可能保守双计。

成功启动的 allocation 冻结任务文件系统 ID 与存储观察摘要；运行集合以该绑定计算预留，
不对 running 的远程输出目录重复探测。旧 allocation 无此绑定且任务有正的磁盘声明时，
报告 `running_storage_reservations_unknown` 并暂停新派发，不猜测、不补写历史。
没有正磁盘声明的旧任务仍按兼容规则处理；它们的实际写入会影响后续可用空间采样。

## quota 的可知范围

仅对已识别的本地 ext2/ext3/ext4/xfs `/dev/` 文件系统，查询当前 UID 的 Linux
`quotactl(Q_GETQUOTA)`；不修改 quota、不提权、不调用外部 quota 管理程序。
块 limit 使用 1024 字节单位，空间 usage 使用字节，inode 为数量；有效位分别核验。
依据为 [Linux quota UAPI](https://raw.githubusercontent.com/torvalds/linux/master/include/uapi/linux/quota.h)。
hard/soft 正 limit 取较小值，保守不借用 soft grace；无正 limit 且字段有效仅表示
`no_user_limit`。已知限额不足始终拒绝，不受 `require_user_quota` 开关影响。

远程/NFS、unsupported、权限/系统错误或有效字段缺失返回 quota `unknown`，不是无限制。
默认允许可用字节/inode 足够且 quota 未知的候选，但逐文件系统 `quota[].known=false`
仍明确保留。`require_user_quota:true` 要求当前用户字节和 inode quota 两项都可读，
任一未知暂停派发；这**不要求或证明**组、项目、远程服务等其他 quota 均已查明。
报告固定 `quota_scope:current_uid_only`、`other_quota_scopes:group_project_remote_not_verified`。

## 实际派发与解释

只有计算节点 daemon 探测。独立 stdlib 子进程以目录 FD 采样 statvfs，单次 deadline
5 秒；没有 SQLite writer 锁时执行。超时发送 kill，但不无限等待内核中不可中断的 IO；
前一个 helper 未退出时不再创建新 helper，候选保持未知/暂停。
同 tick 同目录探测最多缓存 5 秒，按当前 running 预留分别计算；实际 launch 再采样，
tick 新候选探测有 10 秒启动预算，预算耗尽的未缓存路径明确 unknown，不继续探测；
已启动的单次探测最多还可消费其 5 秒 deadline，不声称整个 tick 严格 10 秒。
后续指纹/恢复/显存检查导致观察超过 5 秒或运行集合改变时，进入最终 CAS 前重采样。
已通过一次采样不保证后续外部磁盘变化被原子锁定。

准入不通过发生在新 worker/科学产物清理之前；最初检查不预分配 GPU/CPU。
最终复查失败释放本轮已预留的 GPU，沿用既有释放协议，不杀 running，不抢占，
不自动删除科学产物。后序小任务可补位。热关闭仅关闭后续存储检查，热启用不迁移旧运行。
state 已满时记录观察本身可能失败；决定仍拒绝，查询以缺失/过期 unknown 呈现，
不能据此保证所有其他控制面写入在真实 ENOSPC 时都成功。

```sh
sched storage-explain <full-batch-id>:<task> --version 1 --json
```

查询只读私有 DB/WAL 快照和计算节点已记录观察，不探测网关文件系统、不启动 helper、
不初始化/迁移、修改状态或连接 owner。报告包括 settings/requested、逐文件系统
needed/floor/reserved_running/可用字节/inode/quota、同时拒绝与必需 unknown。
关闭时 `enabled:false, allowed:true` 仅表示未启用此 gate，不授予任务执行权。
开启且未观测/失效时 `allowed:null`；有效拒绝为 false、有效可容纳为 true，
`admission_granted:false` 与 `effect:none` 始终保持。

报告绑定 instance/node/job/spec、全部配置摘要和 running 集合，最长有效 30 秒；
身份/配置/预留变化、未来时间、不可读或丢失均明确 unknown。查询复用实际纯决策函数，
重算并核对记录，不把存储布尔摘要当权威。DB 与观察不是一个跨文件原子快照。
只记录经过前置 gate 的候选；前置预算/依赖拒绝可能没有存储观察，不能捏造全部维度已知。
单报告集合最多 1 MiB，超过时淘汰旧候选记录；查询缺失即 unknown，不回填或现场探测。

`admission-explain` 嵌套同一 storage 报告；必需存储证据未知时 resource_fit 为 null。
不新增 strict status/task/history 字段或 wait_reason（保持插件兼容），需使用独立查询查看
具体存储阻塞；不新增 DB schema。allocation 新增可选存储声明/绑定，不回填旧记录。
此报告不是硬隔离、租约验证、科学文件清理许可或 quota 变更许可。

## 验收范围

[纯决策/私有只读回归](../tests/test_storage.py) 覆盖同/异文件系统余量、running 记账、
quota 单位/有效位/soft ceiling、quota unknown 策略、只读/未知 inode、旧预留缺绑定、
超时不重复 helper、观察时效/配置/运行变化、无探测只读及最终 launch 拒绝。
[Linux CPU/CLI 验收](../tests/run_storage_accept.py) 使用独立 state 和 fake GPU：
极大声明拒绝而不填磁盘，小任务补位，产物 fixture 不被清理，控制面 floor 热更新恢复，
实际用户 quota 不可读时验证明确暂停/可选恢复。不改变真实 quota、不 mount、不占真实 GPU。
全部运行位置必须为计算节点；不能把 mock/可知用户 quota 测试说成真实组/项目 quota 验收。
