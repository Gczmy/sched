# 升级窗口恢复点

命名合同 `sched-upgrade-snapshot/v1`。只提供同一实例、尚未恢复写入/启动任务的
升级窗口回退，不支持任意历史回滚，不自动启动 daemon，不恢复外部研究产物。
CLI 写库仍为 schema 25；完整 schema 10–25 且已有原 instance 的库可创建恢复点。
低于 10、身份缺失、不完整或较新的库拒绝，不在创建/验证时自动迁移或补身份。

该 CLI 已随 [v0.5.0](releases/0.5.0.md) 从 `7471c1c` 正式发布，最终完整 CI 14/14 成功。
下文计算节点测试与早期来源 CI 为历史记录；生产恢复点仍未创建或验证。

## 操作顺序

在实际计算节点，先停用旧版写入端并无损排空生产 daemon。旧客户端不认识新增门禁，
不能声称它们已被强制隔离。确认旧 CLI、看板 writer、前台 supervisor 和后台写脚本
不再运行；`--writers-quiesced` 是对这个实际操作的显式确认，不是系统自动证明。
`daemon drain --stop-when-idle` 后等待自然退出；不能用会取消 running 的 stop 代替排空。

使用候选 CLI（配置与 state 必须仍指向原实例）：

```sh
sched snapshot create --writers-quiesced --yes --json
sched snapshot verify <snapshot-id> --json
sched snapshot migrate <snapshot-id> --yes --json
```

create 返回唯一 32 位小写十六进制 ID，打开持久维护窗口。所有认识门禁的写入、
网关投递、初始化、后台 start、foreground/supervise 和直接 dispatcher 入口被拒绝。
普通只读查询仍可读原 DB/WAL 私有快照；缺库或未完成替换不会初始化替代库。
`snapshot status --json` 只报告窗口，不打开数据库。verify 只校验保存镜像，
`rollback_authorized:false`，不证明当前状态仍可回退。migrate 是窗口内唯一的数据库写入，
只运行候选 schema 迁移，并在前后核对原记录与审计过的默认新增对象。
`daemon check` 涉及可写目录探测，需在关闭维护窗口后、实际计算节点执行。

升级接受后：

```sh
sched snapshot close <snapshot-id> --yes --json
sched daemon check --json
sched daemon resume
sched daemon start
```

若升级失败且窗口仍未关闭：

```sh
sched snapshot rollback <snapshot-id> --yes --json
```

rollback 返回 restored 后窗口仍然开启，数据库为原 schema，配置/其他节点文件不变。
先切回兼容原 schema 的旧独立安装，再用候选恢复点 CLI 关闭窗口；关闭本身不迁移数据库。
随后才由旧安装检查、resume/start。不能使用新 CLI 的普通写命令代替关闭，或在回退后
用新 writer 的自动初始化再次升级。close 永久消费该 ID 的回退权；再次创建窗口也不能
激活旧点。关闭不等于启动；没有自动启动或自动重放任务。

## 保存和拒绝边界

恢复点位于 state 根的私有 `.snapshot-control/<node>/points/<id>`，目录 0700、文件 0600。
稳定共享/独占门禁位于节点树外，不随 DB 替换改变 inode；锁获取有界，忙时拒绝。
源数据库通过已有私有 DB/WAL 复制协议读取，再将完整已提交页备份成自包含 SQLite 镜像；
绝不单独复制 DB 丢弃 WAL。原 instance、schema、逐表/逐列/完整行多重集合摘要、配置原字节、
节点树的普通文件与目录清单一起绑定。源路径、目录 dev/inode、UID 和实际计算主机固定。
镜像 SHA256、SQLite quick_check 与重新计算的数据库事实均须通过。

节点 DB/WAL/SHM 之外的文件/目录必须保持不变，包括 inbox、回执 ticket、通知确认、marker
和日志。配置变化、新增/删除文件、追加回执（含 unknown）、任务/版本/执行/资源事件变化、
未经审计的新表/列均拒绝回退。只允许空新增审计表和原记录上的迁移默认列；不删除未知记录。
0.6.0 的节点树上限为 10,000 文件/目录；0.6.1 候选提高为 20,000，完整保存每一项。
单文件 128 MiB、节点文件总量 512 MiB、数据库事实 500,000 行/
256 MiB；扫描/备份有界。超限不返回部分成功，也不能据此降低保护条件。

0.6.1 候选提供 `sched-upgrade-snapshot-io-budget/v1`。网络文件系统上可显式使用
`snapshot create --io-timeout-sec 300 --writers-quiesced --yes --json`；整数范围 1–900 秒，
默认仍为 30 秒。此预算分别约束完整节点树清点/复制、镜像文件复核及原文件不变复核，
不是整个 CLI 的总时长；SQLite 备份和数据库事实原时间限制保持不变。
非默认值冻结在原 manifest 和维护窗口，并由原摘要绑定；verify、migrate、rollback
沿用该值，没有后续调大参数。旧恢复点缺少此字段时仍使用 30 秒。显式预算在 create、
verify、status JSON 中通过 `io_timeout_sec` 报告，默认输出结构保持不变。
超时保留未完成点和 creating 门禁，不允许使用其回退；显式 close 后才能重新创建。

daemon/前台 owner 或启动文件存在、running、GPU/CPU 活动分配、未决 execution/owner cleanup、
CPU/device scope 未终结时拒绝创建。旧 native session 没有充分清理证明时也拒绝；不依据
当前 PID 不存在、日志或产物猜测原执行已完成。历史 unknown 请求事实可保留，但旧写入端
必须实际停止，不能把恢复点当成隔离同 UID 不合作进程的安全沙箱。

rollback 在实际替换前保存原 DB/WAL/SHM 摘要和持久 intent，将回退前文件逐一原样移至
`before-rollback`，最后原子安装完整镜像。原始文件和 point 都保留，不把新库直接删掉。
中断后窗口保持 restoring，重复同 ID rollback 只按原 journal/digest 续接，不重新提交任务。
中断恢复须仍满足文件/实例绑定；发生漂移则保留现场并拒绝，不猜测或自动覆盖。
restoring 时禁止 close。创建失败保持 creating 并冻结写入，可显式 close 放弃未完成点；
不允许拿未完成点回退。后续源码提供下述恢复点管理扩展；不手工删除受管恢复记录。

## 旧目录权限准备（0.6.1 候选）

合同 `sched-upgrade-snapshot-permissions/v1`。既有私有 state 根下的历史目录可能仍为
0755；create 继续要求目录私有，不通过跳过这些目录创建不完整恢复点。
新增计算节点接口，在 daemon 无损排空、旧 writer 停用且不存在升级窗口时执行：

```sh
sched snapshot permissions --dry-run --json
sched snapshot permissions --writers-quiesced --expect-plan <plan_sha256> --yes --json
```

预览不初始化控制目录、修改权限或迁移数据库。计划绑定原实例、完整数据库事实摘要、
配置原字节、节点目录和每项原 inode/mtime/ctime/权限；最多 20,000 项、30 秒。
执行在独占维护锁内复核原计划，拒绝源链接、非普通文件、外来 UID、活动 owner/resource
及漂移。沿原根 FD 逐级以 O_NOFOLLOW 打开原目录，只移除 group/other 权限；
普通文件、原任务/unknown/wait、配置和 schema 均不修改。

效果前保存独立权限准备审计；完成后再次核对全部原文件和数据库事实。
中断可能留下已收紧的部分目录，保留审计，不自动放宽权限。重新预览剩余目录后，
只能使用新摘要继续修复。成功不授予启动、迁移、回退或任务重放权；之后仍需正式
snapshot create/verify/migrate。已关闭点不因权限准备复活回退权。

## 有界查询与保留期清理（0.6.0）

以下管理扩展已随 [v0.6.0](releases/0.6.0.md) 发布，不改变原升级窗口的回退权。

命名合同 `sched-upgrade-snapshot-management/v1` 独立协商，不改变升级窗口 v1 的回退权限。
此扩展尚未发布；已有 v0.5.0 安装不因文档更新取得新命令。

```sh
sched snapshot list --limit 20 --json
sched snapshot list --limit 20 --cursor <snapshot-id> --json
sched snapshot prune <snapshot-id> --retention-days 30 --keep-last 2 --dry-run --json
```

list 与 prune --dry-run 可在网关读取现有私有控制记录，不打开当前 DB、不初始化
目录或锁、不探测查询主机的进程/Slurm。list 默认 20、最多 100 行，ID keyset 实时
分页，`truncated/next_cursor` 不能拼造一致全量快照。总目录项最多 10,000；一次查询或
保留计划的 metadata 总读取最多 64 MiB、30 秒，list 输出最多 4 MiB。
读取失败不返回部分成功。查询均 `effect:none/rollback_authorized:false`。

prune 只预览单个完整且已关闭的恢复点，默认保留 30 天与最近 2 个未清理关闭点。
`retention-days` 为 0..36500，`keep-last` 为 0..100；0 须显式指定。
保留期依据关闭时持久化的 `closed_at_epoch`。旧记录只有无时区字符串时报告
`closed_time_not_recorded` 并拒绝清理，不猜时区或补写旧事实。创建未完成、镜像/审计损坏、
存在任何活动维护窗口或未知额外文件/目录时拒绝；不靠当前 PID 或任务终态取得清理权。

预览返回 `as_of`、`plan_sha256`、候选字节数和文件数。执行必须在 config.node 计算节点，
沿用同一保留参数、as-of 和计划摘要：

```sh
sched snapshot prune <snapshot-id> --retention-days 30 --keep-last 2 \
  --as-of <preview-as-of> --expect-plan <preview-plan-sha256> --yes --json
```

执行取得稳定独占维护锁，重新核对原关闭记录、配置/state/物理主机绑定、manifest、
目录 inode、每个文件摘要及 mtime/ctime；最终 unlink 前再核对身份、大小和时间，
拒绝摘要检查后的原 inode 内改写。预览变化拒绝，不另选恢复点。删除前持久化原 pruning
意图，按原目录 FD 删除精确副本，逐步 fsync；故障保留现场，同一参数/摘要才可续接。
不接受符号链接、硬链接、目录替换或未知文件。每阶段最多 30 秒，单文件 128 MiB、
总量 1 GiB；0.6.0 最多 10,000 项，0.6.1 候选最多 30,000 项，以容纳较大镜像及元数据。
忙或超限保留原记录。

仅移除该关闭点的 DB/config/节点文件副本与已完成回退的 before-rollback 副本。
原 point inode、manifest.json、rollback.json、永久 closed 记录和 prune 审计保留，
不修改当前 state/config、unknown 请求、execution、scope、任务、revision 或实例。
清理后 phase=pruned；重复同一操作重核对审计并返回原结果，不复活回退权。
verify 因镜像已清理而失败是预期行为，不能据 manifest 摘要推断仍可恢复。

## 验收范围

[私有回归](../tests/test_snapshot.py) 覆盖完整 schema 10→25→10、unknown 回执保留、
原身份/文件绑定、镜像损坏、写门禁、关闭权消费和故障注入替换续接；
[数据库事实回归](../tests/test_snapshot_facts.py) 核对追加/修改/替换记录和扫描上限。
[Linux CLI 验收](../tests/run_snapshot_accept.py) 检查独立进程写入/启动门禁、
pending 保留、显式关闭/恢复后 CPU 执行一次和旧点不可复活。加入 CI 不等于已经验收，
合成迁移/计算节点私有测试也不等于生产恢复点已创建、正式发布或生产已切换。

2026-10-09：计算节点 Python 3.12 的独立临时 state 通过上述 3 组 CPU/CLI 验收，
191 条相关回归全部通过，其中 29 条为新恢复点/数据库事实回归。中断续接使用私有
文件事务故障注入，不冒充真实断电或 NFS 故障验收。生产 daemon 未修改/重启；
新的固定源码完整 CI、发布产物与生产恢复点仍须后续验证。

`8e5c512` 的 [完整 CI](https://github.com/Gczmy/sched/actions/runs/37926114479)
失败于旧 scope-health/foreground 单元夹具：全局 time.monotonic 的有限模拟序列被新增
writer 门禁读取，导致 StopIteration。后续修复仅将模拟时钟限定到被测模块，保留
原三次采样、过期拒绝与重启间隙停止断言，不改维护门禁、启动时效或生产行为。
新来源须独立完整 CI 通过，不能以此前计算节点验收替代这个门禁。

修复来源 `b6c806e` 的 [完整 CI](https://github.com/Gczmy/sched/actions/runs/37926803183)
已确认 14/14 成功，包含新增 Linux 恢复点 CLI 验收；四份原始候选 ZIP 和独立安装
证据已核验保存。后续发布说明链接修复需要新来源 CI，不手工修改已核验资产。
生产恢复点仍未创建，正式发布与生产切换仍需分别确认。
