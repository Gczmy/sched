# 不可变产物验证记录（源码候选）

当前候选提供首次验证记录和只读查询，**尚无复验或重新结算写接口**。
包版本仍为 0.4.0；必须查询实际部署的 `version --json` 中
`contracts.artifact_validations=sched-artifact-validations-v1` 与 schema 范围。
推送、CI、计算节点验收、正式发布和生产切换分别记录，不能混用。

## 原始观察与权威边界

dispatcher 在正常退出或 ready probe 的结算路径追加记录，与任务原始结算共用
同一写事务。非零退出也保存当前规则检查，但通过产物检查不会把非零退出改成成功。
context 为 `exit_zero`、`exit_nonzero` 或 `probe_ready`。取消、超时、未知退出和
非法 spec 不用当前文件补造成功记录。这里保存的是首次 **dispatcher 结算检查**，
不是重建之前的 inline stage validator、客户科学 gate 或 worker 的退出结果。

记录冻结 instance、batch/task/job/version、retry/start、指纹、批次 revision、完整
spec 摘要、task/stage 规则、逐项文件证据、原始返回码及已证明的 wait/cleanup。
原始 cmd/env 不包含在查询证据中，只通过 spec 摘要绑定。原始 backend identity
也只保存摘要；不复制 owner 的重连凭据。记录 ID 为规范有限 JSON payload 的 SHA-256，
规则、wait 和 checks 各自还有摘要。完整记录读取验证这些摘要与索引字段的绑定。

公开 execution attempt 的真实 terminal observation 必须与精确版本、PID、原始整数
返回码、`phase:exited`、`status:exited` 和 `group_clean:true` 一致。普通命令路径只在
daemon 仍持有该 supervisor 的 Popen、实际 poll 取得退出码、强启动身份绑定及进程组
清理已确认时记 `wait_verified:true`；subject 是 `scheduler_supervisor_command_chain`，
不冒充科学 worker。sidecar RC 不一致、身份不确定或原 Popen 已丢失时不会补造 wait。
仅有历史 job.rc、PID、日志或当前产物时保存 `wait_verified:false`，原因明确。
若原始 wait 在落盘前因崩溃丢失，之后检查不能重新产生这份原始事实。

首次失败不可 UPDATE/DELETE，也不能因文件后来通过而覆盖。相同完成绑定重复进入
结算路径读取原记录，不重查文件；普通 retry 即使复用 version，也按 retry/start 保留
独立原始记录。这不意味着 retry 是科学上相同的尝试，客户仍自行管理科学 lineage。

内容规则最多读取 1 MiB，并记录读取字节的 SHA-256；纯存在/min_bytes 规则不读取
大文件内容，SHA-256 为 null。file_identity 用十进制字符串保存 device/inode/mtime_ns/ctime_ns，
避免 JSON 客户端丢失纳秒/uint64 精度；
检查期间属性变化失败；属性不是内容证明，不表示所有文件已完整哈希，也不能证明
客户协议要求的产物来源。规则中的路径和有限诊断信息仍属于私有运行信息。

单条完整记录最多 4 MiB。超限在终态发布前失败，事务回滚，不截断或丢弃历史证据；
这类任务仍须维护解决，不能把写记录失败算作科学内容错误或训练成功。

## 只读 CLI

```bash
sched artifact-validations <batch-id>:<task> --version 1 --limit 20 --json
sched artifact-validations <batch-id>:<task> --version 1 --validation-id <sha256-id> --json
sched artifact-validations <batch-id>:<task> --version 1 --limit 20 --cursor <sha256-id> --json
```

可在网关查询，使用私有只读 DB/WAL 快照，不访问任务文件、不连接 execution owner，
不初始化、迁移、重新结算或创建版本。与计算节点的 `artifact-check` 当前文件检查独立。
省略 version 查询该精确批次/task 的所有已有版本；指定版本必须存在。
名称是显式旧兼容入口，自动化应使用完整 batch ID。

摘要 limit 为 1..100，默认 20，不含 payload；完整证据每次只能按 validation-id 读取一条，
该 ID 仍须属于指定任务/版本，不能跨任务读取。cursor 与 validation-id 互斥。
按 validation_id 实时 keyset 分页，新追加 ID 可能排序在已读游标之前；续页不能合并
成完整当前态。检查 `truncated` 和 `next_cursor`，保留 `pagination:live_keyset_not_complete_snapshot`。
摘要只是索引，不能替代完整证据校验或授予结算权。

JSON 使用 schema_version 1、query `artifact_validations`、独立命名 contract，包含
instance_id、batch_id、task_id、version_filter、available/reason、validations、truncated、
next_cursor、evidence_included，以及 `effect:none`、`historical_failure_reconstructed:false`、
`settlement_authority:false`。成功查询返回 0，记录中 passed=false 不代表查询失败；
非法参数、任务/版本不存在、读取/证据校验失败返回非零，不能当作空记录或未执行。

## schema 与后续开发

写库升级到 schema 12；完整 schema 1–12 支持只读。旧库没有表时返回
`available:false,reason:migration_required`，不迁移、不从历史状态回填。写初始化原子
添加空表/索引/不可变触发器，不改已有任务、版本、状态、revision、身份、wait 或未知回执。
schema 11 候选及已发布 0.4.0 不能直接启动新写库；回退使用升级前已验证的恢复点，
不能删表或回改 user_version。

阶段 5 才实现独立复验/结算，需要新追加观察、CAS/幂等与原始执行权威、规则和文件来源
校验；不能把本页查询的 passed=true 或 wait_verified=true 单独当作重新结算许可。
精确依赖、DAG、allocation 身份和分层 worker/validator 失败同样按
[完整清单](feedback-development.md) 独立开发。
