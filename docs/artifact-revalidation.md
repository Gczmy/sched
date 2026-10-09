# 仅产物复验与重新结算（源码候选）

候选合同为 `sched-artifact-revalidations-v1`，引入时写 schema 13；当前候选写 25、
完整只读范围 1–25，见 [reference](reference.md)，复验合同本身不变。
包版本为 0.5.0 候选，须协商实际部署的合同/schema；源码推送不是新发布或生产部署。
首次记录见 [artifact-validation](artifact-validation.md)，本接口不替代科学验收。

## 命令与回执

```bash
sched request <rid> --json --expect-kind task --expect-id <full-batch-id>:<task> \
  --expect-status blocked --expect-version 1 --expect-revision <current-batch-revision> \
  --expect-instance <instance-id> --expect-project <project> -- \
  artifact-revalidate <full-batch-id>:<task> --validation-id <initial-id> --yes
```

默认只复验并追加事件。增加 `--settle` 才允许重新结算；增加 `--reopen` 需要 settle，
只有结算成功且 blocked 批次没有其他最新失败/中断任务、有未完成任务时，才显式恢复
active。不改变 failure_policy，不重试失败任务、不创建版本、不申请/释放分配，running
继续按原生命周期运行。全部成功的批次由 daemon 原有终态流程检查和发布，不伪造 marker。

操作只在配置的计算节点执行，禁止直接调用写命令或从网关绕过。必须经 task CAS
request，使用完整 ID、状态/版本/当前批次 revision 和 instance；project 可选。
规则、源 spec/指纹、retry/start、原始记录 ID 都冻结，不能修改参数去复用 RID。
相同 RID 返回相同回执，不重新读文件；未知 RID 结果仍返回 75，不能换 ID 自动重投。
格式错误 64、CAS/源绑定等冲突 65 不产生复验事件；回执按原 request 协议持久化。

`request --json` 的 code=0 表示 **事件已提交**，不是产物通过或任务成功。
读取 `result.effect.artifact_rules_passed`、`settled`、`reason`、`revalidation_id` 和任务状态。
系统无法确认、产物失败或来源变化也可成为 code=0、settled=false 的不可变审计事件。
不会因“失败时整体回滚”丢失这次检查，但若写事件/证据超限失败，任务改变与事件整体回滚。

## 成功结算所需证据

只接受该精确 job/version 的首次 artifact 失败，当前仍为 failed/blocked、failure=artifact。
实例、完整 spec/规则摘要、指纹、retry/start 必须与原记录一致，不能用旧训练尝试验证新尝试。
原记录必须有可信 wait 和 group cleanup，rc/kill_reason 未变：正常零退出，或原始
ready probe 的既有成功政策；后者保留真实非零退出码，不改写为零。
普通非零 command chain、训练/OOM、取消、超时不能凭产物复验变成成功。

公开 execution 原始 terminal wait/identity/config 摘要必须未变；普通命令使用已保存的
真实 supervisor wait，不把 owner、sidecar 或应用日志当 worker wait。历史 wait 缺失
或存量没有原记录时，不补判成功。所有同 task 代际的 running/interrupted、未终态
attempt、旧 native session、未决 launch marker、取消意图和未释放分配都会阻止结算。
检查前及发布前均验证当前进程组确已消失；不确定/PID 被复用而出现活动组时保守拒绝，
不信号、不移除 marker、不重连或重新 start execution owner。

内容规则需要相同原始字节哈希，所有文件还需要相同 device/inode/mtime_ns/ctime_ns 和
大小；同内容重写/替换文件也拒绝。原先已通过的纯存在/min_bytes 规则可使用未变的
内核文件身份，**不假称有完整内容哈希或科学 provenance**。原失败文件没有足够原始
文件证据时拒绝，例如首次 missing_file/过小文件不能用后来新建/重写的文件补判。
用户仍可 `artifact-check` 做只读诊断，但该检查不授予历史结算权。

复验不扩大路径范围，paths_escape 的源记录不支持重新结算；正常 cwd 采用 no-follow
目录 FD 校验。检查结束再安全复核文件身份，记录 publication_metadata。证据是该时刻
的观察，不承诺外部程序将来永不修改文件，也不取代客户产物 producer/科学加载合同。

成功仅将当前 job 改为 done、清除当前 artifact failure。原 rc、kill_reason、执行时间、
训练 retries、version 和原始 wait 保留；原失败不可变记录及新事件 previous/decision
均可查询，不把当前状态更新当作删除失败历史。

## 有限系统退避与记录

`--system-retries N` 为 0..2，默认 0；仅对 regex_timeout，或已分类的
regex_child_start_error/io_error 且 errno 为 EAGAIN/ENOMEM/EMFILE/ENFILE/ETIMEDOUT/ESTALE
重试。每次重新检查相同冻结规则，延迟 0.1、0.2 秒，所有尝试各自保存在 attempts。
JSON 错误、真正 no-match、值不匹配、权限错误、未知 child exit 不自动重试或放宽规则。
这个计数与训练 retries 完全独立，不执行训练或清理产物。

每次 request 最多 32 条原始规则观察、最多 3 次检查；规则检查共用 4 秒预算，发布前
元数据复核预算 0.5 秒，正则子进程单次最多 1 秒。预算在打开/分块读取/子进程之间
检查，不声称能硬中断已阻塞的内核文件系统调用。单个内容规则仍最多读取 1 MiB，
完整事件最多 4 MiB，超限不截断、不提交部分结算。复杂任务可只读诊断，不绕过限制。

```bash
sched artifact-revalidations <full-batch-id>:<task> --version 1 --limit 20 --json
sched artifact-revalidations <full-batch-id>:<task> --event-id <sha256-id> --json
```

查询可在网关执行，私有只读快照，不读当前文件、不迁移。独立 JSON query/contract、
events（摘要不含 payload）、truncated/next_cursor；limit=1..100，默认 20。
event-id 读取一条精确任务/版本的完整证据并校验哈希；与 cursor 互斥，非法/不匹配返回
非零。实时 event_id keyset 分页不能合并为完整当前态。旧 schema 没有表时返回
available=false/reason=migration_required；写迁移仅追加空表/索引/不可变触发器，不从
旧失败、日志或当前文件回填。schema 12/11 候选和已发布 0.4.0 不能直接回接新写库，
回退使用升级前验证的恢复点；不得降低 user_version 或删除审计记录。

复验接口本身不定义精确依赖或任务 DAG；当前候选的独立依赖接口见
[reference](reference.md)，各阶段验收/交付状态见 [完整清单](feedback-development.md)。
