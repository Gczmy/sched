# sched execution 发布与部署清单

本清单用于交付准备。当前生产版本、配置和运行状态尚未查询；执行本清单前须取得对应部署授权。

已发布 0.2.2 的历史记录为 [v0.2.2](https://github.com/Gczmy/sched/releases/tag/v0.2.2)，
发布来源为 `8aa2559dc64e74acd2cec6bcb2f5481d1d9fbc1d`；
[最终来源 CI](https://github.com/Gczmy/sched/actions/runs/36881160410) 的 14 项检查全部通过。
七个原始资产已从公开 Release 下载核对，生产切换独立安排。

0.3.0 的正式来源、资产与状态以对应 Release/tag/evidence 为准，
恢复能力与真实 CUDA 验收见 [0.3.0 说明](releases/0.3.0.md)。

## 发布验收

- 确认最终提交的 Repository、所有 Python/native 矩阵及 Candidate 检查全部通过。
- 默认 wheel 不含 native extension，也没有第三方运行时依赖；native wheel 从本仓源码独立构建。
- 记录合并提交、wheel SHA-256、Python ABI 和构建方式。配套客户端仍通过公开 CLI 使用调度器。

候选产物必须从固定提交的独立源码副本构建，忽略本地 `.so`、测试状态与运行配置。
0.3.0 默认产物为 `sched-0.3.0-py3-none-any.whl`；native wheel 的 ABI/平台以实际构建结果为准。
候选目录保存 `manifest.json`、源码归档、两种 wheel、安装说明和 `RELEASE_NOTES.md`；
manifest 记录完整 commit、构建 Python/平台、各文件 SHA-256 与独立安装证据。
发布标签应指向最终验收提交，不能只凭包版本
判断新旧源码。候选构建本身不创建发布标签、Release 或生产切换。
manifest 的 candidate/published/deployed 记录构建时状态；之后的正式发布事实由
Release 与 tag 记录，发布时保留已经验收的包与 manifest 原始字节。

## CI 产物与下载验证

所有 job 使用同一源码提交：PR 使用 head SHA，push 和手动运行使用事件 SHA。
只有 Repository、Python 和 native 三组检查全部成功后，Candidate job 才开始。
0.2.2 按 Python 3.10/3.14 与 Ubuntu 22.04/24.04 保存四份 CI 产物，
名称含完整提交、Python 版本、runner target 和运行 attempt，
保留 30 天，不覆盖已有 artifact。每份包含默认 wheel 与对应 ABI 的 native wheel；
各份中的默认 wheel 不要求逐字节一致，分别以各自 manifest 的哈希为准。

manifest 的 `ci` 只保存公开的仓库、事件、源码/工作流提交、run 链接和前置 job 结果，
不会把构建中的整次 workflow 标为成功。整次 run 完成后，另查最终结果；上传成功的
artifact ID、URL、归档 SHA-256 和 manifest SHA-256 记录在 job summary。
下载归档时先与 GitHub 记录的 artifact digest 核对；解包后使用已审查源码中的校验器：

```bash
python scripts/verify_release_candidate.py <candidate-directory> --commit <reviewed-full-commit> --require-ci
```

预期提交必须从已审查的 PR 或最终 `main` 独立选定，不能从待验 manifest 自行取值。
校验器只读文件，不要求 Linux 或 native 模块；检查文件集合、哈希、归档提交、源码版本/
schema、wheel 元数据/ABI 与安装证据。它不替代对 GitHub run 最终结果的核对。
PR 产物对应该分支提交；合并后须使用新 `main` 提交的 CI 产物准备正式发布。
CI 产物会过期，正式发布前须取得独立授权并保存已验收包、摘要与对应 CI 记录。

0.2.2 增加同仓发布准备脚本和手动 workflow，验证来源完整成功矩阵，保留原始 ZIP，
生成总校验和与 evidence，可选择上传来源一致的草稿。中断后只复用经过哈希核对的
匹配资产；不覆盖已发布版本，不自动发布。入口与离线元数据契约见
[release-preparation.md](release-preparation.md)。

## 本地候选

在具备开发构建工具的本地 Linux 环境运行以下命令，可从指定完整提交生成并验证候选：

```bash
python scripts/build_release_candidate.py --commit <full-40-character-commit> --native
python scripts/verify_release_candidate.py dist/candidate-<commit-prefix> --commit <full-40-character-commit>
```

脚本通过 `git archive` 获取固定源码，分别构建和独立安装两种 wheel，并实际验证
持久 owner 的原始 wait。产物位于 `dist/candidate-<commit-prefix>`，已有目录拒绝覆盖。
本地构建不冒充 CI 产物，正式发布仍须核对该提交对应的完整 CI 并取得明确授权。
开发构建工具版本见 workflow；它们不成为运行时依赖。
默认 wheel 要求 Python >= 3.10，调度执行仍要求 Linux/POSIX。
0.2.2 CI 默认安装/回归覆盖 Python 3.10–3.14；native 候选为 CPython 3.10/3.14 /
Linux x86_64，runner 为 Ubuntu 22.04/24.04；真实 seccomp 故障验收覆盖 execveat 缺失、
close_range、memfd 和 UNIX socket 权限拒绝。尚未通过的矩阵不扩大已发布支持范围。
实际 libc 和 SOABI 写入各自 manifest，不声明 manylinux 通用兼容。
其他 ABI 或 Linux 基础环境须单独构建、安装验收并记录；不能复用不匹配的 wheel。
已发布 0.2.1 的 native 产物仍只覆盖原来的 CPython 3.10/3.14 与 glibc 2.39，
0.2.2 正式资产覆盖 CPython 3.10/3.14 与 glibc 2.35/2.39，
不改变旧 Release 的产物或兼容性声明。

安装后从无关目录验证 `sched --version`、`sched --help` 和 native 能力。默认安装不需要
编译器；native 安装须选择匹配解释器 ABI 的 wheel。两种安装都不需要客户仓库。

## 切换前核对

通过已部署 CLI 的 `--version`、`config get`、`status --json`、`history --json`、
`task <full-batch-id>:<task-id> --json` 和 `daemon status --json` 核对版本、配置和任务。
真实配置、节点、任务名称与检查结果保存为私有运行记录，不加入本仓。

旧 `strict` 和 native metadata 不会在新版本继续执行。切换前须处理旧队列与未解决启动，
不能通过 retry、clean、删除记录或重建名称绕过兼容守卫。如果现有 CLI 无法证明旧 session
状态，应先补充只读查询能力，再安排切换；不得直接读取共享源库或修改 state 文件。

## 维护窗口

登录统一使用 `ssh HPDC`；进入经实查确认的计算节点会话，核对主机与 `config.node`。
按项目操作规则由获授权的维护者执行 `daemon drain --stop-when-idle`，等待 running 与未决启动
清空、daemon 自然退出。`stop` 会取消任务，不能用它代替排空。

在独立版本目录安装已验收产物。仅在计算节点执行 `daemon check`；通过后再执行
`daemon resume` 和 `daemon start`，使用 CLI 检查健康与队列，运行另行授权的隔离 CPU 验收。
配置 backend 前审查 ELF、固定 argv/env、项目 root 与输入 slot，注册值均为冷配置。

## 回退边界

切换前保留旧安装、原配置和经正式备份流程取得的恢复点。
0.2.0 将写库 schema 升至 6，新增不可变 owner binding；0.2.1 升至 7，
新增确认队列与已记录健康，0.2.2 保持 schema 7。对应只读范围分别为 1–6 和 1–7。
旧版本不能被假定为兼容新写库。回退须在 daemon 排空后，
依据实际 schema 兼容性与已审查恢复方案执行，不能直接将旧代码覆盖到新库上。
任何已消费或结果未知的 execution attempt 都必须继续保留，不能因回退再次启动。
不能在持久 owner 仍运行或清理未决时移除原安装；存活服务仍使用原提交的代码。
默认配置不会自动启用持久 backend；启用前先验证客户程序的 FD4 owner identity 支持。

客户协议和科学验收由客户仓库独立发布；进程退出 0 不代表科学验证通过。

## 恢复 FIFO 候选迁移

候选 [恢复策略](recovery-policy.md) 增加不可变恢复结算和队列 lineage，写 schema 8、
只读完整 schema 1–8。初始化在单事务中建表并最后写 schema marker，不回填历史任务
的执行权。0.2.2/更旧二进制拒绝 schema 8；不要对新写库直接回退旧程序或删除恢复记录。
发布、安装、部署与状态迁移仍独立安排，不能把候选 PR 当作生产已升级。

候选显存与恢复观察写库升级到 schema 9（完整 1–9 只读兼容，查询不迁移）。
schema 8 可读恢复 queue/settlement，但 watch 为 null；0.2.2 无法打开新写库。

## 集成 schema 10

0.4.0 writer 原子增加实例身份与不可变提交回执，不改写旧请求或执行身份。
只读支持完整 schema 1–10。0.3.1 不能回接 schema 10；回退依赖已验证恢复点与
原请求/执行记录，不能丢弃未知事务、重生成身份或凭历史日志重放任务。

## 候选失败隔离 schema 11

当前失败隔离源码候选增加批次 `failure_policy` 和 revision trigger；旧批次全部
默认 freeze，不迁移状态、任务代际、原始 wait/身份、未知尝试或回执，不自动重开 blocked。
完整 schema 1–11 可私有快照只读查询。包版本仍为 0.4.0，不代表正式新版本或部署完成。
已发布 0.4.0 的 schema 上限为 10，升级后不能直接启动旧 writer；回退必须使用升级前
已经验证的恢复点，不把 schema 字段删除或回改 user_version 当成回退方案。
发布前必须固定最终来源，完成迁移/失败回滚、Linux CLI/default/native CI 和计算节点
隔离验收，再另行授权生产排空切换。CPU 验收不代替真实 GPU 或租约硬隔离验收。

## 候选不可变产物验证 schema 12

后续源码候选追加空的 artifact_validations 表、索引和不可变触发器，不回填历史
退出/验证，不改已有身份、任务、revision、wait 或未决回执。完整 1–12 可只读查询，
旧库查询不迁移。schema 11 候选和正式 0.4.0 不能直接启动新写库；使用升级前已验证
恢复点回退，不删除记录或降低 user_version。原始 wait 在落盘前丢失仍为 unknown，
当前产物不能补权威。冻结记录不构成复验/结算授权，具体合同见
[artifact-validation](artifact-validation.md)。正式发布及生产升级仍按独立验收流程执行。

## 候选仅产物复验 schema 13

后续候选添加空的 artifact_revalidations 表/索引/不可变触发器；不迁移或补造历史 wait、
复验或结算决定。完整 1–13 只读兼容，旧库查询不迁移。12/11 候选和已发布 0.4.0
不能直接启动新写库；回退依赖已验证的升级前恢复点，不能删事件或降低 schema。
原任务改变、新事件及 request 终态回执同事务；代码/记录写入失败整体回滚。
业务复验失败仍保存事件，code=0 不是任务 done；见 [产物复验](artifact-revalidation.md)。

## 候选精确依赖 schema 14

添加默认空的 batches.depends_on_exact 与不可变更新触发器；不将历史名称绑定到
迁移时的最新来源，不重写历史状态、revision、身份、wait、marker 或请求回执。
schema 10 及以上升级保留等待态；更旧库仍执行已有的历史等待别名兼容迁移，
不把该格式转换当作取得新的执行或 wait 权威。
完整 schema 1–14 可只读查询，旧库查询不迁移。13/12/11 候选及已发布 0.4.0
不能启动新写库；回退仍依赖已验证的升级前恢复点，不能删依赖或降低 schema。
精确来源和混合名称环在提交接受事务内检查，旧名称兼容语义保持，不能把未知或
来源失效当成功。此阶段无任务 DAG、依赖更新或科学发布合同，见 [reference](reference.md)。

## 后续候选任务 DAG schema 15

新增 append-only task_dependency_events、查询索引与不可变/保留/revision 触发器。
初始选择在所有任务插入后绑定；受审计 task CAS 更新产生新事件并链接旧事件，
不覆盖原始 spec 或批次 exact 列。混合 job/batch 环检查、事件、revision 与回执
处于同一事务。迁移不生成历史依赖事件，不重写现代等待态、旧执行身份或未知请求。
完整 schema 1–15 只读查询不迁移；所有 10–14 旧 writer 均不能回接新写库，
回退使用已验证的升级前恢复点，禁止删事件或降低 user_version。
任务声明、CAS 内联输入和只读路径边界见 [reference](reference.md)；
[CPU/CLI 验收](../tests/run_task_dependencies_accept.py) 不占真实 GPU、不替换生产安装。

## 后续候选 allocation schema 16

新增空的 allocations/allocation_events、不可变/保留触发器、jobs.allocation_id
NULL 指针及 revision/回队清空触发器。迁移不生成历史分配/退出事件，不重写原
状态、wait、执行身份、未知尝试、请求回执或 instance。启动前提交不可变分配意图，
不能把意图当作子进程已出生；原 owner/marker/恢复守卫仍生效。
完整 schema 1–16 只读查询不迁移。schema 10–15 writer 均不能回接新写库；回退只能
使用已验证的升级前恢复点，禁止删分配/事件或降低 user_version。
新 validation key 按 allocation 区分同版本 retry，旧 key/证据不改写。
独立查询、上限和真实 wait/监控/产物边界见 [allocation-evidence](allocation-evidence.md)；
生产排空、安装和切换须另行授权，CPU/fake-GPU 验收不证明真实 CUDA/worker 归属。

## 后续候选 daemon 租约 schema 17

新增空 daemon_leases/daemon_lease_events、查询索引与不可变/保留触发器；旧库不
补造出生/租约事实，不改变任务/原 wait/执行身份、allocation、请求或 instance。
首次就绪前原子持久化最小来源与首次校验；默认 auto 有 Slurm 来源时执行验证，
未知默认暂停。部署前须检查目标租约实际 job/step cgroup，不能只凭 RUNNING 或环境
宣称有效；显式 observe/unknown allow 是降低保护，不是修复隔离。
完整 schema 1–17 只读不迁移，schema 10–16 writer 不能回接新写库；回退只能使用
升级前恢复点，禁止删来源/事件或降低 user_version。新查询与告警/退出保留合同见
[daemon-lease](daemon-lease.md)。真实 lease 结束测试、真实 CUDA 与生产切换另行授权。
