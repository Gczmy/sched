# 通用反馈修复交付清单

目标是完成下表所有项目；每个独立修复完成相称检查后单独提交、推送。
源码、CI、计算节点实机验收、正式发布、生产部署分别记录；推送不是部署。
这里只保存通用事实，实际主机、租约、会话和运行回执保存在仓库外。

## 当前交付边界

第一阶段源码提交 `5c9a170` 加入产物逐项诊断、`json_equals`、只读
`artifact-check`、请求格式/JSON 拒绝、同 RID 有界等待/多 RID 查询与
GPU 池/硬亲和交叉校验。该阶段使用 DB schema 10；阶段 3/4 引入 11/12，阶段 5 后续候选写 13。
包版本仍为 0.4.0，不能仅凭版本号推断这些候选能力已部署。合同以 [集成接口](integration-contract.md) 为准。

该提交的 [CI](https://github.com/Gczmy/sched/actions/runs/37845958558) 失败：
九个 Python/native 回归矩阵均为同一处路由 fixture 未携带完整取消前置条件。
`ba5e5e7` 只补齐 fixture 的 kind/id/status，不放宽 CLI 校验；29 项相关
本地回归通过。`ba5e5e7` 的 [完整 CI](https://github.com/Gczmy/sched/actions/runs/37846875009)
14 项均通过。

新增 [Linux CLI 验收](../tests/run_feedback_accept.py) 经真实 CPU 子进程检查
rc=0、有效/无效 JSON、类型不匹配、regex no-match/timeout、缺失文件，
以及只读检查不改状态/版本/文件/启动次数、同 RID、CAS 拒绝、查询和重启。
所有调度器操作走 CLI；临时 state 与生产隔离。纳入 CI 不代表已经通过。
`de43077` 的 [CI](https://github.com/Gczmy/sched/actions/runs/37848312749) 因验收脚本
在首次 tick 前把 queued 等待误判为 draining 而失败；`f7d561d` 修正状态/CAS 断言，
其 [独立 CI](https://github.com/Gczmy/sched/actions/runs/37849442135) 14 项均通过，包含新增
CPU/CLI 验收、default/native 独立安装与四组候选构建；这仍不等于 HPDC 验收或部署。
它不模拟真实 SSH，不代替网关 delivered/落盘延迟/断线的跨主机验收，
也不证明真实 CUDA、持久 backend 或历史失败的根因。

阶段 3 源码候选增加默认 freeze/opt-in continue_independent、私有只读 batch-policy、
request CAS 策略变更与显式重开。写库升级至 schema 11，旧批次默认 freeze；不改写旧
状态、任务、wait/身份、未知尝试与回执。独立任务继续派发不代表支持 DAG。
[CPU/CLI 失败隔离验收](../tests/run_batch_policy_accept.py) 已纳入候选 CI；
`113818e` 的 [完整 CI](https://github.com/Gczmy/sched/actions/runs/37850633202)
14 项均通过，包含真实 CPU/CLI 默认冻结、策略改变不自动重开、CAS 显式重开与独立失败
最终 blocked。尚未取得计算节点证据，不代表发布或生产已升级。

阶段 4 后续候选追加不可变首次 dispatcher 验证和独立只读查询。原始执行身份、wait/cleanup、
规则和逐项文件证据冻结；历史 wait 缺失明确未验证，不补判成功。schema 12 添加空表，
查询旧库不迁移。本阶段不提供复验/结算写接口；完整规则见 [artifact-validation](artifact-validation.md)。
`6758f60` 的 [完整 CI](https://github.com/Gczmy/sched/actions/runs/37852236705) 14 项均通过，
包含普通 CPU 和 public backend 原始 wait/产物证据、只读检查/重启不改原记录。
尚未取得计算节点证据，不代表部署。

阶段 5 后续候选加入 task CAS/instance request 的仅产物复验、显式重新结算、有限系统
退避和不可变事件查询。旧 wait 缺失、原规则/尝试不匹配、文件变化或未决执行不补判；
code=0 只表示事件已提交。新 CPU/public backend 验收以 pidfd 精确暂停临时 daemon 的
正则子进程产生真实超时，验证未改文件/原 wait 且训练启动次数不增加；脚本已扩展，
尚未取得本阶段完整 CI/计算节点证据，见 [复验合同](artifact-revalidation.md)。

## 完整阶段与完成依据

| 阶段 | 工作 | 当前状态 | 必须取得的完成依据 |
| --- | --- | --- | --- |
| 2 | 已提交修复验收 | 进行中 | 固定来源完整 CI/独立安装；计算节点隔离验收；原 RID 跨网关恢复不重复投递 |
| 3 | opt-in 独立失败策略 | 源码候选、验收中 | 默认冻结兼容、独立任务继续、running/未知启动不变、CAS/schema 迁移 |
| 4 | 不可变 artifact validation | 源码候选、验收中 | 精确 job/version、规则/产物/wait/cleanup 绑定，失败历史不可覆盖 |
| 5 | 仅复验及重新结算 | 源码候选、验收中 | 不训练/不删文件、幂等/CAS、原执行权威与失效证据拒绝、有限系统错误退避 |
| 6 | 精确依赖 | 未实现 | 冻结 instance/batch/task/version；名称显式兼容；同名或 resubmit 不漂移 |
| 7 | 任务 DAG/阻塞路径 | 未实现 | 事务内环检查；A→C、B→D 且 A 失败时 B/D 继续、C 等待 |
| 8 | bounded 精确事实/成组 pending-only cancel | 未实现 | 单次源批次 CAS；核对所有代际启动记录；中断/竞争/同 RID 恢复 |
| 9 | admission explain/装箱解释 | 未实现 | 复用真实判断，全部资源/原因/观测时效，预约不冒充硬限制 |
| 10 | allocation 身份/分层失败 | 未实现 | 不可变分配关联；原始退出/监控声明/产物/资源分层；owner 不冒充 worker |
| 11 | 磁盘/inode/quota 准入 | 未实现 | 控制面余量；unknown 明确；容量不足不删科学产物 |
| 12 | 独立发布与生产切换 | 未执行 | 最终提交 CI/原始包/hash/迁移与回退；获授权的计算节点排空/安装/验收 |
| ND-02 | 租约来源持久化/持续验证 | 未实现 | Slurm/cgroup 白名单来源；失效停新派发、不杀 running、不迁移；unknown 与退出追溯 |
| ND-03 | CPU auto 容量 | 未实现 | Slurm/affinity 保守边界；0 兼容；固定超额提示/拒绝；持续租约验证 |
| 硬隔离 | per-job affinity/cgroup | 未实现 | 明确启用和可用性；真实 CPU/设备边界；退出/取消/清理与恢复兼容 |

阶段 3 的候选合同见 [reference](reference.md)；阶段 4–11 具体约束见 [ND-04](next-development.md)，ND-02/03 与硬隔离也在
该文件记录。不能把本清单中的设计当作已可用 CLI/config。
客户端负责替代 spec/RID 冻结、reservation、lineage 与跨系统恢复，
不把客户台账或科学 gate 引入 daemon；单实例 pending-replace 依需求评估。

每阶段单独核对配套插件的严格 JSON/等待原因/CAS 合同；该仓库的功能改动
独立安排，不混入 sched 提交。阶段可分别发布，但只有实际证据齐全才记完成。
实机测试须先确认用户指定的既有计算节点租约；生产切换另行授权，
流程与 schema 回退边界见 [部署清单](execution-rollout.md)。
