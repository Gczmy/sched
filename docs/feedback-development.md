# 通用反馈修复交付清单

目标是完成下表所有项目；每个独立修复完成相称检查后单独提交、推送。
源码、CI、计算节点实机验收、正式发布、生产部署分别记录；推送不是部署。
这里只保存通用事实，实际主机、租约、会话和运行回执保存在仓库外。

## 当前交付边界

第一阶段源码提交 `5c9a170` 加入产物逐项诊断、`json_equals`、只读
`artifact-check`、请求格式/JSON 拒绝、同 RID 有界等待/多 RID 查询与
GPU 池/硬亲和交叉校验。该阶段使用 DB schema 10；阶段 3/4/5/6 引入 11/12/13/14，阶段 7 后续候选写 15。
阶段 10 后续 allocation 候选写 16。包版本仍为 0.4.0，不能仅凭版本号推断这些候选能力已部署。合同以 [集成接口](integration-contract.md) 为准。

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
最终 blocked。后续固定候选 `12967f9` 在计算节点的同一 CPU/CLI 脚本也通过；
不代表发布或生产已升级。

阶段 4 后续候选追加不可变首次 dispatcher 验证和独立只读查询。原始执行身份、wait/cleanup、
规则和逐项文件证据冻结；历史 wait 缺失明确未验证，不补判成功。schema 12 添加空表，
查询旧库不迁移。本阶段不提供复验/结算写接口；完整规则见 [artifact-validation](artifact-validation.md)。
`6758f60` 的 [完整 CI](https://github.com/Gczmy/sched/actions/runs/37852236705) 14 项均通过，
包含普通 CPU 和 public backend 原始 wait/产物证据、只读检查/重启不改原记录。
后续固定候选 `12967f9` 在计算节点通过普通 CPU 首次验证/重启不变验收；
public backend 的本阶段证据来自 Linux CI，不把它描述为计算节点 native 验收。

阶段 5 后续候选加入 task CAS/instance request 的仅产物复验、显式重新结算、有限系统
退避和不可变事件查询。旧 wait 缺失、原规则/尝试不匹配、文件变化或未决执行不补判；
code=0 只表示事件已提交。新 CPU/public backend 验收以 pidfd 精确暂停临时 daemon 的
正则子进程产生真实超时，验证未改文件/原 wait 且训练启动次数不增加；脚本已扩展，
`12967f9` 的 [完整 CI](https://github.com/Gczmy/sched/actions/runs/37854068642) 14 项均通过，
包含普通 CPU 与 public backend 的实际超时和重新结算。计算节点的隔离普通 CPU/CLI
验收也已通过；这不等于真实 GPU 验收、正式发布或生产切换，见 [复验合同](artifact-revalidation.md)。

## 两主机隔离回执验收

[手工验收脚本](../tests/run_gateway_feedback_accept.py) 使用一个新建的共享目录，
计算节点与网关运行不同角色。所有调度器读写仍走 CLI；不自行打开共享 SQLite，
不编辑 state，不申请新租约，不读取生产配置。脚本只生成一个 CPU-only 任务，
daemon 使用 fake GPU；错误时保留 fixture，只有显式 cleanup 且 CLI 确认私有 daemon
已停止后才清理本次创建的临时目录。

使用固定候选源码和两边都可见、尚不存在的 `ACCEPT_ROOT`；在源码目录运行
`PYTHONPATH="$ACCEPT_SOURCE" python tests/run_gateway_feedback_accept.py <role> --root "$ACCEPT_ROOT"`。
Python 必须满足项目版本要求。计算角色还要求已核对的 Linux/Slurm 租约 shell；
节点身份取自该 shell，而不是网关或旧会话记录。

1. 计算节点 `prepare`：初始化独立身份和配置，daemon 保持 stopped/drained。
2. 网关新连接 `deliver --pause-after-delivery`：投递后不回传 submit 回执/批次 ID；
   观察 `TRANSPORT_DISCONNECT_WINDOW` 后断开本次测试 SSH 客户端。
3. 网关另一个新连接 `ticket`：仅按原 RID 恢复 delivered ticket；有界查询仍报告
   未入库，显式同 RID/同绑定 replay 不重复投递。这不是任意断线时点的混沌验收。
4. 计算节点 `consume`：启动私有 drained daemon，收编 inbox，但不启动 worker。
5. 网关新连接 `confirm`：私有 DB/WAL 快照确认同 RID 的 done/database 接受回执，
   与 ticket 的 binding digest 一致；显式 replay 后仍只有一个 pending job。
6. 计算节点 `finish`：恢复派发，验证一次实际 CPU 执行、原 wait/产物证据；
   私有 daemon 排空重启后仍无第二次执行。
7. 网关新连接 `final`：身份、原 RID、接受回执和一次执行计数均保持不变。
8. 计算节点 `cleanup`：只清理这个已停止的独立 fixture。

实际节点、会话、租约、目录和原始回执记录保存在仓库外。此验收不覆盖未知 intent
窗口的故障注入，也不证明任意 NFS 故障、真实 CUDA 或生产部署安全。

2026-10-08：固定实现来源 `12967f99a7ed79dbda9292d5784e70212e3684a0`
通过上述两主机隔离验收：实际 SSH 投递后断开并重连，ticket/database 回执保持
同一绑定，计算节点收编及私有 daemon 重启前后仅一批次/一版本/一次 CPU 执行。
新增手工脚本 SHA256 为
`d8350023c0b5af686ece5c7fb7c8184bbf0012716aa2547104ebcc9c6c839306`。
使用私有临时源码和 state，不替换生产安装；具体原始证据不纳入公共仓库。
该通用脚本提交 `0a760da` 的 [独立 CI](https://github.com/Gczmy/sched/actions/runs/37855828681)
14 项均通过，不以这个脚本的绿色 CI 冒充两主机验收；两主机证据来自上面的实际运行。

阶段 6 后续候选提供显式 depends_on_exact、事务内来源/混合环校验、冻结 job/spec/fingerprint
和独立只读 batch-dependencies。schema 14 默认给旧批次加空清单，不重绑名称。
解锁及派发复核原版本，未决来源暂停 pending 而不取消 running；同名/新版本成功不能
替换失败原版本。[CPU/CLI 验收](../tests/run_exact_dependencies_accept.py) 已纳入候选 CI；
本阶段计算节点隔离 CPU/CLI 已通过：失败源的成功子集继续、v2/同名新批次不替换
失败原 v1、显式选择原批次 v2 后只运行一次、重启保持绑定/等待/执行次数。
实际测试源码/脚本摘要已核对，私有证据不入仓库；固定来源
`b40c95bd472d6ebcc7f79d441cbf8c6de5b32c49` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37858263574) 14 项通过。
详细行为与旧 status 字段边界见 [reference](reference.md)。

阶段 7 候选增加本地 task/version 与任务级 external exact DAG、独立
task-dependencies 的有界已记录路径，以及 task/instance CAS 的 dependency-update。
事件不可变且链接旧绑定，事务内检查任务/批次混合环，resubmit 继承生效的固定选择。
计算节点最终候选隔离验收通过：A 失败时 B/D 继续、C 等待；A v2 成功不自动替换；
显式 CAS 更新保留旧事件，同 RID 不重复；成环更新原子拒绝；私有 daemon 重启后
C v1 只执行一次；后续改变来源后 C v2 不会 SKIP 旧绑定的产物。
最终候选 [CPU/CLI 脚本](../tests/run_task_dependencies_accept.py) SHA256 为
`2193aee454c6d8f58a0505eb7b621716cec1324382465ee4b1caa364b028e488`，
源码/测试摘要已实查一致；16 条 Linux 私有 fixture 和 143 条相关回归通过。
现有看板严格字段/等待原因已只读核对，无配套仓库改动。
不占真实 GPU、不重启生产 daemon、不更改生产配置；固定提交
`e2d7fef08b13092192da1e8bebbc63df0f654ba0` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37860518170) 14 项全部通过。

阶段 8 候选加入独立 task-facts 和一次 batch/instance CAS 的 cancel-pending，
不改 status/task/history 字段，不新增 schema；全部代际启动/旧未知/恢复/原始 validation
及计算节点启动文件重验后才取消。失败整组回滚，同 RID 原回执恢复，unknown 不重放。
[隔离 CPU/CLI 验收](../tests/run_pending_cancel_accept.py) 已通过：精确 A/C 取消，
B/D 保留并只实际执行一次；测试 CLI 提交前 SIGKILL 全部回滚，提交后丢回执仍恢复
原结果；两个竞争 RID 一次成功、一次冲突；真实失败 retry 清空当前字段后仍拒绝
误判为从未启动。故障注入仅针对测试拥有的 CLI 进程和确定事务/回传窗口，不宣称
覆盖任意网络/NFS 故障。最终脚本 SHA256 为
`99e28db7c2c73c57dfaf4c885553b85d181cb7fbe30f58b981e002024a2fa192`；
161 条相关 mock/只读回归通过，运行时来源摘要已实查一致。生产 daemon/config 不变；
固定提交 `a31361c42f95f51a6e5ae3c1524a5f943db5f3d6` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37862279472) 14 项全部通过。

## 完整阶段与完成依据

阶段 9 候选提供独立 admission-explain，预算和 legacy/fresh VRAM 选卡与实际
派发复用同一决策函数。只读查询列出同时拒绝原因、逐卡装箱、优先级与补位，使用
已记录计算节点观测而非网关探测；过期/配置滞后/运行预留变化明确 unknown。
声明预留不是 affinity/cgroup 硬限制；最终产物/执行/恢复/CAS 仍须实际派发复核。
[隔离 CPU/CLI 验收](../tests/run_admission_accept.py) 通过：未观测时明确未知，
实际 daemon 观测后同时解释 CPU/内存/两卡容量拒绝，后序小任务只补位运行一次，
重复解释不改版本/revision/分配/文件/执行次数。计算节点 26 条相关回归通过，
包含 fake 外部占用下的实际普通 CPU 执行；扩展计算节点回归的 168 条均通过。
真实 CUDA 未测试，生产 daemon/config 未改。
旧共享装箱脚本的 7 组场景、软亲和优先及硬亲和独占拒绝范围验收通过；80 条本地
mock/只读回归通过。最终 CPU/CLI 脚本 SHA256 为
`bd6d98131c47865214bc627bfd28c104b969abc6fbdd8ad1e50af73ae88abc95`，
运行时源码/脚本摘要已核对一致，私有节点/租约事实不入仓库。
最终只读资源格式守卫另通过 14 条本地及计算节点查询回归，最终来源的 CPU/CLI 脚本通过。
固定来源 `22c20f80c172bce6817bd5ed6c1dd143c07448bf` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37864183197) 14 项均通过。
不能把解释报告当作启动许可。

阶段 10 候选追加不可变 allocation 与分层观察，写 schema 16；普通 retry 即使
复用 job/version 也独立关联，首次 validation key 包含 allocation，不覆盖旧失败。
监控声明、scheduler 分类、原 supervisor/backend-child wait 与产物/资源事实分开。
计算节点首次隔离验收发现 SIGKILL wait 在 Popen 被移除后丢失，修复为有界精确
pid/start-token 内存保留，不改变兼容 poll_rc 或从历史日志/文件补判。
最终 [CPU/CLI 验收](../tests/run_allocation_accept.py) 通过：rc=1 与 rc=0/产物失败
分别保留；ready/done 仍呈现非零 supervisor wait；fake GPU 记账释放不声称物理占用；
普通 retry 保留前次不可变失败、实际运行两次；只读/私有 daemon 重启不新增运行。
最终脚本 SHA256 为
`ef5c0e7ea88c727a0e20c0e59115cc62851be239ef5bb781d5d37ad02c4392cd`。
计算节点扩展回归 336 条通过（4 条因 native 不可用跳过），包括实际普通 CPU
执行与恢复；本地 109 条 mock/私有只读回归通过。
owner service/直接 child 区分、真正旧 schema 15 迁移不回填、同秒 retry、事务回滚
及历史 pending cancel 守卫纳入回归。未测试真实 CUDA、未修改生产 daemon/config。
固定来源 `067c458d73acee80c0629e5d14725bc3b7f78e00` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37865886944) 未通过：
Python 3.10 任务 DAG 验收发现旧已完成 allocation 误阻止未启动新版本更新依赖；
其余仓库/普通回归/native 项通过，candidate 被阻断，不能记录为完整绿色。
后续单独修复仅限定当前版本的 allocation 守卫，保留跨代际运行/未知/marker/进程组
守卫及不可变旧记录；增加旧版本已完成允许新版本更新、同版本 retry 清空仍拒绝的回归。
修复固定来源 `873dca533f53ed3411a91e3ae15f93740483242c` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37867186601) 14 项均通过，
计算节点任务 DAG 验收通过，包括新依赖绑定不误 SKIP 旧产物；31 条相关
mock/只读回归通过。完整合同见 [allocation-evidence](allocation-evidence.md)。

阶段 11 候选增加 opt-in storage_admission：磁盘字节/inode、当前 UID 可知的本地
quota、输出与 state 控制面余量。默认关闭；声明预留不是硬 quota，组/项目/远程
quota 未知不冒充无限制。独立 storage-explain 与 admission-explain 嵌套报告共享
实际纯决策，已记录观察有身份/配置/运行绑定与 30 秒时效；不探测网关。
最终计算节点 [CPU/CLI 验收](../tests/run_storage_accept.py) 通过：极大声明拒绝发生
在 GPU 分配/worker/产物清理之前，小任务补位一次，控制面 floor 热更新恢复，
实际用户 quota 不可读时明确 unknown，require_user_quota 暂停、显式可选后只运行一次。
最终脚本 SHA256 为
`15592f03b12e412405acf621d01d3f3a96eb19ddb3892b7749e5e174a2ed1814`。
计算节点扩展回归运行 206 条：205 通过、1 条 native 不可用跳过；本地 101 条
mock/只读回归通过。源码/脚本摘要已核对一致，测试 daemon 停止后只清理自有临时
fixture；不填满磁盘、不改 kernel quota、不 mount、不占真实 GPU、不改生产。
配置读取失败暂停而非把任务误判执行失败；超时 helper 不重复创建，tick 探测预算
耗尽明确未知。旧库不迁移/补证，strict status/task/history/wait_reason 不变，配套
插件已只读核对。固定提交 `4363c66233be6b69ce11b70e997f6a8210a0a7ab` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37868471261) 14 项全部成功；
完整合同见 [storage-admission](storage-admission.md)。

ND-02 候选新增 schema 17 不可变出生/校验/退出历史与独立 daemon-lease；默认
健康 JSON 不变，嵌套扩展仅显式 include-lease。auto 有 Slurm 来源时校验，
unknown 默认暂停，确认失效后锁存、不杀 running、不迁移同节点新租约。
计算节点 [CPU/CLI 验收](../tests/run_cluster_lease_accept.py) 通过：真实来源观察
明确 unknown 且默认 pause 不创建 allocation/worker；测试夹具改变控制器回答后
停新派发、运行中 CPU child 自然完成、
allocation 关联精确 daemon lease、通知 CLI 确认、后续 RUNNING 不能恢复；仅显式
私有重启后排队任务运行一次，历史保持不变。不取消真实租约，不占真实 GPU，
不改生产。最终验收脚本 SHA256 为
`f215d65b7f0d0966f0826a9d3872b2c23557631d5d124d1f747106d87f61cb87`；
计算节点系统 Python 3.12.3 回归运行 810 条，801 通过、9 条 native 不可用跳过；
本地相关 mock/只读回归 76 条通过（其中租约专项 24 条）。初次 Conda 解释器缺少
memfd_create 导致 3 条既有执行 backend 回归报错，切换支持该能力的系统解释器后
最终全部通过；没有用兼容降级掩盖该环境差异。源码/脚本摘要核对一致。
固定提交 `ae652b3b5aa0b0076104fe762d44a6d87b265c65` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37871104844) 14 项全部通过；
合同与仍未覆盖的真实租约结束/硬隔离边界见 [daemon-lease](daemon-lease.md)。

2026-10-09：ND-03 的 CPU auto 源码候选通过指定既有租约内的隔离 CPU/CLI 验收。
真实原始 affinity/Slurm 来源先单独观察；默认未知暂停没有产生 allocation/worker。
随后只修改 state 外 fixture 控制器回答，验证来源最小值/上限、GPU 与 CPU 共用
冻结预留、热缩容不杀 running、超额 pending 无启动记录、控制器 unknown 暂停、
确认 invalid 后 RUNNING 仍锁存，以及显式重启/零值并发回退/各任务只执行一次。
没有修改真实 Slurm job、使用真实 GPU 或变更生产 daemon/config。
计算节点系统 Python 3.12.3 全量回归 830 条：821 通过，9 条 native 不可用跳过；
本地相关 mock/只读回归 88 条通过（CPU 专项 20 条）。首次回归中一个旧租约门禁
Mock 未提供新增容量上下文；明确该窄夹具只隔离租约门禁后全量通过，没有放宽
运行代码。固定 runtime/fixture 清单摘要为
`edd334512b5b84af4df2c34140c8b929d3296fbe940a6ee5740c5c80f04ee5a9`，
CPU 验收脚本 SHA256 为
`c67e9236417519155b9fd764948837e65b5af15d48b9549fe169f2b2c6ea9962`；本地与
计算节点摘要一致。临时 state 经 CLI 停止确认后清理；源码/传输文件亦已清理。
固定提交 `bed2c0aac6bcf5af840d0c071d74ec23baad5b8f` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37873350599) 14 项全部通过；
[CPU 容量合同](cpu-capacity.md) 仍明确非 per-job
硬隔离、非真实租约终止/CUDA 验收、非发布或生产切换。

per-job 硬隔离的第一层候选提供通用 LaunchConstraints，不新增 scheduler 配置或
schema：subprocess、linux_fd、linux_fd_owner 都可在用户 exec 前绑定显式 CPU mask
与保留 cgroup.procs FD；旧调用不变、失败不降级。计算节点明确构建 native 后
全量回归运行 864 条：862 通过，2 条平台专项跳过；16 条启动约束专项包含实际
CPU/后代继承、native 启动失败无客户副作用、原始 wait/取消/FD 生命周期、只读
真实 cgroup FD 拒绝以及持久 owner 认证重连。两个旧 supervisor fixture 曾因
实际 Slurm 来源 unknown 被生产默认 pause 阻止；仅对独立执行 fixture 显式 observe
后通过，未放宽租约守卫。固定 runtime/fixture 清单（164 文件）SHA256 为
`2c8d8afce1e129efe8b636fec599367e388c4a8406282624c935b8e90b5966c3`，
本地与计算节点一致；本地 7 条纯值/Mock 检查通过。当前计算环境没有用户 cgroup
写委派，因此不宣称实际 cgroup join、controller/设备硬隔离或 CUDA 验收通过。
同一来源的 public execution CLI 全部 12 组场景通过：普通/default 不回归，直接
原始 wait/rusage、取消/timeout、缺失或摘要漂移拒绝、无权重建 wait 的 daemon
重启、持久 owner 重连/丢失均保持原契约，实际故障仅注入独立 fixture。
scheduler 的分配/scope 持久绑定/清理/恢复仍待实现；具体边界见
[execution constraints](execution-constraints.md)。固定提交 `90164d81dfd7564dc739c861181a208437bb25de`
的 [CI #37876034264](https://github.com/Gczmy/sched/actions/runs/37876034264) 全部
14 项通过；未发布/部署。下面的 scheduler affinity 层独立验证、提交。

per-job 工作的第二层候选接入显式冷配置 cpu_isolation.mode=affinity（默认 off），
以 schema 18 在启动前事务固定 allocation CPU 集合/唯一活动 claim，三种 backend
均使用同一启动约束；只有原清理事实确认后释放，不补造 wait。普通 CPU/fake-GPU
及后代的实际 mask 不重叠，池耗尽在分配前保持 pending；释放、取消、小任务补位、
native 原始 wait 和 cold 配置暂停/显式重启均通过独立计算节点 CLI 验收。
专项故障只杀独立 fixture 的精确 daemon：原持久 owner/attempt/CPU claim 跨重连
保持，任务只运行一次，原 wait 后才释放 CPU 给排队任务，不迁移旧绑定。
admission-explain 共用池/claim 决策，查询不探测网关，未知原 owner/过期观察不当作
可用容量；默认严格 JSON 和 cpu 等待原因不变，已核对配套插件，无需跨仓库修改。
同一 runtime 来源完整回归 877 条：875 通过，2 条平台专项跳过；CPU claim 专项
13 条通过。固定 runtime/fixture 清单（167 文件）的 SHA256 为
`b53f8d9e56f89173bf646fd041b1b6d7e4c95442733e8c4c094c7faa2f26416b`，
本地与计算节点一致。初次 CLI 的 native fixture cwd 不符合已有注册 root 契约，
只修测试程序的固定工作目录，没有放宽执行校验。CPU 亲和可被应用扩大，不是
非可越界 cpuset；cgroup scope/设备策略仍未实现、未取得真实委派/设备验收。
具体可用候选与剩余边界见 [CPU 亲和](cpu-isolation.md)。固定提交
`3fd4f488fc075cd1c798f1a206f430ece3eba510` 的
[CI #37878283268](https://github.com/Gczmy/sched/actions/runs/37878283268) 全部 14 项
通过；未发布/部署，没有新租约、真实 GPU 使用或生产变更。

第三层候选新增独立 [CPU scope 原语](cpu-scopes.md)，不改变 schema 18 或默认派发。
调用方需先持久化唯一 intent，再持久化真实 inode 后配置；明确要求已有 private
单用户 cpuset 委派，不启用父 controller、不改 Slurm 父级。配置仅限新子 scope，
请求/有效 CPU 和 NUMA 集合都核对；恢复只观察原 inode，不能重建、改配或再启动。
有后代/直属进程、身份替换、内核上下文变化或清理未知均保留，不由空 scope 补造
wait；同 UID 委派不当作恶意程序沙箱。16 条纯模型检查通过；指定计算租约内
完整 native 回归运行 896 条，892 通过、4 跳过（其中 2 条正向 scope 测试因无委派
明确跳过，不计 kernel 验收成功）。runtime/fixture 169 文件 SHA256 为
`5a90c3ba2f4b1f87ab373e3ff08562c496f959312a2b2e01e10e2bca2d37dd65`，
本地与计算节点一致。同一最终来源的 CPU 亲和 CLI 六组全部通过：普通/fake-GPU/
native mask、释放/取消、原 owner 崩溃重连和冷配置/默认 off 没有回归，未创建实际
cgroup 或动生产。scheduler 持久 intent/claim/资源事件、scope 漂移和取消/
timeout/失效租约恢复接入仍待实现，设备与授权生产切换也未完成。固定提交
`0aa7dc856fcee862744ccda38989b7e394ddd023` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37879967943) 14 项均通过；
未发布或部署。

后续候选新增 schema 19 的 scope 持久记录层，绑定原 allocation 全文摘要、
instance/job/version/lease/CPU claim、唯一创建 intent 和原 inode。所有外部效果先
消耗独立 CAS intent；unknown 不按名字猜 inode，不重建或重复 start，也不借任务
终态/retry 释放原 CPU。原 scope removed、或尚未创建的 reserved abandoned 才放行
原 CPU release；cold off 不能绕过旧未决 scope。有 scope 的 launch_constraints
拒绝仅 affinity 降级。独立 cpu-scopes CLI 只查询私有快照，不探测或授予 wait。
19 条纯事务模型通过；指定计算租约内最终来源完整 native 回归运行 915 条，911
通过、4 跳过，其中正向 scope 两条因无委派明确跳过。171 文件 runtime/fixture
SHA256 为 `d0717d398d9f7fe9b8dec2570e40767729b331b3df92014bc3cfc633fa4a0bef`，
本地与计算节点一致。同一最终来源 CPU 亲和 CLI 六组全部通过，涵盖普通/fake-GPU/
native mask、取消/释放、原 owner 崩溃重连与冷配置/默认 off，生产 daemon/config
未改。此记录层不自动启用 cgroup，没有实际 scope mkdir/join/恢复/设备验收，
不能当作完整硬隔离。固定来源 `6d17aab65e5c4b428cc2c6b11e3fe21aa92d8e7d` 的
[完整 CI](https://github.com/Gczmy/sched/actions/runs/37881749346) 14 项均通过；未发布或部署。

后续 schema 20 源码候选接通显式 cgroup/delegated_root、原 lease/parent/inode
验证与创建/配置/启动/清理的独立提交边界；不自动准备父委派。execution 已清理
与原 scope 含后代为空分开，cleanup_ready 后仅在 writer 外删除原目录，真实
removed 落库后才释放。普通 wait 在同 daemon 内按原 allocation 保留跨 tick；
丢失原 wait 不从 sidecar/产物补判成功。原 owner 亦保留至清理/结算完成，缺少
原 cgroup intent 不允许 affinity 降级或释放。18 条 controller 模型、19 条记录模型
与 15 条亲和纯决策/事务回归通过，模型不计 kernel 验收。
指定计算租约内最终固定来源完整 native 回归运行 936 条：931 通过、5 跳过；
其中两条底层 scope 和一条 scheduler 正向项均因缺少明确委派跳过。174 文件
runtime/fixture 清单 SHA256 为
`c2401eaaaa2713cdc0b96d41abf4a8fde1bba7e6bf810d1c198a1cedb4058c13`，
本地与计算节点核对一致；同来源普通目录 cgroup 委派拒绝 CLI 通过，无 daemon/
allocation/CPU claim/worker，无父级写入。兼容 CPU 亲和 CLI 六组在此前固定来源
`1f9d7baa4241dd5801c18dab933180784c7aab6432b59ff2e9395cd6d81dd14e` 全部通过，
之后仅补缺失 cgroup intent 的拒绝守卫并取得最终全量/拒绝路径证据。
未使用真实 GPU、修改生产 daemon/config 或 Slurm 父级；尚未取得正向 cpuset/
故障恢复/设备证据，未发布或部署。合同与授权正向脚本见 [CPU scope](cpu-scopes.md)。

| 阶段 | 工作 | 当前状态 | 必须取得的完成依据 |
| --- | --- | --- | --- |
| 2 | 已提交修复验收 | 候选 CI/CPU/两主机验收通过 | 固定来源完整 CI/独立安装；计算节点隔离验收；原 RID 跨网关恢复不重复投递 |
| 3 | opt-in 独立失败策略 | 候选 CI/计算节点 CPU 通过 | 默认冻结兼容、独立任务继续、running/未知启动不变、CAS/schema 迁移 |
| 4 | 不可变 artifact validation | 候选 CI/计算节点普通 CPU 通过 | 精确 job/version、规则/产物/wait/cleanup 绑定，失败历史不可覆盖 |
| 5 | 仅复验及重新结算 | 候选 CI/计算节点普通 CPU 通过 | 不训练/不删文件、幂等/CAS、原执行权威与失效证据拒绝、有限系统错误退避 |
| 6 | 精确依赖 | 候选 CI/计算节点 CPU 通过 | 冻结 instance/batch/task/version；名称显式兼容；同名或 resubmit 不漂移 |
| 7 | 任务 DAG/阻塞路径 | 候选 CI/计算节点 CPU 通过 | 事务内环检查；A→C、B→D 且 A 失败时 B/D 继续、C 等待；CAS 依赖更新保留不可变历史 |
| 8 | bounded 精确事实/成组 pending-only cancel | 候选 CI/计算节点 CPU 通过 | 单次源批次 CAS；核对所有代际启动记录；中断/竞争/同 RID 恢复 |
| 9 | admission explain/装箱解释 | 候选 CI/计算节点 CPU 通过 | 复用真实判断，全部资源/原因/观测时效，预约不冒充硬限制 |
| 10 | allocation 身份/分层失败 | 候选 CI/计算节点 CPU 通过 | 不可变分配关联；原始退出/监控声明/产物/资源分层；owner 不冒充 worker |
| 11 | 磁盘/inode/quota 准入 | 候选 CI/计算节点 CPU 通过 | 控制面余量；unknown 明确；容量不足不删科学产物 |
| 12 | 独立发布与生产切换 | 未执行 | 最终提交 CI/原始包/hash/迁移与回退；获授权的计算节点排空/安装/验收 |
| ND-02 | 租约来源持久化/持续验证 | 候选 CI/计算节点 CPU 通过 | Slurm/cgroup 白名单来源；失效停新派发、不杀 running、不迁移；unknown 与退出追溯 |
| ND-03 | CPU auto 容量 | 候选 CI/计算节点 CPU 通过 | Slurm/affinity 保守边界；0 兼容；固定超额提示/拒绝；持续租约验证 |
| 硬隔离 | per-job affinity/cgroup | 原语、亲和/CPU claim、持久 scope 与 schema 20 显式 cgroup 派发/清理源码候选；正向内核/设备验收未完成 | 明确启用和可用性；真实 CPU/设备边界；退出/取消/清理与恢复兼容 |

阶段 3 的候选合同见 [reference](reference.md)；阶段 4–11 具体约束见 [ND-04](next-development.md)，ND-02/03 与硬隔离也在
该文件记录。不能把本清单中的设计当作已可用 CLI/config。
客户端负责替代 spec/RID 冻结、reservation、lineage 与跨系统恢复，
不把客户台账或科学 gate 引入 daemon；单实例 pending-replace 依需求评估。

每阶段单独核对配套插件的严格 JSON/等待原因/CAS 合同；该仓库的功能改动
独立安排，不混入 sched 提交。阶段可分别发布，但只有实际证据齐全才记完成。
实机测试须先确认用户指定的既有计算节点租约；生产切换另行授权，
流程与 schema 回退边界见 [部署清单](execution-rollout.md)。
