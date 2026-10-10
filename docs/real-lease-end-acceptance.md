# 真实租约结束验收

此流程补充[学生账号兼容路线](student-compatibility.md)的剩余验收，
不把控制器夹具的 CANCELLED 当作真实结束。只使用用户指定的非生产既有租约、
独立配置/state、低并发 CPU 与 fake GPU；不申请租约，不结束生产租约。
需要结束测试租约时须另有明确授权；否则等待其自然到期。

## 执行前冻结

在指定原租约 shell 核对 hostname、SLURM_JOB_ID 和真实 scontrol job/step 信息。
独立安装须匹配解释器/native ABI，并通过版本、实例和计算节点检查。
通过 CLI 确认私有 daemon 的原租约 valid、启动祖先已核验、CPU affinity claim 可用，
仍报告 hard_isolation=false。测试 state 和输出目录须独立，不能复用生产配置。

准备两个只追加一次计数文件的 CPU 任务：一个先启动并等待测试目录内的释放信号，
另一个保持未启动。通过 CLI drain 保持第二个 pending，保存其完整 batch/task/version、
原 spec、revision、allocation/attempt 事实和计数文件证据。释放第一个后，确认真实
wait/cleanup、终态和 claim 释放；这一步须在原租约尚有效时完成。

随后仅对私有 daemon resume，在第二个任务的资源或依赖仍明确不可满足时继续观察；
不能仅靠 drain 证明失效守护有效。保存独立 admission-explain 的等待原因。
真实结束后若原 daemon 仍存活，通过网关正常 submit 投递一个 CPU 可派发探针，
绑定原私有实例与唯一 request-id；投递与入库分别核对，同一未知请求不换 ID 重发。
该探针只验证原 lease 的 invalid/unknown 门禁；不得触发计算节点自动启动入口。

## 原租约结束后

由获授权的结束动作或自然到期形成真实控制器终态。只读观察可在网关执行；
任务实际执行和 daemon 管理仍在计算节点。保存真实控制器结果、原 anchor/stepd 是否仍可读、
精确 lease 事件及 daemon 健康，不凭环境变量、PID 或产物推断执行归属/退出。

若 daemon 仍存活，要求原来源 invalid 锁存或 unknown 暂停，新派发探针没有 allocation、
attempt 或执行计数。若控制器随后不可读，只能记录 unknown，不能补判租约 valid。
同节点后来出现的新租约不得替换原出生身份或自动解除 invalid。

若 Slurm 在结束时清理了测试 daemon/worker，记录这一实际结果。daemon 被清理不能
证明“存活 daemon 在失效后持续暂停”；该覆盖项保留未完成。不能为了补齐矩阵
绕过集群清理、将测试搬到网关执行或修改父 cgroup。运行中任务被 Slurm 清理时，
不得把缺失的原 wait 补判成成功，资源与历史未知事实仍按现有合同保留。

## 新租约中的显式恢复

另由用户指定有效既有租约，在同一私有实例中核对 hostname/config.node 与真实 job/step。
确认旧 daemon 已停止或完成受控排空，再显式 check/resume/start。保存新的出生身份；
旧 lease 原始事实不得变化。第二个任务的约束解除后与已入库探针各执行一次，历史未知尝试不得重放。
收尾通过 CLI 无损排空私有 daemon，核对 running、未决启动及活动 claim 清空，
保留受管 state、日志和验收证据。

验收结果逐项记录：真实原租约终态、存活守护、新租约不自动接管、原 wait/未知保留、
显式恢复及不重复执行。缺少执行条件的项报告未完成，不用已有夹具结果替代。
