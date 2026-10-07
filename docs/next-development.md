# sched 开发范围与后续工作

本文只记录通用调度器工作。配置和 CLI 以 [reference.md](reference.md) 为准，
execution 以 [execution-api.md](execution-api.md) 及其同仓验收为准。
候选代码、验证、正式发布和生产部署分别记录，不能互相替代。

## 历史版本 0.2.1

[GitHub Release](https://github.com/Gczmy/sched/releases/tag/v0.2.1) 保存正式来源、
源码、默认/native wheel、安装说明、哈希与 CI 证据。运行时无第三方依赖；
默认安装要求 Python >= 3.10，原 native 包覆盖 CPython 3.10/3.14、
Linux x86_64、glibc 2.39。

该版本包含通用 FD 执行与原始 wait、持久 owner、认证重连、有限确认队列、
冷配置保留期、execution 详情/列表、本机 capabilities 和结构化 daemon 检查。
写库 schema 7，只读 schema 1–7；未知尝试和旧 session 不重放。
职责见 [execution-boundary.md](execution-boundary.md)。生产现状未查询，
不能把发布记录当作生产已升级的证据。

## 已发布 0.2.2 历史记录

[v0.2.2 Release](https://github.com/Gczmy/sched/releases/tag/v0.2.2) 于 2026-10-01 发布，
来源为 `8aa2559dc64e74acd2cec6bcb2f5481d1d9fbc1d`。
[最终来源 CI](https://github.com/Gczmy/sched/actions/runs/36881160410) 的 14 项检查通过；
四份原始 ZIP 和三个说明/证据/校验和文件已从公开 Release 下载核对。
native 资产覆盖 CPython 3.10/3.14 × glibc 2.35/2.39 / Linux x86_64。

- 按 backend ID 预检 executable 和项目 root：有界 SHA-256、ELF 标识、
  读取漂移、文件类型与目录权限。错误码不输出私有路径，不授予后续执行权。
- 默认安装和回归覆盖 Python 3.10–3.14；native/真实执行与候选构建覆盖
  CPython 3.10/3.14、Ubuntu 22.04/24.04 / Linux x86_64。
  真实 syscall 拒绝用例验证不可用能力不会标为 verified，不弱化执行方式。
- 同仓脚本和手动 workflow 从最终 main 的成功 CI 校验四份原始 ZIP，
  生成总校验和、说明与 evidence，可续传匹配的草稿资产，不自动发布。

见 [0.2.2 Release 说明](releases/0.2.2.md) 与
[发布准备](release-preparation.md)。验收以独立选定的完整提交、最终 CI run
和实际产物为准；只有通过的矩阵才构成支持证据。正式发布不代表生产已切换。
写库仍为 schema 7，状态、FD4 identity、原始 wait 和未知结果守卫保持不变。

## 已发布 0.3.1 与 0.4.0

0.3.1 的 SQLite 锁修复与版本查询见 [版本说明](releases/0.3.1.md)。
0.3.0 发布准备章节保留为历史；它不能代表当前生产状态。
0.4.0 的实例身份、任务归属、结构化请求查询、网关幂等投递及两侧客户端适配已实现并发布。
兼容边界见 [集成合同](integration-contract.md)，固定来源与完整 CI 见 [0.4.0 说明](releases/0.4.0.md)。
部署按机器实际任务、启动标记和健康状态分批进行；不能把已发布当作全部生产节点已升级。
查询缓存、事件流及额外诊断需有实际测量依据，不列为已实现功能。

## 后续候选

- 更多 Linux ABI/架构须先有对应构建、探测、独立安装和实际执行验收，
  再扩大兼容范围；不把当前 x86_64 证据用于未验收环境。
- 配套客户端的 execution 展示归其仓库独立安排，继续消费公共 CLI，
  不重写 scheduler 语义或将实时多页浏览当作完整当前态。

维护与回退见 [execution-rollout.md](execution-rollout.md)。客户协议、
科学验收和研究部署由客户仓库独立维护，不作为 sched 发布前置条件。
已有 GPU 策略、资源准入、drain/resume 和幂等维护请求继续独立维护。

## 0.3.0 历史发布准备

该版本协议见 [recovery-policy.md](recovery-policy.md)。候选已实现独立新尝试授权与持久
OOM FIFO、固定 12 GiB 默认剩余显存准入、分级和持久无进展策略，以及前台 daemon/supervisor 和完整本地故障验收。
四组 PR 已审查合入 main，合并提交的 14 项 CI 全部通过。当时包版本进入 0.3.0 发布准备，
隔离真实 CUDA 验收已通过全部 8 项。正式来源、资产与发布状态见
[v0.3.0 Release](https://github.com/Gczmy/sched/releases/tag/v0.3.0) 及其 evidence；
该段只保留历史准备过程；生产现状需重新查询。
验收矩阵见 [recovery-acceptance.md](recovery-acceptance.md)。

## ND-02：持久化并校验 daemon 的集群租约来源

**状态：待设计、未实现。** 当前 daemon lease 只用于确认进程身份，没有持久记录
Slurm job、step、cgroup、cpuset 或启动 shell 的资源上下文，`daemon status` 也不展示
这些信息。

### 匿名化历史场景

daemon 从既有 Slurm 租约 shell 启动；随后用于进入租约的 screen 被删除、旧租约
关闭，但脱离 TTY 的 daemon 在该环境下继续运行。后来同节点创建了新租约，
旧 daemon 不会自动感知或绑定新租约，直至按 idle timeout 自动退出。进程退出后，现有
sidecar 与日志不足以百分之百还原它当时属于哪个 Slurm job/cgroup。

这说明“启动时继承 allocation/cgroup”与“租约生命周期持续有效”是两个独立条件；
`start_new_session=True` 本身既不会申请新租约，也不会完成租约迁移或有效性验证。

### 目标行为

1. daemon 发布 lease 时原子持久化最小、白名单化的启动来源：`started_at`、
   `physical_host`、PID/start token、`SLURM_JOB_ID`、`SLURM_STEP_ID`、Slurm 声明的 CPU
   数量、`sched_getaffinity(0)`/`Cpus_allowed_list` 结果及 cgroup 标识。禁止保存完整环境，
   避免把令牌或其他秘密写入 state。
2. 扩展现有 `sched daemon status --json`，兼容已有身份与健康字段，展示持久化来源、当前可见来源和
   `allocation_state=valid|invalid|unknown`；daemon 已退出后仍保留最后一次启动与退出
   上下文供审计，而不是只剩无法归属的历史日志。现有 lease 已记录 `physical_host`、
   PID/start token 与 lease ID，缺口是 Slurm/资源上下文及退出后的来源保留。
3. daemon 周期性验证已记录 Slurm job 是否仍为目标节点上的 RUNNING allocation，并
   检查自身 affinity/cgroup 是否与启动快照一致。`scontrol` 不可用或集群未启用可靠
   cgroup 时必须报告 `unknown`，不能伪装成 valid。
4. 已确认租约失效时停止派发新任务、留下可诊断事件并通知；默认不暗中迁移到后来创建
   的租约，也不直接杀死已经运行的任务。恢复必须在目标新租约内显式 stop/start。
5. 增加旧租约关闭但 daemon 存活、新租约同节点建立、Slurm 查询失败、PID 复用、daemon
   正常退出后追溯，以及状态 JSON 兼容性的验收测试。

## ND-03：CPU 容量自动解析与可执行约束

**状态：待设计、未实现。** 当前 `cpus_total=0` 表示关闭总 CPU 配额，只在 CPU-only
任务上回退到 `max_cpu_jobs` 并发计数；正整数仅做声明值求和，不会设置 affinity 或
子 cgroup。

### 已确认的设计方向

1. 显式增加 `cpus_total: "auto"`（最终字段形式实现前定案），保留
   `cpus_total=0` 当前“不启用总 CPU 配额”的兼容语义；不得把现有零值静默改成 auto。
2. auto 模式综合 Slurm 声明、`sched_getaffinity(0)` 与可选配置上限解析有效 CPU 容量。
   多个可信来源不一致时取保守边界并明确告警，无法确认安全容量时 fail-closed 或报告
   unavailable，不能静默选择更大的值。
3. 与 ND-02 联动，daemon 持续验证启动时记录的 Slurm job 是否仍是目标节点上的有效
   RUNNING allocation，并复核自身 affinity/cgroup；启动时解析一次容量不足以覆盖租约
   后续被删除而 daemon 继续存活的场景。
4. 旧租约确认失效时停止派发新任务、记录事件并通知，不自动迁移或绑定同节点后来创建
   的新租约。已经运行的任务默认不被暗中杀死；恢复要求在目标新租约内显式 stop/start。
5. per-job affinity/cgroup 作为独立的硬隔离能力后续实现。在它完成前，
   `resources.cpus` 与解析后的 `cpus_total` 仍只是 admission control/调度记账，不能描述
   成对单个任务的物理 CPU 限制。

### 可观测性与验收

- `status --json` 同时展示配置值、解析后容量、容量来源与租约有效性；固定正整数仍可
  用于明确部署，但若大于当前 affinity/Slurm 容量必须告警或拒绝派发。
- 覆盖 auto、固定值、零值兼容、Slurm/affinity 一致与冲突、租约运行中失效、新租约
  同节点建立但禁止自动迁移，以及未启用硬隔离时的机器可读语义测试。
