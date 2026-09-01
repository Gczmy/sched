# sched + dsh-node-sched 联合代码复审报告（2026-08-29）

> **文档定位（2026-09-01 更新）**：本文是带时点的历史审计与修复证据，不是当前
> CLI/config 权威参考。文中的“当前”、测试计数、未验证边界、源代码行号和运行环境
> 残留均只描述各节标注的 2026-08-29/30 复审时点；请勿据此覆盖
> [`reference.md`](reference.md) 的现行契约。尚未实现的工作只记录在
> [`next-development.md`](next-development.md)。

## 后续版本补记（代码基准 `e9a44d0`，2026-09-01）

- `1fda6cd` 将计算节点本机和异机的数据库查询统一为稳定私有 DB/WAL 快照上的
  `mode=ro + query_only`，并在源变化、锁争用、非 WAL 或未来 schema 时有界
  fail-closed；因此下文 S-H04 状态行里“本机可读取 live WAL”的描述只代表旧复验时点。
- `1fda6cd` 还为“compute 列表完整为空但 utilization>0”的空闲卡底噪增加连续 3 次
  确认；确认期间仍显示 `free`，但从全部派发路径排除。compute PID 或不确定探测仍
  首次采样立即 `unmanaged`，连续 2 次干净采样自动恢复。
- `e9a44d0` 修复顶层 argparse 路由字段与 `sched run` 的 positional `cmd` 碰撞，
  真实 `main()` 入口、dry-run、payload 保留和异机写保护均有回归覆盖。
- 后续真机复验已覆盖真实外部/managed CUDA 占用与释放、GPU 底噪防抖及自动恢复，
  并在隔离 NFS state 上完成 96 个 CPU-only job 与 6 个并发查询者的压力测试；生产
  checkout 已快进并运行到 `e9a44d0`。因此下文“没有连接真实 GPU、真实远端 SSH 或
  生产部署”的边界仍是原报告的历史结论，不代表上述代码基准的最终验证状态。
- 当前精确行为、网关/计算节点边界、项目 quota/priority 语义和命令清单统一见
  [`reference.md`](reference.md)；下文保留原样用于追溯，不再作为开放项列表。

## 修复状态更新（最终复验：2026-08-30）

本节记录审查之后的修复结果。下文的原始结论、40 项发现、证据和放行建议均保留为审查时点的历史记录，不应再解读为当前开放项；本节所列 40 个 ID 以及最终独立复审追加项均已解决。

证据类别：**P** = sched 新增 Python 定向测试；**A** = sched 既有验收回归；**N** = dsh Node 后端测试；**U** = dsh UI 契约测试及直接 bundle 构建；**C** = 隔离状态目录中的真实 sched CLI JSON smoke；**B** = 本地真实 dsh 浏览器 smoke。每一行的类别表示该修复至少由哪些层次覆盖。

### sched 报告项

| ID | 状态 | 具体修复 | 证据类别 |
|---|---|---|---|
| S-H01 | 已解决 | 每个 stage 在命令成功后先校验其声明产物，再向 mode 0700 目录原子提交 mode 0600 指纹 sidecar；stage skip、ready probe 和最终结算都会校验 task 与全部 stage 产物，`force_rerun` 禁止复用。 | P、A |
| S-H02 | 已解决 | Git 指纹只哈希 tracked 文件的 staged/unstaged 内容并排除 untracked 文件；Git 探测禁用 fsmonitor、external diff 与 textconv，限制时间/输出，自动模式遇到损坏仓库时 fail closed。 | P |
| S-H03 | 已解决 | resubmit 会把终态批次重开为 `active`、清除旧终态 marker；新版本失败后可收敛为 `blocked` 并通知。 | P、A |
| S-H04 | 已解决 | 异机查询与所有 dry-run 预览都从私有稳定 DB+WAL 副本打开；没有 WAL 时同样先复制，不再对 live SQLite 使用 `immutable=1`，也不初始化、迁移或写共享状态。计算节点本机查询仍可读取 live WAL。 | P、C |
| S-H05 | 已解决 | `daemon start/stop/check` 默认只允许配置的计算节点执行，只有显式 override 可越过；`daemon status` 保持只读。 | P、A |
| S-H06 | 已解决 | 项目 GPU 配额在同一 tick 内按成功 launch 实时扣减；skip/launch 失败不占配额，CPU-only 任务不受 GPU 配额阻挡。 | P |
| S-H07 | 已解决 | 单个无法装卡的队首任务不再把后续 GPU 任务整轮短路，后续可装入任务仍会尝试派发。 | P |
| S-H08 | 已解决 | releasing 卡发现外部 PID 时直接保持不可分配并转为 `unmanaged`，不再出现可重分配窗口。 | P |
| S-H09 | 已解决 | 启动锁使用 `lease_id`、PID 和进程启动令牌；陈旧回收在跨进程 guard 内精确复核 owner，PID 复用无需发信号即可回收，活进程或替换租约不会被清理。 | P |
| S-H10 | 已解决 | schema 预编译 artifact regex；运行时只通过 descriptor 读取普通文件、拒绝 symlink/特殊文件、限制内容为 1 MiB，并把 regex 匹配隔离到 1 秒子进程。 | P、A |
| S-H11 | 已解决 | stop 最多等待 120 秒完成合法慢 tick，超时不 SIGKILL；shutdown marker 带调用者 token，信号发送失败只清自己的 marker，同时保留 daemon PID/heartbeat ownership。 | P |
| S-H12 | 已解决 | 每次 PID 归属都原子重探完整 UUID→index 拓扑；查询失败、未知 UUID 或拓扑重排均返回“不确定”，不沿用永久空映射或跨重排误归属。 | P |
| S-M01 | 已解决 | dry-run JSON stdout 只含一个 JSON 文档；SKIP 预测纳入 producer fingerprint 与 `force_rerun`。 | P、C |
| S-M02 | 已解决 | `history --project` 正确加载项目配置；合法空结果和未知项目分别返回稳定结果与明确错误。 | P、A |
| S-M03 | 已解决 | resubmit 与 ready-job launch 都在 SQLite DML 前只计算一次 Git 指纹，并把同一快照复用于 skip、过期产物清理和 launch 元数据。 | P |
| S-M04 | 已解决 | `config.state_dir` 成为实际运行时数据根目录；相对值锚定 config 文件目录，bootstrap config 路径保持稳定，`SCHED_STATE` 仍是显式最高优先级 override；`node` 必须是安全单路径分量。 | P、A、C |
| S-M05 | 已解决 | batch、task 与 dependency 标识符在 schema 层限制为安全、有限长度的路径分量。 | P |
| S-M06 | 已解决 | task `cmd` token、`env` 名和值、`duration`、`paths_escape` 及数值资源字段增加严格类型/有限值校验；拒绝 NUL、shell bootstrap 和 LD_/DYLD_ loader 注入变量。 | P |
| S-M07 | 已解决 | status 在一个 SQLite 视图中先有界选择批次，再只返回这些批次的各 task 最新版本；输出 canonical `status`、独立 `wait_reason`、`batch_id`/`batch_name`，并分别报告 batch/job 截断。 | P、N、C |
| S-M08 | 已解决 | daemon 拒绝、失败 check 和异常返回非零；“已运行/未运行”等幂等状态仍返回成功。 | P、A |
| S-L01 | 已解决 | schema 不再接受没有运行时消费者的 `batches.gpus` 与 `retry_transform` 字段。 | P |

### 跨仓库契约项

| ID | 状态 | 具体修复 | 证据类别 |
|---|---|---|---|
| X-H01 | 已解决 | 看板确认 cancel 后固定向 sched 传递必需的 `--yes`。 | N |
| X-H02 | 已解决 | status、task/log/diag/retry/resubmit/cancel 和 UI 全部以 exact `batch_id` 构造复合引用；不再读取 legacy `jobs[].batch` 或按重名 batch name 回退，batch cancel 传递选中记录的精确 ID。 | P、N、U、C |
| X-H03 | 已解决 | sched 提供真实的 history JSON envelope：`schema_version=1`，limit 默认 50、钳制到 1..200，并独立报告 `truncated`；dsh 改为消费该协议。 | P、N、C |
| X-M01 | 已解决 | HTTP submit/op 路由读取统一 operation envelope 的 `text`，成功输出和 CLI 拒绝原因均不再丢失。 | N |
| X-M02 | 已解决 | producer/consumer 统一使用 canonical status，等待原因只从 `wait_reason` 读取，live/terminal 汇总不再解析装饰字符串。 | P、N |
| X-M03 | 已解决 | dsh 写路径绑定唯一 writer；每次 mutation 都在同一 transport 上重新取得 hostname 与 `sched config get`，按大小写/末尾点规范化后精确匹配 expected node；破坏性 GPU preflight 在该已认证 writer 上、同一 single-flight 内紧邻 mutation 执行。 | N |

### dsh 后端报告项

| ID | 状态 | 具体修复 | 证据类别 |
|---|---|---|---|
| D-M01 | 已解决 | 只读敏感 GET 保持 loopback/Host/Origin 围栏；所有 mutation（含 SSH host、auth answer 与 client log）统一为 POST，并要求 present、协议/主机/端口完全一致的 same-origin `Origin`。 | N |
| D-M02 | 已解决 | auth patch 逐字段合并，省略字段保持原值、显式 null 只清目标字段；切换 auth kind 时清除不兼容凭据。 | N |
| D-M03 | 已解决 | 通用 SSH exec 只建立并执行一次 channel；结果未知时不重连、不重放，也不回退到第二种 transport。 | N |
| D-M04 | 已解决 | CLI 上传/标准输入 runner 增加总 deadline、取消传播和原始字节输出上限，超限或超时会终止子进程；远端 inbox 写入使用 `umask 077`、目录 0700 和文件 0600。 | N |
| D-M05 | 已解决 | status consumer 请求有界 canonical 文档；成功缓存严格按 TTL 失效并保留可观测 stale metadata，不把过期 body 当新鲜成功；GPU mutation 强制查询已认证 writer 的 fresh status。 | P、N、C |
| D-M06 | 已解决 | SSH connect、私钥口令与 keyboard-interactive 共用从连接开始计算的单一 180 秒绝对 deadline 和 AbortSignal；隐藏看板不算交互 audience。 | N、U |
| D-M07 | 已解决 | challenge 在短暂断连后保留并可重放；answer/cancel/expiry 返回可区分 outcome，清理 pending/timer/listener，并按 challenge ID 仅广播一次 resolved/expired/cancelled 终态。 | N、U |
| D-M08 | 已解决 | 零提示 challenge 只有显式 `{state:"answered", answers:[]}` 才调用 ssh2 `finish([])`；取消或过期会终止 SSH，不能伪装成空回答成功。 | N、U |
| D-L01 | 已解决 | 三种 transport 共用按原始 UTF-8 字节计数的 limiter，`droppedBytes` 始终初始化为数值且不会切断字符边界。 | N |
| D-L02 | 已解决 | entry override 通过 mode 0600、`O_EXCL|O_NOFOLLOW` 临时文件原子 rename；发布前先以 `O_DIRECTORY|O_NOFOLLOW` 打开父目录，目录提交失败时原子恢复并 fsync 旧 generation，且只清理由本进程成功创建的临时路径。远端 inbox/screen 临时目录和文件同样按 0700/0600 收紧。 | N |

### dsh UI 报告项

| ID | 状态 | 具体修复 | 证据类别 |
|---|---|---|---|
| UI-L01 | 已解决 | auth answer 的服务端拒绝、HTTP 错误和网络错误均显示为有长度上限的用户可见消息。 | U |
| UI-L02 | 已解决 | captured sidebar listener 的 dispose 使用与注册完全相同的 `capture=true`。 | U |
| UI-L03 | 已解决 | submit 示例为合法 JSON，使用已加载配置中的 project，命令固定为可直接执行的字符串数组 `["echo", "hello from sched"]`，不再展示未定义字段或 profile placeholder。 | U |

### 最终独立复审追加项

- sched：Darwin 使用 libproc 微秒级启动令牌；只有 supervisor 正常退出才证明原进程组已排空，被信号终止时保持 fail-closed。相同 PGID 被复用、成功 SIGKILL 后的旧组、pgid 尚未回写且 launch marker 未决等路径均有独立回归；adopt、stop 和 reap 不再提前释放任务或 GPU。
- sched：daemon lease owner、launch marker 与 RC sidecar 均通过 `O_NONBLOCK|O_NOFOLLOW` descriptor 做普通文件、owner、link count、身份和硬字节上限复核；FIFO、并发增长和替换文件不能阻塞或绕过上限。profile 选择按 project 命名空间隔离，daemon/CLI 物理主机身份与测试 override 分离。
- dsh：终端 WebSocket 先发送 `ready` 再安装会同步重放早期输出的 handler，初始 prompt/banner 不会被 UI 随后的清屏擦除。SSH stream 的早期 data/terminal event、pending acquire abort、challenge/prompt/UTF-8 总量和全局并发上限均有回归；CLI SSH runner 在 leader 已退出但 stdio 未 close 时继续保留总 deadline，超时或 abort 会有界收敛且不再对已退出 leader 的 PGID 升级信号。
- dsh：HostStore 与 entry override 在 rename 前持有已验证父目录；parent fsync 失败会用新的 0600、`O_EXCL|O_NOFOLLOW` 临时 generation 恢复旧内容、fsync 文件、rename 并再次 fsync 目录。entry override 会精确保留并恢复旧文件的原始字节或“不存在”状态，malformed/non-object operator 内容不会在失败 mutation 中变成 `{}`。HostStore 同时兼容既有缺失或为空的 password 记录，失败写入不发布内存状态或重启状态。
- sched 验收清理：任一 claimed root 的身份、daemon stop 或 quiescence 证明失败时，本轮所有 state/project/output root 均 fail-closed 保留；不会在 daemon 仍可能使用辅助目录时单独删除无 `config.json` 的 project/output root。

### 聚合证据与边界

- sched Python 全量单元/契约测试：**220/220**。
- sched 验收：修改完成后的独立全量重跑 **51/51**；其中 cancel pending 与 UX 分别另行定向复验 **12/12**、**22/22**。全部 acceptance/helper shell 脚本通过 `bash -n`。
- dsh Node 后端测试：**153/153**。
- dsh UI 契约测试：**19/19**；client bundle 直接构建通过，`index.js`、`ssh-engine.js`、`entry-override.js` 通过 Node 语法检查。
- 真实表面 smoke：隔离目录中的实际 sched CLI 完成 daemon start → dry-run JSON → submit → status `done` → stop，并确认相对 `state_dir`、canonical `batch_id` 与产物。本地真实 `nodesched` profile 启动成功；实际浏览器页面标题为 `DeepSeek Harness`，看板显示 `○ offline` 与“调度器状态不是最新快照，操作已切换为只读”，History 显示 0 行与本机 token 错误，SSH 显示 0 台主机，Submit 的 project/payload、dry-run 和确认提交均有效 disabled。只执行导航和检查，没有触发 mutation。
- 最终浏览器证据：看板 `/var/folders/qx/ppkvfc_x2yb2d2mh38b4wxkm0000gn/T/omp-sshots-156b83ec204a4033.webp`；History `/var/folders/qx/ppkvfc_x2yb2d2mh38b4wxkm0000gn/T/omp-sshots-156b83f4e04a4034.webp`；SSH `/var/folders/qx/ppkvfc_x2yb2d2mh38b4wxkm0000gn/T/omp-sshots-156b8413134a4035.webp`；Submit `/var/folders/qx/ppkvfc_x2yb2d2mh38b4wxkm0000gn/T/omp-sshots-156b841975ca4036.webp`。
- 最终独立复审：sched 与 dsh 的 reviewer 均确认追加 finding 已关闭、未发现剩余 blocker/high/medium；强制 `code-reviewer` lane 另行调用，但基础服务返回 `401 User not found`，未把失败调用误报为审批证据。
- 运行环境残留：早期、修复前的验收进程仍有 **15** 个旧 daemon；最终 51 项重跑未新增残留。对其中一个已验证测试 root 执行唯一允许的 `sched daemon stop` 后等待 120 秒仍超时，CLI 按设计拒绝强杀并保留 ownership。严格遵守 CLI-only 契约，本次没有直接发信号或编辑 state；清除此历史残留需要新增受支持的 force-stop 运维命令或由操作者另行处理。
- **硬件/部署边界**：动态证据覆盖 fake-GPU、隔离 CLI 和本地真实浏览器；没有连接真实 GPU、真实远端 SSH 或生产部署，因此不据此声称真实硬件或生产环境已验证。

## 原始结论（审查时点；已由上方修复状态更新取代）

**当前代码不能判定为正确，也不建议按“已可安全投入长期运行”放行。**

本次在当前工作区确认 **40 项开放问题**：

| 严重性 | 数量 | 判定 |
|---|---:|---|
| Critical | 0 | 未发现可直接跨权限接管宿主的当前证据 |
| High | 15 | 会造成过期产物复用、单写者边界破坏、资源冲突、虚假终态、daemon 退出，或公开集成路径确定性失效 |
| Medium | 19 | 条件性错误、协议漂移、配置失真、认证生命周期问题和可观测性缺失 |
| Low | 6 | 边界输出、持久化耐久性、UI 清理和示例质量问题 |

最需要先处理的五类风险：

1. **产物正确性**：stage checkpoint 和 dirty-tree 指纹都可能复用由旧代码生成的结果。
2. **单写者与 daemon 生命周期**：登录节点“只读”命令仍写 WAL；`daemon start/stop` 未受异机保护；dsh 的写目标没有稳定绑定到配置中的计算节点。
3. **GPU 调度安全与活性**：同 tick 项目配额可超卖；大任务可永久饿死小任务；刚释放但仍被外部进程占用的卡可在确认窗口内被重新分配；UUID 探测瞬时失败会被永久缓存为“无进程”。
4. **状态真实性**：done 批次 resubmit 后仍保持 done，后续失败不收敛、不通知，marker/API 仍呈现旧成功状态。
5. **跨仓库公开契约**：看板 cancel、失败任务 log/retry/resubmit、模型工具 history 当前均存在确定性断裂。

51 个 sched 验收脚本和 43 个 dsh Node 测试全部通过，但这些测试没有覆盖上述触发条件；“现有测试全绿”不能抵消本报告的代码路径和定向复现实证。

---

## 1. 审查对象、快照与约束

### 1.1 代码快照

- `sched`
  - 路径：`/Users/zzc/quant_trade/sched`
  - 分支：`main`
  - HEAD：`b7c6ffb9aa1312cdae51fe5400b3b40742f05b39`
  - 审查开始时已有未跟踪文件：`docs/audit_2026-08-28.md`
- `dsh-node-sched`
  - 路径：`/Users/zzc/quant_trade/dsh-node-sched`
  - 分支：`main`
  - HEAD：`01617b9468155bc3546d07c45fe152be6dfded26`
  - 审查的是包含未提交修改的实际工作区；开始时修改文件为：
    - `packages/node-sched-ui/src/client.jsx`
    - `packages/node-sched-ui/lib/client.js`
    - `packages/node-sched-ui/lib/client.js.map`
    - `packages/node-sched/lib/index.js`
    - `packages/node-sched/lib/ssh-engine.js`
    - `packages/node-sched/test/ssh-error.test.js`

因此，本报告针对的是上述 HEAD 加当前工作区修改，不等同于只审查两个 HEAD commit。

### 1.2 范围

- sched：CLI 解析与命令、schema、SQLite 状态层、dispatcher、allocator、executor、指纹、产物校验、daemon 生命周期、通知和验收脚本。
- dsh 后端：CLI/Local/ssh2 三类传输、写操作门、HTTP/WS 边界、SSH host store、交互认证、输出限制、状态缓存和 sched 命令适配。
- dsh UI：状态视图、批次/任务操作、提交、incidents、SSH 认证弹窗、WebSocket 生命周期、HMR/dispose 和生成物构建。
- 跨仓库：CLI argv、退出码、JSON 形状、批次/任务引用、状态枚举、版本视图、写目标和认证事件。

### 1.3 方法与限制

- 静态逐路径审查；并行独立复核后由主审逐项回看证据。
- 全量执行仓库现有验收/单元测试；执行 Python 编译检查、Node 语法检查和 UI 构建。
- 使用临时 git 仓库、mocked `subprocess` 和直接 schema 调用做定向复现。
- 严格遵守 sched 操作契约：**没有直接编辑任何 `state.db` 或 state 目录文件**。
- 未连接生产 GPU 节点、真实 SSH 服务或生产 dsh 浏览器会话；涉及这些环境的结论来自确定代码路径，动态端到端验证仍需在隔离计算节点完成。
- 这是代码复审，不是形式化证明；“未发现”不代表不存在其他问题。

### 1.4 严重性口径

- **High**：可破坏结果正确性、单写者/单实例不变量、GPU 隔离、任务/批次真实性，或使公开核心路径确定性不可用。
- **Medium**：需要特定规模、时序、部署或输入，但会形成真实错误、挂起、协议失真或敏感信息暴露。
- **Low**：影响边界诊断、耐久性、热重载或首次使用，不直接破坏核心状态。

---

## 2. High 级问题

### S-H01：stage checkpoint 只看文件存在，绕过指纹、完整产物规则和 force_rerun

- **证据**：`sched/gsched/executor.py:174-195` 在多 stage 任务启动时，只要每个声明路径满足 `os.path.isfile()` 就跳过该 stage。`stage_fingerprints` 虽由 `fingerprint.py:90-96` 计算并在 `state.py:612-621` 入库，但没有参与该 checkpoint 判定。
- **触发**：stage 产物已存在后修改代码、修改命令、产物不满足 `check: json`/`min_bytes`/`regex`，或提交 `force_rerun: true`。
- **影响**：任务级指纹可以正确判定“应运行”，但真正进入 executor 后所有旧 stage 仍被跳过，最终把过期或损坏产物当成功结果。
- **修复**：checkpoint 必须复用 `check_artifact`，比较该 stage 的 producer fingerprint，并让 `_force_rerun` 明确禁用所有 stage checkpoint。不要在 executor 中维护第二套弱化的 skip 规则。
- **缺失测试**：产物存在 + 代码变化、规则失败、命令变化、force_rerun 四组 stage 回归。

### S-H02：dirty-tree 指纹哈希的是状态元数据，不是修改内容

- **证据**：`sched/gsched/fingerprint.py:63-88` 对 `git status --porcelain -uno` 的文本取哈希。同一已修改文件从内容 A 再改为内容 B时，status 仍可能保持 ` M train.py`。
- **定向复现**：临时 git 仓库中，对同一个 tracked 文件先写 dirty-A、再写 dirty-B，两次 `compute_fingerprint()` 结果完全相同。
- **影响**：HEAD、命令和 dirty 状态字符串均不变时，第二次未提交代码修改仍可能命中旧 producer 指纹，错误 SKIP。
- **修复**：对 tracked staged/unstaged 内容取稳定哈希，例如合并 `git diff --binary HEAD --` 与必要的 index 信息；git 查询失败时应 fail-closed，不允许基于未知代码状态 SKIP。
- **缺失测试**：clean→dirty 之外，还必须覆盖 dirty-content-A→dirty-content-B、staged→staged 内容变化和 git 查询失败。

### S-H03：done 批次 resubmit 后仍保持 done，失败不会收敛或通知

- **证据**：
  - `sched/gsched/cli.py:1389-1514` 允许 done 批次生成 pending 新版本，但只在原状态为 blocked 时改回 active。
  - `sched/gsched/dispatcher.py:2011-2017` 会派发属于 `active` 或 `done` 批次的 pending job。
  - `dispatcher.py:603-605` 的终态收敛只扫描 `active`/`blocked` 批次。
- **触发**：对已 done 批次执行 `resubmit --all`，新版本随后运行或失败。
- **影响**：批次在新版本运行期间仍显示 done；新版本失败后也不会变 blocked、写 blocked marker 或发失败通知；done marker/API 继续呈现旧成功状态，dsh UI 还会在 `client.jsx:2093` 过滤该批次并隐藏重跑。`_batch_successful()` 已按每任务最新版本判断，因此新建或仍 queued 的依赖不会被错误解锁；已经 active 的下游不会因上游 resubmit 自动回锁。
- **修复**：插入任何新版本前，把 done/blocked 批次统一重开为 active，删除旧 done/blocked marker，并保证通知迁移点只基于最新版本重新触发。
- **缺失测试**：done→resubmit→running，以及 done→resubmit→failed→blocked/notify 的完整状态机。

### S-H04：登录节点只读命令仍会写共享 NFS SQLite/WAL

- **证据**：
  - `sched/gsched/cli.py:2594-2603` 除 foreign submit 和 `config get` 外，所有命令先调用 `state.init_db()`；这包含 `status`、`history`、`task`、`diag`、`incidents` 等查询。
  - `state.py:330-340` 执行 schema、迁移和 `INSERT OR IGNORE`。
  - 即使跳过 init，`state.py:467-474` 的普通 `connect()` 也会建目录并执行 `PRAGMA journal_mode=WAL`。
- **触发**：登录节点对共享 state 执行任一查询命令。
- **影响**：所谓只读查询会成为第二个 NFS WAL writer，违反代码自身记录的单写者事故约束；存在锁异常、检查点覆盖和状态丢失风险。
- **修复**：异机查询使用独立的真正只读连接：SQLite URI `mode=ro`、不建目录、不切 journal mode、不迁移、不 commit；迁移只能由计算节点 writer 完成。
- **缺失测试**：foreign `status/history/incidents` 前后 DB、WAL、SHM、目录和 schema 均零变化。

### S-H05：daemon 生命周期未纳入异机写保护

- **证据**：`sched/gsched/cli.py:2563-2574` 的 `_WRITE_COMMANDS` 漏掉 `daemon`。`daemon.py:102-140` 可启动 dispatcher，`:143-175` 可读取共享 PID 文件、发信号并清 heartbeat/PID 文件。
- **触发**：在登录节点执行 `sched daemon start/stop`；`start --fake` 还能绕开 GPU 前置检查。
- **影响**：可在错误主机启动第二个 state writer；stop 在 PID 命名空间不一致或 PID 复用时可能误处理本地进程并删除计算节点共享生命周期文件。也直接违反本项目 AGENTS.md 的“daemon 生命周期必须在计算节点执行”。
- **修复**：按 action 分类：start/stop 必须与 `config.node` 主机严格一致；check 若依赖本机 GPU 也应限制；status 应走 S-H04 的异机只读通道。
- **缺失测试**：登录节点 start/stop/check 拒绝、status 只读，以及 `SCHED_ALLOW_FOREIGN_WRITE` 明确豁免路径。

### S-H06：项目 GPU 配额在同一派发 tick 内可超卖，并会误挡 CPU-only 任务

- **证据**：
  - `sched/gsched/dispatcher.py:164-180` 把项目 running GPU 数缓存到 `_project_quota_used`。
  - `dispatcher.py:2055-2067` 只在 ready 循环前刷新一次；成功启动 GPU job 后 `:2107-2115` 只更新 CPU 和 batch 计数，不增加项目 GPU 计数。
  - 配额门发生在 `:2065-2067`，而是否 CPU-only 要到 `:2079-2082` 才解析。
- **触发**：`gpu_quota=1`、至少两张空卡、同项目同一 tick 有两个 GPU pending job；或项目 GPU 配额已满时存在 CPU-only job。
- **影响**：两个 GPU job都可启动，配额在下一 tick 才看见超限且不会驱逐；CPU-only job在项目 GPU 用量持续达到 quota 期间会被无关地延迟，直到 GPU 用量降到 quota 以下。
- **修复**：先解析 resources；仅 GPU job检查项目 GPU 配额；每次成功 launch 后原子增加本轮项目使用计数。
- **缺失测试**：同 tick 双 GPU、GPU 配额满 + CPU-only、launch 失败/skip 不计数。

### S-H07：队首超大 GPU 请求可永久饿死后续可运行小任务

- **证据**：`sched/gsched/dispatcher.py:2096-2105` 中，一次 `_assign_in_tx()` 返回 None 且 scope 为 `all` 就设置 `gpu_full=true`，后续所有 GPU job 本轮直接跳过。`dispatcher.py:2191-2235` 中，声明显存超过所有卡容量的独占任务正会返回该结果。
- **触发**：固定 ready 顺序中，无法装入任何卡的大任务排在可装入的小任务之前。
- **影响**：每个 tick 都由同一个大任务先设置 `gpu_full`，小任务即使有空卡也永久不派发，形成队首阻塞。
- **修复**：不要由一个任务的失败推导“所有任务都不可能分配”；继续逐任务尝试，或把永久不可满足请求显式标为 blocked/invalid resource request。
- **缺失测试**：oversized-first + small-second，独占/共享、异构卡容量三组队列。

### S-H08：刚释放但仍被外部进程占用的 GPU 可在确认窗口内重新分配

- **证据**：
  - `sched/gsched/allocator.py:523-554` 把 releasing 卡上的“全外部 PID”视为 sched 残留已离场。
  - `allocator.py:446-499` 在两次 release 确认后把卡改为 free。
  - 同 tick 的 `probe_free()` 在 `:703-734` 虽检测到外部 PID，但第一次只创建 occupied-confirm 标志，仍保留 free。
  - `dispatcher.py:558-575` 随后立即执行派发。
- **触发**：sched job 释放卡时，卡上仍有非 sched compute PID，并且队列中有 pending GPU job。
- **影响**：pending job可在第一次 occupied 确认后被派上实际占用的卡；一旦状态变 assigned，`probe_free()` 不再扫描它，冲突持续到任务失败或人工处理。
- **修复**：release 阶段发现外部 PID应直接转 unmanaged；或把任何 occupied-confirm 中的卡从分配候选中排除。
- **缺失测试**：完整 tick 的 releasing + external PID + queued job 状态序列。

### S-H09：ownerless 新鲜启动锁可被并发启动者删除

- **证据**：`sched/gsched/dispatcher.py:212-257` 先创建 lock 目录，再写共享 `pid_file`。第二个进程看到目录但尚读不到 PID 时，会在 `:243` 无条件清锁后重新创建。
- **触发**：两个 daemon 在第一个 `mkdir(lock_dir)` 与写 pid 文件之间并发启动。
- **影响**：第二个进程删除并重建目录后，两个进程都可能写 PID、触心跳并成功运行，形成双 dispatcher、双派发和双 WAL writer。
- **修复**：ownerless 且新鲜的 lock 必须视为 startup-in-progress；使用原子 owner 文件、`flock` 或带创建时间的 lease，只有可证明过期才能清理。
- **缺失测试**：在 mkdir/write-pid 之间加屏障并发启动，断言只有一个实例通过。

### S-H10：未校验的运行时规则可让 daemon 连续异常退出

- **证据 A（probes）**：`sched/gsched/schema.py:638-669` 直接透传 `probes`；`dispatcher.py:1478-1495` 假设它是映射且模式是字符串，调用 `.get()`/`.encode()`。
- **证据 B（artifact regex）**：`artifacts.py:49-56` 直接执行 `re.search()`，不捕获 `re.error`；schema 没有预编译规则。
- **触发**：例如 `probes: "not-an-object"`，或存在产物配 `regex: "["`。
- **定向复现**：`validate_batch()` 接受了非对象 probes；同类 task env 和不安全标识符也被接受。
- **影响**：异常发生在每个 tick 的 probes/reap/adopt 路径；`dispatcher.py:433-456` 连续五次异常后退出，但存活任务进程和 GPU 不经过正常 stop 收尾。坏存量会在重启后再次触发。
- **修复**：提交期严格验证 probes 对象和字符串值，预编译 regex；运行期仍将坏规则降级为“验证失败/记录 incident”，不得让异常逃出单 job 边界。
- **缺失测试**：非法 probes、非法 artifact regex 的提交拒绝，以及坏存量下 daemon 保持运行。

### S-H11：daemon stop 的 10 秒硬杀窗口短于合法 tick，可能遗留任务进程

- **证据**：`sched/gsched/daemon.py:143-175` 发 SIGTERM 后只等 10 秒便 SIGKILL dispatcher 并清生命周期文件。dispatcher 的 handler 只置停止标志，`dispatcher.py:433-443` 到 tick 边界才执行 `stop()`；一个 tick 可串行执行多个单次 timeout=10 秒的 GPU/subprocess 探测。
- **触发**：stop 发生在慢 `nvidia-smi`、NFS 或其他长 tick 操作中。
- **影响**：dispatcher 在进入 `dispatcher.stop()` 前被 SIGKILL，跳过 job killpg、cancelled 落库、GPU release；任务因独立 session 继续运行。jobs/gpus 中的 PGID 和 assignment 仍保留，后续 daemon 可 adopt，但在重启前没有活跃 dispatcher 完成状态收敛。
- **修复**：等待明确 shutdown ack，超时必须大于可证明的最大 tick；若最终 hard-kill，协调器需先按登记 PGID 终止任务并完成事务收尾。
- **缺失测试**：人为阻塞 tick 超过 10 秒后 stop，核对 job、PGID、GPU 和生命周期文件。

### S-H12：GPU UUID 映射瞬时失败被永久缓存成“无 compute 进程”

- **证据**：`sched/gsched/allocator.py:303-332` 在第一次查询前先把 `_uuid_map={}`，不检查 `--query-gpu=index,uuid` 返回码，失败后永久保留空 map。`_compute_pids_by_card()` 在 `:255-301` 随后把无法映射的 compute PID当成成功的空结果 `{}`。
- **定向复现**：mock 第一次 compute-apps 返回一个 PID，UUID 查询 rc=1；结果为 `{}` 且 `_uuid_map={}`。第二次即使 UUID 查询本可成功，也不再执行映射查询，仍返回空结果。
- **影响**：daemon 生命周期内持续看不见真实 GPU compute 进程；idle util=0 时释放/孤儿防线可把占用卡当干净并重新分配。
- **修复**：检查返回码和映射完整性；失败时不缓存，返回 `None` 让调用方 fail-closed；成功后才原子安装 cache，并支持设备拓扑变化后的刷新。
- **缺失测试**：UUID 查询瞬时失败→恢复，以及存在未知 UUID 时不得返回“确定无进程”。

### X-H01：看板 cancel 缺少必需 `--yes`，当前对所有批次都是无操作

- **证据**：`dsh-node-sched/packages/node-sched/lib/index.js:700-708` 生成 `sched cancel <id>`；sched 的 `cmd_cancel` 要求 `--yes`，缺少时返回 1 且不执行。UI `client.jsx:1178-1180` 虽有二次确认，但没有把确认传给 CLI。
- **触发**：看板点击任一批次 cancel 并完成确认。
- **影响**：CLI 固定拒绝；结合 X-M01，HTTP 还丢失拒绝原因，用户只看到无上下文失败码。
- **修复**：白名单命令追加 `--yes`；同时统一批次 ID/name 精确解析，避免未来同名实例误操作。
- **缺失测试**：UI→HTTP→CLI argv→退出码→批次状态的跨仓库测试。

### X-H02：status 返回 batch ID，但 sched 任务复合引用只接受 batch name

- **证据**：
  - `sched/gsched/cli.py:856-864` 输出 `jobs[].batch = batch_id`。
  - `_resolve_task_ref()` 在 `cli.py:1192-1199` 只把冒号前段交给 `_batch_id_from_name()`。
  - UI `client.jsx:1148-1152` 也确认 `t.batch` 与 `b.id` 对齐，却在 `:1192-1202` 用 `${t.batch}:${t.task}` 调 log/retry/resubmit。
- **触发**：任一失败任务查看日志、retry 或 resubmit。
- **影响**：三条任务级看板操作对正常 status 行均报“批次不存在”。
- **修复**：sched 的统一 resolver 先按完整 batch ID 精确查找，再按 name 回退；task/log/diag/retry/resubmit/cancel 共用同一实现。JSON 应明确同时提供 `batch_id` 和 `batch_name`。
- **缺失测试**：从真实 status payload 取引用，再逐条调用所有任务命令。

### X-H03：dsh history 工具调用不存在的 `sched history --json`

- **证据**：dsh `packages/node-sched/lib/index.js:535-543` 始终执行 `sched history --json`；sched history parser 在 `gsched/cli.py:2438-2444` 没有 `--json`，`cmd_history()` 也只输出文本。
- **定向复现**：`python3 -m gsched.cli history --json` 在 argparse 阶段退出 2：`unrecognized arguments: --json`。
- **影响**：公开模型工具 `sched_history` 无论参数如何都不可用。
- **修复**：优选给 producer 增加纯 JSON、版本化且有分页的 history 协议；否则 consumer 必须按文本查询并声明非结构化结果。
- **缺失测试**：工具注册命令与真实 sched parser/JSON shape 的契约测试。


---

## 3. Medium 级问题

### S-M01：dry-run 的 JSON 输出和 SKIP 预测都不符合实际契约

- **证据**：`sched/gsched/cli.py:265-297` 在 `--json` 时仍先打印标题、任务和汇总，最后才追加 JSON；`cli.py:203-231` 虽计算 fingerprint，预测函数却只看产物规则，不比较 producer fingerprint，也不处理 force_rerun。
- **影响**：`sched submit x.json --dry-run --json | jq` 无法解析；代码已变或强制重跑时，预览仍可能显示 SKIP，而实际 dispatcher 会 RUN。
- **修复**：JSON 模式 stdout 只写一个完整对象，诊断转 stderr/结构化字段；skip 预览复用生产判定的只读实现。
- **缺失测试**：标准 JSON parser 纯度，以及 code drift/force_rerun 的预览与实际一致性。

### S-M02：`history --project` 使用未定义变量 `cfg`

- **证据**：`sched/gsched/cli.py:1001-1028` 的 `cmd_history()` 未加载配置，却在 project 分支访问 `cfg.get("projects")`。
- **触发**：任何 `sched history --project <name>`。
- **影响**：稳定抛 `NameError` traceback，而不是返回历史或正常的未知项目错误。
- **修复**：函数入口加载配置并复用 status 的项目校验。
- **缺失测试**：有效项目、未知项目和空历史三种退出码/输出。

### S-M03：resubmit 在 SQLite 写事务内执行慢 git 指纹探测

- **证据**：`sched/gsched/cli.py:1470-1489` 先 insert task 获得写锁，再逐任务执行最多两次 10 秒 git subprocess 的 `compute_fingerprint()`；外层还持 submission lease。
- **影响**：慢仓库/NFS 下长期占写锁，daemon 的 5 秒 busy timeout 可报 `database is locked`。旧审计 M9 的 submit/inbox 主路径已修，但 resubmit 仍开放。
- **修复**：先只读收集 spec 并在事务外预计算全部 fingerprint，再开启短写事务统一插入。
- **缺失测试**：慢 git 与 daemon 并发写。

### S-M04：`config.state_dir` 被提示、存储和当作冷键，但运行时完全忽略

- **证据**：`sched/gsched/cli.py:97-99` 的 init 向导保存 `state_dir`；CLI/dispatcher 还把它列为冷键。实际 `gsched/config.py:26-28` 的 `default_state_dir()` 只读 `SCHED_STATE` 环境变量或 `~/.sched`，state/dispatcher/notify/allocator 均调用该函数。
- **触发**：用户在 `sched init` 选择非默认 state 目录，但没有另设 `SCHED_STATE`。
- **影响**：配置表面成功，实际 DB、marker、log 和 inbox 写入另一目录；诊断与备份可能针对错误位置。
- **修复**：只保留一个事实来源。若配置字段生效，应解决 config 路径自举并全局消费；否则删除向导字段/冷键，只文档化 `SCHED_STATE`。
- **缺失测试**：init 选择自定义 state 后，所有派生路径一致。

### S-M05：batch/task 标识符未限制，却直接进入文件系统路径

- **证据**：`sched/gsched/schema.py:409-411,545-550` 只要求非空字符串。batch ID由 name直接拼时间戳；`dispatcher.py:723-736` 用 name生成 marker，`:2568-2573` 用 batch ID/task ID生成日志路径。
- **定向复现**：schema 接受 batch `../../outside` 和 task `../task`。
- **影响**：斜杠、绝对路径和 `..` 可把 marker/log 写到预期 state 子树之外，并破坏 `batch:task` CLI 引用语法；dsh 的操作白名单又只接受较窄字符集，形成新的跨仓库不兼容。
- **修复**：为 batch name、task ID、dependency name定义统一的安全 slug/长度；所有派生路径再做 `realpath/commonpath` containment 防御。
- **缺失测试**：绝对路径、`..`、斜杠、冒号、超长 Unicode 名称。

### S-M06：若干 task 运行时字段只透传，不做类型/范围验证

- **证据**：`sched/gsched/schema.py:638-669` 接受任意 task env、非正/非有限 duration 和任意类型 `paths_escape`；batch env 反而在 `:429-431` 正确验证。
- **定向复现**：`validate_batch()` 接受 `env: ["BROKEN"]`；launch 时 `dispatcher.py:2411-2414` 的 `dict(...)` 才失败。负 duration 会立即超时；字符串 `"false"` 对 paths_escape 为 truthy。
- **影响**：本应提交期拒绝的批次进入队列后才重试/blocked，或产生与声明相反的路径/超时语义。
- **修复**：env 必须是字符串键值映射；duration 必须 finite 且 >0；paths_escape 必须 bool；所有 numeric 字段统一排除 bool、NaN 和 Infinity。
- **缺失测试**：边界类型和值的表驱动 schema 测试。

### S-M07：status 当前视图混入所有历史版本，进度和可操作失败项失真

- **证据**：`sched/gsched/cli.py:793-797` 用该 batch 的全部 jobs 计算 progress；`:806-865` 把所有版本都输出。dispatcher 的 settle/depends 已按每个 task 的 MAX(version) 判定。
- **触发**：任一任务 resubmit 产生 v2+。
- **影响**：例如 v1 failed、v2 done 的单任务批次显示 progress `1/2`；dsh 继续把旧失败版本当当前失败项，展示重复/过期操作按钮。
- **修复**：status 默认只输出每任务最新版本；历史版本放入独立 history/paginated 字段。若保留全部行，必须标记 `is_latest` 并以 latest 计算 progress。
- **缺失测试**：多版本批次的 status JSON 和 UI。

### S-M08：daemon start/stop/check 的失败仍返回退出码 0

- **证据**：`sched/gsched/cli.py:2258-2276` 只打印 daemon 层文本，函数末尾无条件 `return 0`，即使 start 前检拒绝、check 有 FAIL、stop 提示错误主机。
- **影响**：自动化和 dsh 的 `operate()` 把未启动/未停止误报为成功。
- **修复**：daemon 层返回结构化结果或 `(ok,text)`；拒绝和 FAIL 非零，幂等“已运行/已停止”可明确定义为 0。
- **缺失测试**：每个 action 的成功、拒绝、幂等和异常退出码。

### X-M01：operate 已生成统一 envelope，但 HTTP 路由读取不存在的 stdout/stderr

- **证据**：dsh `packages/node-sched/lib/index.js:148-161,470-476` 返回 `{ok,code,text,raw}`；submit 路由 `:960-962` 和 op 路由 `:1008-1009` 却读取 `r.stdout/r.stderr`。
- **影响**：响应 text 恒为空；提交成功丢 batch ID/持久化提示，失败丢 CLI 原因。X-H01 的 cancel 拒绝因此只剩 code=1。
- **修复**：两处直接返回受限长度的 `r.text`；路由测试同时断言成功和非零退出正文。

### X-M02：producer/consumer 没有共享 canonical 状态枚举

- **证据**：sched status 在 `cli.py:831-855` 把 pending 派生为字符串 `pending(quota)`；dsh `index.js:169-183` 只把 `running/pending/waiting_dep` 当 live，并只把 `done/skip` 当 terminal，导致 discarded 被列为 active、quota 等待任务从 live 明细消失。
- **影响**：模型工具摘要与 UI/CLI状态语义不一致；新状态加入时会静默归入错误类别。
- **修复**：JSON 保留 canonical `status: "pending"`，另给 `waiting_reason: "quota"`；共享并测试完整枚举，consumer 对未知值显式标注 unknown。
- **缺失测试**：逐状态 producer payload→`summarizeStatus()` 表驱动测试。

### X-M03：dsh mutation 缺少配置驱动的唯一计算节点 writer

- **证据**：
  - CLI 模式在 `packages/node-sched/lib/index.js:336-368` 硬编码 screen session `3323979.ambior1`。
  - local/engine 模式在 `:341-345` 直接在当前 target 执行。
  - bind 在 `:1365-1400` 只要求 alias 可达；daemon probe 失败不阻止绑定，也不核验远端 hostname 等于 sched `config.node`。
- **触发**：目标部署没有该 screen session，或 engine 绑定到网关/其他可达主机。
- **影响**：CLI 模式依赖一个未配置、易变化的部署会话；会话不存在时写操作失败。engine 误绑定时，大多数写命令被 sched foreign-write guard 拒绝；daemon wrong-host 的高危部分由 S-H05 单独计数。
- **修复**：读取 target 和 mutation writer 必须分离；writer 由显式部署配置指定并在启动/绑定时核验 hostname/config.node。不要把用户选择的查询 SSH alias 隐式变成写节点。
- **缺失测试**：local/CLI/engine × gateway/compute 的路由矩阵、screen 不存在和错误主机拒绝。

### D-M01：敏感读取路由没有 loopback/Origin 围栏

- **证据**：写路由在 `packages/node-sched/lib/index.js:688-696` 使用 `writeGuard`，SSH/WS 路由也有 loopback+Origin 校验；但 `/sched/api/status`、`gpus`、`incidents`、`config` GET、`daemon` 和 `log`（约 `:779-1025`）均未调用同类 guard。
- **触发**：宿主 webServer 绑定非 loopback，或经反向代理暴露这些路径。
- **影响**：未认证访问者可读取任务/项目名、完整 config、incident 和任务日志；日志/config 可能包含路径、命令、邮箱和运行环境信息。
- **修复**：若插件设计为本地面板，所有 `/sched/*` 路由统一套同一个 Host/Origin/remote-address policy；若必须远程访问，应由宿主认证授权，而不是仅保护写路径。
- **备注**：若生产 webServer 被外部机制严格限制为本机，该问题风险降低，但代码自身没有建立该不变量。

### D-M02：HostStore 的部分 auth 更新会清空未提供的已存字段

- **证据**：`packages/node-sched/lib/ssh-engine.js:118-127` 用 `patch.auth ?? entry.auth` 整体替换；`normalizePayload()` 在 `:230-246` 只构造请求中出现的 auth 字段。
- **触发**：PATCH 只更新 passphrase、kbdintPassword 或 auth kind 的部分字段。
- **影响**：未随请求重发的 keyPath/password/passphrase 被清空，后续连接失败。浏览器又不会回读 secret，调用者无法安全做 read-modify-write。
- **修复**：auth 内部字段级 merge，并定义显式 clear（例如 `null`）与 omitted 的区别；kind 变化时才按新 kind清理不兼容字段。
- **缺失测试**：keyPath 保留、secret 保留、显式清空、kind 切换。

### D-M03：通用 SSH exec 会在未知结果时自动重放非幂等命令

- **证据**：`packages/node-sched/lib/index.js:1321-1344` 的 `/sched/ssh/exec` 调 `sshEngine.exec()`；`ssh-engine.js:620-646` 最多重试三次，并在注释中承认连接中断可能重放非幂等命令。默认 alias 最终还可回退 CLI 再执行一次。
- **触发**：远端命令已经产生副作用，但 exit status 回来前连接断开。
- **影响**：submit、删除、移动、启动等任意用户命令可能执行多次；最终返回也无法表达“第一次是否已成功”的不确定结果。
- **修复**：通用 exec 默认 `execOnce`，命令一旦交给远端就不自动换通道重放；只有显式声明 idempotent 的调用才允许 retry。
- **缺失测试**：远端已执行、close 无 exit status 的 unknown-outcome 场景。

### D-M04：CLI 上传通道没有总超时和输出上限

- **证据**：`packages/node-sched/lib/index.js:736-759` 直接 `spawn("ssh")`，累积 stdout/stderr 字符串，没有 deadline、kill 或 max bytes。`ConnectTimeout` 只覆盖连接建立，不覆盖远端 `cat`/shell。
- **影响**：远端或网络异常可让 HTTP dry-run/submit/config 请求无限挂起；异常 banner/stderr 可持续增长内存。
- **修复**：复用已有 bounded runner/transport 的 stdin 执行，设置总 deadline、进程树终止和逐流原始字节上限。
- **缺失测试**：远端不退出、stderr 洪泛、客户端取消。

### D-M05：全量 status JSON 迟早超过 2 MiB，并会在 JSON 中间被截断

- **证据**：sched status 默认输出全部历史 batches/jobs；dsh `index.js:164-167` 已注明当前 raw 约 1 MiB。CLI/Local/Engine transport 默认 2 MiB 上限；截断后 `envelope()` JSON.parse 失败，status cache 拒绝更新。
- **触发**：历史增长使合法 JSON 略超 2 MiB。
- **影响**：命令退出码仍为 0，但 `/sched/api/status`、看板和模型工具同时失去权威状态。
- **修复**：producer 提供有界 current-status API（活跃项、最新版本、聚合计数）和分页 history；不能在结构化 JSON 字节流中间截断。
- **缺失测试**：略大于 2 MiB 的合法 status，覆盖 CLI、Local、Engine 三通道。

### D-M06：keyboard-interactive 的 180 秒承诺被 15 秒握手 deadline 截断；隐藏面板仍被当作可交互 audience

- **证据**：
  - `ssh-engine.js:24-26,366-369` 默认 `readyTimeout=15s`；同一 `connectClient()` 握手内的 keyboard-interactive 在 `:447-476` 等待 UI 最长 180 秒，因此会被握手 deadline 抢先终止。
  - private-key passphrase 路径不同：`connectWithInteractiveAuth()` 在首次 `connectClient()` 失败后才等待 prompter，取得口令后启动第二次连接，不受第一次 15 秒 deadline 覆盖。
  - 后端 `index.js:1078` 仅以 `clients.size>0` 判断 audience；前端 Dashboard 在 `client.jsx:2276` 始终挂载并建立 WS，面板隐藏仅是 CSS，认证 modal 也在隐藏树内。
- **触发**：keyboard-interactive 用户超过约 15 秒回答；或面板关闭但后台查询触发 keyboard-interactive/private-key passphrase。
- **影响**：keyboard-interactive 的 180 秒 UI 承诺无效；隐藏面板仍可让后台查询等待不可见输入，用户看见的只是离线/陈旧状态。
- **修复**：用一个可取消 deadline统一 keyboard-interactive 握手和 UI challenge；前端显式上报可见 audience，或把 auth modal portal 到始终可见的宿主层并主动通知。
- **缺失测试**：keyboard-interactive 在 15-180 秒之间回答、private-key passphrase 独立 deadline，以及 panel hidden 时的 challenge。

### D-M07：认证 challenge 的重连、超时和多客户端结束状态不闭环

- **证据**：`packages/node-sched/lib/index.js:1061-1066` 最后一个 events client 断开就 `clearPendingAuth()`，与 `:1196-1200` 声称的重连重放相冲突；timeout/answer 在 `:273-279,1469-1478` 删除 pending 后不广播 resolved/expired。前端 `client.jsx:429-436` 只处理新增 auth 帧。
- **触发**：唯一看板短暂断线；challenge 超时；两个看板中一个回答。
- **影响**：重连必然取消正在进行的 SSH 登录；保持连接的其他看板会持续显示已过期 prompt并遮挡后续 challenge，直到用户提交/取消触发 404 清除，或 WS 重连清空队列。
- **修复**：断线保留 challenge 到原 deadline/短重连宽限；所有终止路径广播带 ID 的 resolved/expired/cancelled；前端按 ID删除队列项。
- **缺失测试**：challenge→断线→重连→回答，以及双客户端同步。

### D-M08：合法的零提示 keyboard-interactive challenge 无法完成

- **证据**：后端记录真实 `promptCount=0` 并在 `index.js:1466-1467` 要求答案数严格等于 0；前端 `client.jsx:548-550` 把空 prompts 变成一个虚构输入框，`:582-583` 又禁止真正的零 prompt 提交。
- **影响**：前端发送一个答案时固定收到 400 `authentication answer count mismatch`；只能取消/超时。
- **修复**：保留显式空数组并自动发送 `answers: []`，或提供“继续”按钮；只有旧帧缺失 prompts 字段时才使用兼容 fallback。
- **缺失测试**：0 prompt 的后端→前端→answer-count 端到端契约。

---

## 4. Low 级问题

### S-L01：仍有被接受、持久化但没有运行时消费者的字段

- `batches.gpus` 被 schema 接受并入库，但 dispatcher 选卡只使用 daemon config 和 task resources。
- `retry_transform` 在 task/stage 透传并入库，但没有重试变换消费者。
- stage 级 `probes` 被保存，但 `_check_probes()` 只读取 task 顶层 probes。
- `stage_fingerprints` 的“无消费者”已升级为 S-H01，不仅是卫生问题。

**影响**：调用者会合理相信声明生效，实际被静默忽略。应删除未实现字段，或实现并为其添加行为测试；不能继续“接受但不执行”。

### D-L01：输出限制实现的计数单位/初始化不一致

- `packages/node-sched/lib/transport.js:27-28,52-53` 的 LocalTransport accumulator 没有初始化 `droppedBytes`，溢出后显示 `truncated NaN bytes`。
- `ssh-engine.js:649-658` 用 JS 字符串长度与 Buffer 字节长度混算 `maxOutputBytes`，多字节输出可超过声明的原始字节上限。

**修复**：三传输共用同一个按原始字节计数、UTF-8 边界安全的 accumulator；补本地/引擎溢出测试。

### D-L02：entry override 重复同步写，且持久化不是原子替换

- `packages/node-sched/lib/index.js:842-844` 对同一 entry 更新连续调用两次 `persistEntryOverride()`。
- 该 helper 使用直接 `writeFileSync`，崩溃窗口可能留下截断 JSON；与 HostStore 的 tmp+rename 模式不一致。

**影响**：重复 I/O；异常退出可丢失 binding。删除重复调用并采用 mode 0600 的 tmp+fsync/rename（至少 tmp+rename）。

### UI-L01：认证答案提交失败没有任何可见错误

- `packages/node-sched-ui/src/client.jsx:559-578` 对非 2xx（除 404）和 fetch rejection 只复位 busy，不读取或显示服务端原因。
- 用户无法区分长度不匹配、403、500 和网络失败，通常会等待到超时。

**修复**：保留输入，显示受限长度的可访问 error/status；重试成功或切换 request 时清除。

### UI-L02：HMR dispose 未按 capture=true 移除 sidebar click listener

- 注册：`packages/node-sched-ui/src/client.jsx:2301`，`addEventListener("click", ..., true)`。
- 移除：`:2306`，缺少第三个参数 true。

**影响**：每次插件热重载都残留一个 capture listener，重复执行 panel hide 逻辑。移除时必须使用相同 capture 选项。

### UI-L03：提交框唯一示例本身不符合 sched schema

- `packages/node-sched-ui/src/client.jsx:1944-1946` placeholder 没有 schema 强制的 `project`，却展示未定义的 `schema_version`。
- `sched/gsched/schema.py:449-458` 明确要求 project 存在于 `config.projects`。

**影响**：用户按唯一示例 dry-run 必定失败。示例应从已加载 config提供 project 选择，并只使用当前受支持字段。

---

## 5. 跨仓库契约矩阵

| 公开路径 | Producer/Consumer | 当前判定 | 主要问题 |
|---|---|---|---|
| `status --json` → 看板/工具 | sched → dsh | 条件可用 | 当前/历史版本混合；`pending(quota)` 枚举漂移；超过 2 MiB 后整体失效 |
| `history` → `sched_history` | sched → dsh | **确定性失败** | dsh 固定传不存在的 `--json` |
| batch cancel | UI → dsh → sched | **确定性失败** | 缺 `--yes`；失败正文又被 envelope 字段错配吞掉 |
| task log/retry/resubmit | UI → dsh → sched | **确定性失败** | status 给 batch ID，resolver 只接受 batch name |
| batch submit | UI → dsh → sched | 部分可用 | 上传绝对路径已修；submit gate 已固定；响应 text 丢失；示例无效 |
| dry-run | UI → dsh → sched | UI 文本路径可用 | sched 自身 `--json` 不纯；skip 预测与实际不一致 |
| config get/set | UI → dsh → sched | 主路径可用 | GET 暴露边界；`state_dir` 配置无效；entry override 重复写 |
| incidents list/detail | UI → dsh → sched | 可用 | 数字 ID/job quoting、行内 loading/error/overflow 当前正确 |
| daemon start/stop | UI → dsh → sched | 不可信 | writer target 不稳定；sched 可在异机执行；失败退出码恒 0 |
| Local transport | dsh | 主路径可用 | exec/stdin/stream 测试通过；溢出计数为 NaN |
| SSH interactive auth | dsh backend ↔ UI | 条件失效 | 15s/180s deadline、隐藏 audience、重连/结束事件、0 prompt |

建议建立一个真正的跨仓库契约测试包，至少固定：

1. 每个 dsh 命令模板对应 sched parser 的 argv 与退出码。
2. status/history/incidents 的 JSON schema、canonical 状态枚举和版本策略。
3. status 产生的每个 batch/task 引用可直接回喂 task/log/diag/retry/resubmit/cancel。
4. local/CLI/engine 三通道的 writer hostname 不变量。
5. auth challenge 的 0/N prompts、断线重连、多客户端和超时状态机。

---

## 6. 对 2026-08-28 旧审计的复核

`docs/audit_2026-08-28.md` 是审查开始时已有的未跟踪候选报告。当前代码已经修复其中大部分问题，但不能把旧报告的“已排查无问题”段落继续当成事实。

### 6.1 已确认解决

| 旧项 | 当前状态与证据 |
|---|---|
| 已知 UI incidents 文本重叠 | 已解决：log excerpt 有 `maxHeight` + `overflow:auto` |
| 已知 UI incidents 详情在列表底部 | 已解决：详情/loading/error 位于对应 row wrapper 内，可 toggle |
| 已知 SSH 不支持本地 daemon | 已解决主路径：`LocalTransport` 已实现 exec/stdin/stream 并有测试 |
| sched C1 inbox 模板不展开 | 已解决：CLI/inbox 共用 `expand_cmd`，验收覆盖 task/stage 模板 |
| sched C2 foreign submit 仍写 DB | 已解决 submit 主路径：登录节点 file-only inbox，daemon 扫描消费 |
| sched H1 run project 错位 | 已解决：关键字 `project=proj`，验收通过 |
| sched H2 adopt rc | 已解决：持久 rc/launch marker 接管路径与验收存在 |
| sched H3 capacity 不查 rc | 已解决：rc/空输出 fail-closed |
| sched H4 inbox 不提示 daemon 健康 | 已解决：foreign submit 有健康提示/verify；不跨节点自行 start 是设计约束 |
| sched H5 依赖全版本 | 已解决主路径：depends/settle 使用 latest version 并检查 stale live |
| sched M1-M8、M10 | 当前对应 venv、running guard、cancel 去重、显存文案、共享 affinity、notify command、daemon 健康、incident 豁免和文档问题已修/有验收 |
| dsh B-C1/B-H3 Web/WS 写边界 | 已解决写面：terminal/events/auth-answer 和 mutating HTTP 使用 loopback+Origin/Host policy |
| dsh B-H1/B-H2 上传路径 | 已解决：解析上传命令返回的绝对路径 |
| dsh B-H4 tail 泄漏 | 已解决：clients 归零和 disposer 都停止 tail/restart timer |
| dsh B-M2-B-M7 | entry merge、tail 单飞/重试、status cache、submit gate、alias 查重、screen framing/长度保护已修 |
| dsh 原 B-L1-B-L10 主症状 | body cap、CLI cap、redaction、有限数值、r.ok、错误分类、in-flight close、日志清洗、safeError、status singleflight 已实现 |
| UI U-M1-U-M4/U-L1-U-L6 | pending/异常处理、wss、稳定 key、incidents 状态、WS cleanup、调试代码、telemetry disposer、ArmButton、数值输入和 toggle 已修 |

上述“已解决”指旧症状关闭，不表示相关子系统没有本报告中新问题。

### 6.2 仍开放或演化为新问题

| 旧项 | 当前状态 |
|---|---|
| sched M9：事务内 git 指纹 | submit/inbox 已修；**resubmit 仍开放**，见 S-M03 |
| sched L3：stage_fingerprints/死字段 | stage_fingerprints 无消费者已升级为 **S-H01**；其他字段见 S-L01 |
| dsh B-M1：screenExec 与 transport/writer | local/engine 分支已实现，但唯一计算节点 writer 仍未闭环，见 X-M03 |
| dsh B-M8：auth 部分更新 | **仍开放**，见 D-M02 |
| 旧报告“dirty-tree 指纹正确” | 被本次 dirty-A→dirty-B 定向复现推翻，见 S-H02 |
| 旧 L7 独占分配延迟 | 当前同类逻辑可造成永久队首阻塞，升级为 S-H07 |

---

## 7. 已确认的正向证据

这些区域经过检查，当前没有发现与本报告同等级的开放缺陷：

### sched

- foreign submit 已改为文件 inbox，dsh 也没有直接打开 `state.db`。
- dispatcher 的普通 job launch/cancel 条件更新、进程组 `start_new_session`/killpg、rc/launch marker、最新版本 settle 主路径已有明确保护。
- `probe_capacity` 已检查失败并 fail-closed；GPU 配置卡号和显存覆盖有解析校验。
- config 写入使用 tmp+replace，热更新对冷键有拒绝逻辑。
- 日志读取使用尾部读取；executor 正常 launch/Popen 失败路径会关闭日志句柄。
- notification file/email/command 主路径和 ack cleanup 有验收覆盖。

### dsh 后端

- 未发现直接访问 sched SQLite/state DB 的代码；调度操作均经 CLI。
- mutating HTTP、terminal WS、events WS、auth-answer 均已有 remote-address + Host/Origin 防护。
- sched 白名单参数大多经过 `shellQuote` 或数字/alias正则；未发现当前外部 batch/task/log 参数的直接 shell 注入。
- 审计日志对 secret、Authorization、URL credential、sshpass 和控制字符有测试覆盖。
- HostStore 主文件使用 mode 0600 的 tmp+rename；重复 alias 已拒绝。
- standard CLI runner 的 body/output cap、UTF-8 边界、status singleflight、tail cleanup 和 in-flight connection disposal 有测试。
- LocalTransport 的普通 exec/stdin/stream 生命周期测试通过。

### dsh UI

- 未使用 `dangerouslySetInnerHTML` 渲染远端日志、incident 或命令；当前均走 React 文本节点。
- incidents 具备行内展开、loading/error、请求代数防竞态、冻结快照缓存和 overflow 约束。
- 普通操作有前端稳定 key 去重、pending 状态和 try/catch/finally。
- events WS 使用 `ws/wss` 自适应、JSON parse 防护、unmount 后停止重连。
- terminal 组件会释放 ResizeObserver、WebSocket 和 xterm；服务端 close/error 会关闭 shell。
- UI 源码已成功重新构建为 `lib/client.js` 和 source map。

---

## 8. 验证记录

### 8.1 动态检查

| 检查 | 结果 |
|---|---|
| sched `tests/run_*_accept.sh` 全量顺序执行 | **51/51 通过**，总计约 1237.97 秒 |
| `python3 -m compileall -q gsched` | 通过 |
| `node --test packages/node-sched/test/*.test.js` | **43/43 通过**，0 fail |
| `npm run build`（`packages/node-sched-ui`） | 通过，`client bundle built` |
| `node --check`：index/ssh-engine/transport/UI bundle | 全部通过 |
| `python3 -m gsched.cli history --json` | 退出 2，确认 unsupported argv |
| 临时 git dirty-A→dirty-B 指纹 | 两次 fingerprint 相同，确认 S-H02 |
| mocked GPU UUID 查询失败→恢复 | 空 map 被永久缓存，确认 S-H12 |
| 直接 `validate_batch()` 边界输入 | 接受非对象 probes、list task env 和路径型 name/id，确认 schema 缺口 |
| 隔离 Executor：存在但不满足 min_bytes 的 stage 产物 | 任务 rc=0、stage 命令未执行、日志为“产物已存在, 跳过”，确认 S-H01 |
| LocalTransport `maxOutputBytes=4` 输出 9 bytes | 返回 `…[truncated NaN bytes]`，确认 D-L01 |

全量 sched 验收脚本名称：

<details>
<summary>51 个脚本</summary>

`run_adopt_rc_accept.sh`, `run_affinity_config_accept.sh`, `run_b13_accept.sh`, `run_cancel_daemon_health_accept.sh`, `run_cancel_dedupe_accept.sh`, `run_cancel_forward_accept.sh`, `run_cancel_pending_accept.sh`, `run_cancel_project_versions_accept.sh`, `run_clean_versions_accept.sh`, `run_cmd_project_accept.sh`, `run_colocate_accept.sh`, `run_config_get_guard_accept.sh`, `run_confirm_flags_accept.sh`, `run_cpu_quota_accept.sh`, `run_daemon_heartbeat_accept.sh`, `run_dependency_versions_accept.sh`, `run_discard_accept.sh`, `run_docs_refs_accept.sh`, `run_duration_help_accept.sh`, `run_exclusive_scope_accept.sh`, `run_gpu_mem_accept.sh`, `run_gpu_selfheal_accept.sh`, `run_hotreload_accept.sh`, `run_inbox_template_accept.sh`, `run_incident_accept.sh`, `run_init_notify_accept.sh`, `run_launch_failure_accept.sh`, `run_lock_heartbeat_accept.sh`, `run_mode_consistency_accept.sh`, `run_multilevel_caps_accept.sh`, `run_notify_accept.sh`, `run_notify_project_usage_accept.sh`, `run_probe_capacity_accept.sh`, `run_probe_unicode_accept.sh`, `run_probes_accept.sh`, `run_proj_colocate_accept.sh`, `run_resubmit_batch_accept.sh`, `run_resubmit_running_accept.sh`, `run_resubmit_venv_accept.sh`, `run_runtime_accept.sh`, `run_runtime_bounds_accept.sh`, `run_shared_affinity_accept.sh`, `run_state_migration_accept.sh`, `run_submit_daemon_health_accept.sh`, `run_submit_fingerprint_lock_accept.sh`, `run_submit_inbox_accept.sh`, `run_submit_single_writer_accept.sh`, `run_sudo_guard_accept.sh`, `run_taskenv_accept.sh`, `run_unmanaged_recover_accept.sh`, `run_ux_accept.sh`。

</details>

### 8.2 现有测试为什么没有发现 High 问题

当前测试主要覆盖单次正常状态迁移和此前修复的具体事故；缺少以下“相邻状态/第二次变化/同 tick/跨仓库”维度：

- dirty→dirty 内容再次变化，而非只测 clean→dirty。
- task-level fingerprint 判 RUN 后，executor stage checkpoint 的第二层决策。
- done 批次重开后的失败收敛。
- foreign read 的零写入属性和 daemon action 的主机身份。
- 同 tick 多 job 的项目 quota、队首 oversized、小任务补位。
- releasing→free→probe_free→dispatch 的整 tick 组合。
- UUID 查询瞬时失败后恢复。
- concurrent start 在 mkdir 与 pid write 之间的竞态。
- malformed probes/artifact regex 对 daemon 存活的影响。
- 慢 tick 中执行 daemon stop。
- dsh 生成的每条 CLI argv 对真实 sched parser 的契约。
- status 多版本/大于 2 MiB、auth 断线/超时/0 prompt。

---

## 9. 建议修复顺序与放行门

### P0：先恢复不变量

1. **单 writer/单 daemon**：S-H04、S-H05、S-H09。
2. **结果不可错误复用**：S-H01、S-H02。
3. **GPU 安全与活性**：S-H06、S-H07、S-H08、S-H12。
4. **状态真实性和正常退出**：S-H03、S-H10、S-H11。
5. **恢复公开操作契约**：X-H01、X-H02、X-H03。

### P1：稳定结构化接口和认证

- S-M01、S-M02、S-M07、S-M08、X-M01、X-M02、X-M03、D-M05。
- D-M06、D-M07、D-M08。
- 建立跨仓库 contract suite；不要继续分别靠两个仓库的局部测试猜测对方协议。

### P2：收紧边界和可维护性

- S-M03-S-M06、D-M01-D-M04。
- 删除或实现 S-L01 的死字段。
- 修复输出 accounting、override 原子性、认证错误反馈、HMR listener 和提交示例。

### 放行条件

在以下条件满足前，不应把当前版本标记为“调度正确/可长期无人值守”：

1. 15 个 High 项全部关闭，且每项都有能在修复前失败的行为测试。
2. 建立跨仓库 argv/JSON/reference contract suite，cancel、history、task log/retry/resubmit 全部从真实 status payload 端到端通过。
3. 在隔离计算节点验证 GPU 配额、oversized queue、external PID release、UUID probe failure 和 slow-stop。
4. 在 CLI/Local/Engine 三种部署分别验证 mutation 最终只在 `config.node` 执行。
5. 在真实浏览器/SSH 测试 auth 的 0/N prompts、keyboard-interactive 15-180 秒回答、private-key passphrase、面板隐藏、断线重连、多客户端和超时。
6. 再执行本报告中的 51+43 现有回归，确保旧事故修复未回退。
