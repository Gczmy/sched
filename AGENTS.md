# sched 开发指南（gsched GPU 任务调度器）

## LLM agent 操作契约（L0）

本目录的调度器保持"无意识"：不含任何 agent 逻辑，只提供标准接口。**agent 操作调度器的唯一正确方式是 `sched` CLI**（入口 `gsched.cli:main`，零依赖纯标准库）。
- 配套项目为 `/Users/zzc/quant_trade/dsh-node-sched`，与 `sched` 配合使用。

### 铁律

- **只走 CLI，禁止直接改 state.db / state 目录文件**（WAL 并发有协议，手工 SQL 属反模式，历史事故）。要改状态就找对应子命令；没有对应命令 = 先提需求，不要绕。
- **脚本/agent 解析输出一律用 `--json`**（`status --json`、`submit --dry-run --json`），不要解析人类可读文本。
- 破坏性命令需要 `--yes`（`cancel`、`discard`、`clean`、`config set`、`gpu-free`）；缺 `--yes` 返回码 1 是“未确认”，不是执行失败。
- daemon 生命周期与前置检查（`sched daemon start/stop/check`）必须在**计算节点**执行；查询类命令在登录节点可直接用，数据库查询通过私有只读 DB/WAL 快照完成。
### 网关纪律

- 任何 SSH 操作前，必须先询问用户当前是校外还是校内环境；校内使用 `HPDC`，校外使用 `HPDC_outside`。
- 网关禁止运行任何计算任务；测试、训练、批处理和 smoke test 必须在计算节点执行。正常提交统一使用网关上的 `sched submit <batch.json>`，由文件 inbox 交给计算节点 daemon 收编；返回“已投递”后用 `sched verify` 确认入库。
- `sched run` 是计算节点直写入口，不走网关 inbox；除 dry-run 外不得在网关执行。网关上的其他 mutation 默认也会被主机守卫拒绝，不把 `SCHED_ALLOW_FOREIGN_WRITE=1` 当作日常工作流。
- daemon 统一运行在 `84016.ambior1` 上。
- 如需重启 daemon，严格按以下步骤执行：
  1. 根据网络环境执行 `ssh HPDC`（校内）或 `ssh HPDC_outside`（校外）。
  2. 执行 `screen -d -r 84016.ambior1`。
  3. 重启 daemon。

### 子命令速查

| 命令 | 用途 | 关键参数 |
|------|------|---------|
| `sched submit <batch.json>` | 提交批次（网关推荐入口） | `--dry-run [--json]` 只读预览；异机投递后 `sched verify` |
| `sched run` | 计算节点一行提交单任务 | 只支持 `--gpus 1` 或 `--cpu-only`；`--dry-run` 只读 |
| `sched status [批次]` | 三视图总览 | `--json`（脚本用）、`--detail`、`--project`、分页 cursor |
| `sched task <批次>:<任务>` | 单任务详情 | `--json` 输出稳定版本化文档 |
| `sched diag <批次>[:任务]` | 失败诊断（状态+命令+git+日志尾部） | 排雷首选，一步到位 |
| `sched log <批次>:<任务>` | 任务日志 | `-n N` 尾部行数、`-f` 跟踪 |
| `sched retry <批次>[:任务]` | 解锁失败终态重跑 | 不带 `:任务` = 批次级全部 |
| `sched cancel <批次>[:任务]` | 取消（转发 daemon 组级 kill） | `--yes` |
| `sched resubmit <批次>:<任务>` | 新版本重新提交 | 批次级另支持 `--failed` / `--all` / `--dry-run` |
| `sched history [批次]` | 历史查询 | `--json` / `--limit` / `--status done,failed` / `--project` |
| `sched markers` | 批次终态一览 | |
| `sched discard <批次>` / `sched clean <批次>` | 退役旧批次 / 清 skip 产物并重排 | `--yes`；约束见 reference |
| `sched incidents [id]` | OOM/硬件事故快照 | `--json` / `--job ID` / `--gpu N` |
| `sched list-gpus` | GPU 状态/显存 | |
| `sched gpu-set-mem/ok/ignore/free` | 卡管理 | 未知 idx 会报错 |
| `sched config get/set/reload` | 配置读取、补丁写入、热更 | `set -f patch.json --yes` |
| `sched project list` | 项目配额、优先级、亲和与用量 | `gpu_quota=0` 显示为 `∞` |
| `sched notify-inbox` | 列批次终态通知事件（agent 检查点） | `--all` 含已确认 / `--json` |
| `sched notify-ack <文件>` | 确认通知事件（rename `.acked`，7 天后自动清理） | |
| `sched notify-test` | 发测试通知验证 config.notify 各渠道 | |

### 状态语义速查

- 任务：`pending → running → done / failed / blocked / cancelled / timed_out / interrupted / skip`（skip = 产物指纹命中，成功等价终态）
- 批次：依赖未满足为 `queued`；解锁后 `active`；全部成功终态 → `done`；任一失败终态 → `blocked`；`blocked/queued` 可人工退役为 `discarded`
- 卡：`free → assigned → releasing → free`；外部 compute PID 或 compute/topology/utilization 任一探测不确定会立即 `unmanaged`；仅 compute 列表完整且为空、utilization 可读且 >0 的信号连续 3 tick 才确认，确认期间虽显示 free 但禁止派发；`unmanaged` 连续 2 次干净采样自动恢复；`quarantined` 需 `gpu-ok` 解除

### 项目配额与优先级

- `projects[P].gpu_quota` 省略或设为 `0` 表示无限制；正整数限制并发 running GPU job 数，不是物理卡数。共享 job 各计 1，CPU-only 不计。
- 当前没有项目级“禁止 GPU”开关；不要把 `gpu_quota:0` 或空 affinity 当成禁用。该后续开发项记录在 `docs/next-development.md`。
- 派发顺序为项目 priority 降序、批次 priority 降序、同值 FIFO；数值越大越先考虑。优先级不抢占，高优候选暂不可运行时低优候选可补位。
- `gpu_affinity_hard` 只限制本项目的候选卡，不反向保留 GPU；独占隔离要求所有竞争项目使用互不重叠的硬亲和集合。

### 通知与检查点

批次终态 marker：`{SCHED_STATE}/<node>/markers/<批次名>.done|.blocked`

**通知快速入门（3 步启用）**：

1. **配置 config.json**（手动或 `sched init` 交互引导）：
   ```json
   "notify": {
     "on": ["batch_done", "batch_blocked"],
     "file": {"enabled": true}
   }
   ```

2. **验证**：`sched notify-test` → 应输出 `ok: file -> .../notify_inbox/...`

3. **agent 使用**：每次被唤醒先 `sched notify-inbox` 查未读事件，处理后 `sched notify-ack <文件>` 确认

**可用渠道**：
- `file`（推荐）：事件 JSON 写 inbox，agent `ls + Read` 即可
- `email`：需配置 SMTP（`notify.email.smtp_host/port/user/to`，密码走 `password_env` 环境变量）
- `command`：调用用户脚本（事件 JSON 走 stdin），示例见 `sched/scripts/notify_*.sh`

notify_inbox：`{SCHED_STATE}/<node>/notify_inbox/*.json`——批次终态事件落盘（需 config.json 配 `notify.file` 渠道），agent 每次被唤醒先 `sched notify-inbox` 查未读事件再开工，处理后 `sched notify-ack` 确认
- command 推渠道（可选）：config.json 配 `notify.command` 指向用户脚本（事件 JSON 走 stdin），批次终态即唤醒 agent；示例 `sched/scripts/notify_tmux_example.sh`（tmux 注入）/ `notify_headless_example.sh`（无头调用，自动检测 claude/kimi/pi CLI）

### 参考文档

- API、配置与命令：`docs/reference.md`
- 尚未实现的下一步开发项：`docs/next-development.md`
