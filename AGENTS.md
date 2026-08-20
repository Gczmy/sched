# sched 开发指南（gsched GPU 任务调度器）

## LLM agent 操作契约（L0）

本目录的调度器保持"无意识"：不含任何 agent 逻辑，只提供标准接口。**agent 操作调度器的唯一正确方式是 `sched` CLI**（入口 `gsched.cli:main`，零依赖纯标准库）。

### 铁律

- **只走 CLI，禁止直接改 state.db / state 目录文件**（WAL 并发有协议，手工 SQL 属反模式，历史事故）。要改状态就找对应子命令；没有对应命令 = 先提需求，不要绕。
- **脚本/agent 解析输出一律用 `--json`**（`status --json`、`submit --dry-run --json`），不要解析人类可读文本。
- 破坏性命令需要 `--yes`（`cancel`、`gpu-free`）；缺 `--yes` 返回码 1 是"未确认"，不是执行失败。
- daemon 生命周期（`sched daemon start/stop`）必须在**计算节点**执行；查询类命令在登录节点可直接用（共享 state，定案 43/44）。

### 子命令速查

| 命令 | 用途 | 关键参数 |
|------|------|---------|
| `sched submit <batch.json>` | 提交批次 | `--dry-run [--json]` 只读预览 |
| `sched run` | 一行提交单任务 | `--cpu-only` / `--gpus N`（N>1 会被拒绝）/ `--dry-run` |
| `sched status [批次]` | 三视图总览 | `--json`（脚本用）、`--detail`（耗时/版本） |
| `sched task <批次>:<任务>` | 单任务详情 | |
| `sched diag <批次>[:任务]` | 失败诊断（状态+命令+git+日志尾部） | 排雷首选，一步到位 |
| `sched log <批次>:<任务>` | 任务日志 | `-n N` 尾部行数、`-f` 跟踪 |
| `sched retry <批次>[:任务]` | 解锁失败终态重跑 | 不带 `:任务` = 批次级全部 |
| `sched cancel <批次>[:任务]` | 取消（转发 daemon 组级 kill） | `--yes` |
| `sched resubmit <批次>:<任务>` | 新版本重新提交 | |
| `sched history [批次]` | 历史查询 | `--limit` / `--status done,failed` |
| `sched markers` | 批次终态一览 | |
| `sched list-gpus` | GPU 状态/显存 | |
| `sched gpu-set-mem/ok/ignore/free` | 卡管理 | 未知 idx 会报错 |
| `sched notify-inbox` | 列批次终态通知事件（agent 检查点） | `--all` 含已确认 / `--json` |
| `sched notify-ack <文件>` | 确认通知事件（rename `.acked`，7 天后自动清理） | |
| `sched notify-test` | 发测试通知验证 config.notify 各渠道 | |

### 状态语义速查

- 任务：`pending → running → done / failed / blocked / cancelled / timed_out / interrupted / skip`（skip = 产物指纹命中，成功等价终态）
- 批次：全部成功终态 → `done`；任一失败终态 → `blocked`（等人工 `retry`/`resubmit` 后自动回 `active`）
- 卡：`free → assigned → releasing → free`；外部占用 → `unmanaged`（空闲后自动恢复）；`quarantined` 需 `gpu-ok` 解除

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

notify_inbox：`{SCHED_STATE}/<node>/notify_inbox/*.json`（已实施，见 `docs/sched_notify_design.md`）——批次终态事件落盘（需 config.json 配 `notify.file` 渠道），agent 每次被唤醒先 `sched notify-inbox` 查未读事件再开工，处理后 `sched notify-ack` 确认
- command 推渠道（可选）：config.json 配 `notify.command` 指向用户脚本（事件 JSON 走 stdin），批次终态即唤醒 agent；示例 `sched/scripts/notify_tmux_example.sh`（tmux 注入）/ `notify_headless_example.sh`（无头调用，自动检测 claude/kimi/pi CLI）

### 参考文档

- 设计：`docs/scheduler_design.md`；定案：`docs/scheduler_decisions.md`
- 代码审查报告：`docs/sched_code_review_2026-08-19.md`（H/C/M/P 系列修复状态）
- agent 接口评估（MCP 路线图）：`docs/sched_mcp_evaluation.md`
