# sched

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-%E2%89%A53.10-blue)

[English](README.md) | **中文**

**sched**（包名 `gsched`）是一个面向多卡计算节点的 GPU/CPU 批量任务调度器。它用"单个 daemon + 纯 CLI 驱动"替代"screen 里 nohup 一把梭"的工作流，提供可重入、可断点续跑、失败自动诊断的批量执行层。

零第三方依赖：纯 Python 标准库（`>= 3.10`）。

## 功能特性

- **批次语义** — 一个 `batch.json` 提交 N 个任务；任一任务进入失败终态即阻塞批次等待人工处理，绝不静默吞错。
- **产物指纹与 SKIP** — 任务声明产物路径及校验规则（如 `min_bytes`）；重提批次时，产物已就绪的任务自动跳过。长流水线断点续跑，不重复训练。
- **每卡一任务为默认语义** — 需要更高利用率时，可通过声明式 `vram`/CPU 配额做 co-location 共享装箱；自动识别外部占用（`unmanaged`），支持人工隔离（`quarantined`）。
- **多项目模式（B11c）** — 单 daemon 上的每项目 GPU 配额、优先级与硬亲和隔离。硬亲和意味着项目任务只落其专属卡——不外借、不被邻居挤占导致 OOM。未注册 `project` 的提交直接拒绝。
- **Sweep 参数矩阵** — 任务内声明 `sweep` 参数数组，笛卡尔积展开为多个作业，可用 `max_parallel` 限并发。
- **失败诊断** — `sched diag <批次>:<任务>` 一步给出状态 + 完整命令 + git rev + 日志尾部。
- **通知** — 批次终态事件推送 file inbox / email / 用户脚本（webhook 预留）。
- **git 指纹** — 每个任务记录提交时的 `git_rev`；retry/resubmit 复用旧 spec 时警告代码漂移。

## 安装

```bash
# 方式 A: pip 安装 (console script)
pip install .

# 方式 B: 包装脚本 + PYTHONPATH (生产用法, 不污染环境)
#   ~/bin/sched:
#!/bin/bash
export SCHED_STATE="${SCHED_STATE:-$HOME/.sched}"
export PYTHONPATH="/path/to/sched-repo${PYTHONPATH:+:$PYTHONPATH}"
exec python3 -m gsched.cli "$@"
```

初始化配置（交互引导生成 `~/.sched/config.json`）：

```bash
sched init
```

最小配置示例：

```json
{
  "schema_version": 1,
  "node": "compute-01",
  "state_dir": "/home/user/.sched",
  "gpus": [0, 1, 2, 3],
  "venvs": {"dl": "/home/user/venvs/dl/bin/python"},
  "co_locate": true,
  "co_locate_safety": 0.7,
  "notify": {"on": ["batch_done", "batch_blocked"], "file": {"enabled": true}},
  "projects": {
    "vision": {
      "root": "/home/user/repos/vision", "git": true,
      "gpu_affinity": [0, 1], "gpu_quota": 2,
      "priority": 10, "gpu_affinity_hard": true
    },
    "nlp": {
      "root": "/home/user/repos/nlp", "git": true,
      "gpu_affinity": [2, 3], "gpu_quota": 2,
      "priority": 5, "gpu_affinity_hard": true
    }
  }
}
```

配置 `projects` 后，所有提交必须携带 `--project <名称>`；派发顺序为 `(项目优先级降序, 批次优先级降序)`，且每个项目不会超出自身配额。

## 快速开始

```bash
# 1. 预览 (只读; 产物已就绪的任务会标 SKIP)
sched submit batch.json --dry-run --project vision

# 2. 提交
sched submit batch.json --project vision

# 3. 启动 daemon (必须在计算节点上; 查询类命令登录节点即可)
sched daemon start

# 4. 观测
sched status                # 三视图: 批次 / 任务 / GPU+CPU
sched status --project nlp  # 按项目过滤
sched log <批次>:<任务> -f   # 跟踪任务日志

# 5. 失败排查 (出问题先跑这个)
sched diag <批次>:<任务>

# 6. 解锁 / 重跑
sched retry <批次>            # 解锁全部失败终态任务 (同 spec)
sched resubmit <批次>:<任务>  # 新版本排队尾 (同 spec)
```

### 单条命令一次性提交

```bash
sched run --project vision --gpus 1 -- python train.py --seed 42   # 占 1 卡
sched run --project vision --cpu-only -- python prep_data.py       # CPU 任务
```

## batch.json 格式

```jsonc
{
  "name": "my_batch",                       // 批次名 (id 自动追加时间戳)
  "mode": "mix",                            // 当前唯一实现的批次模式
  "priority": 5,                            // 项目内批次优先级 (可选)
  "cwd": "{ROOT}",                          // 工作目录, 支持 {ROOT}/{VENV:key} 模板
  "env": {"NN_NO_CUDNN": "1"},              // 批次级环境变量 (可选)
  "depends_on": ["other_batch_name"],       // 上游依赖, 上游 done 前挂起 (可选)
  "tasks": [
    {
      "id": "train_s42",
      "stages": [                           // 多阶段顺序执行, 任一阶段失败即任务失败
        {
          "cmd": ["{VENV:dl}", "train.py", "--seed", "42"],
          "artifacts": {                    // 阶段产物指纹 (命中则 SKIP)
            "ckpt": {"path": "{ROOT}/out/checkpoint.pt", "rule": "min_bytes", "min_bytes": 100000}
          }
        },
        {"cmd": ["{VENV:dl}", "infer.py"]}
      ],
      "sweep": {                            // 可选: 展开为多个作业
        "over": {"seed": [42, 43, 44]},
        "max_parallel": 2                   // 展开作业并发上限
      },
      "duration_min": 50,                   // 预估时长 (超时告警用)
      "max_retry": 1,                       // 自动重试次数 (0 = 失败即终态)
      "resources": {"gpu": 1, "vram": 8.0}  // vram 声明用于 co-location 装箱
    }
  ]
}
```

模板变量：`{ROOT}` = project root；`{VENV:<key>}` = config `venvs` 中的解释器。

## 状态机

```text
任务:  pending → running → done / failed / blocked / cancelled / timed_out / interrupted
       pending → skip                                    (产物指纹命中, 与成功等价)
批次:  active → done          (全部成功终态)
       active → blocked       (任一失败终态, 等待 retry/resubmit)
卡:    free ⇄ assigned ⇄ releasing;  外部占用 → unmanaged;  需人工 → quarantined
```

## 自动化脚本建议

CLI 为可靠的脚本化使用而设计：

- 脚本解析输出一律用 `--json`（`status --json`、`submit --dry-run --json`）。
- 破坏性命令需 `--yes`（`cancel`、`gpu-free`）；缺 `--yes` 返回码 1 表示"未确认"而非"失败"。
- daemon 生命周期命令须在计算节点执行；查询类命令登录节点同样可用（共享 state 目录）。
- 不要直接改 `state.db` —— 它采用 WAL 并发协议，临时手写 SQL 属反模式。

## 测试

```bash
bash tests/run_probes_accept.sh          # probe 探针语义
bash tests/run_colocate_accept.sh        # co-location 装箱
bash tests/run_gpu_mem_accept.sh         # 显存管理
bash tests/run_cancel_forward_accept.sh  # cancel 转发 kill 信号
bash tests/run_daemon_heartbeat_accept.sh
bash tests/run_notify_accept.sh          # 通知渠道
bash tests/run_ux_accept.sh              # CLI UX
# ... 完整套件见 tests/
```

## License

MIT — 见 [LICENSE](LICENSE)。
