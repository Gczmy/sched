# sched — GPU 任务调度器 (gsched)

统一任务调度框架：**每卡一任务 / 批次语义 / 产物指纹 / co-location 共享装箱 / 可观测性 / 通知**。
零第三方依赖，纯 Python 标准库（`>=3.10`），CLI 入口 `gsched.cli:main`。

设计初衷：在多卡 SLURM 计算节点上替代"screen 里 nohup 一把梭"，提供可重入、
可断点续跑（产物已就绪自动 SKIP）、失败自动诊断的批量任务执行层。

> 本仓库自 `Gczmy/veighna-trade` 的 `sched/` 子目录经 `git subtree split` 迁出，
> 保留完整提交历史。

---

## 特性

- **批次语义**：一个 `batch.json` 提交 N 个任务；任一任务进入失败终态 → 批次 `blocked`
  等人工（`retry`/`resubmit` 解锁后自动回 `active`），不会静默吞错
- **产物指纹 SKIP**：任务声明产物路径 + 校验规则（如 `min_bytes`），重提批次时
  产物已就绪的任务自动跳过 —— 断点续跑、防重复训练
- **GPU 分配**：每卡一任务为默认语义；支持声明式显存 (`vram`) / CPU 配额做
  co-location 共享装箱；外部占用识别（`unmanaged`）、隔离（`quarantined`）
- **失败诊断**：`sched diag <批次>:<任务>` 一步给出 状态 + 完整命令 + git rev + 日志尾部
- **通知**：批次终态事件推 file inbox / email / 用户脚本（webhook 预留）；
  agent 检查点协议 `notify-inbox` → 处理 → `notify-ack`
- **git 指纹**：任务记录提交时的 `git_rev`；retry/resubmit 复用旧 spec 时警告代码漂移

## 安装

零依赖，两种方式：

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

最小 config 示例（见 `~/.sched/config.json`）：

```json
{
  "schema_version": 1,
  "node": "ambiorix",
  "state_dir": "/home/USER/.sched",
  "gpus": [0, 1, 2, 3],
  "projects": {"proj": {"root": "/path/to/repo", "git": true}},
  "default_project": "proj",
  "venvs": {"k": "/path/to/venv/bin/python"},
  "co_locate": true,
  "co_locate_safety": 0.7,
  "co_locate_max_jobs": 4,
  "notify": {"on": ["batch_done", "batch_blocked"],
             "file": {"enabled": true}}
}
```

## 快速开始

```bash
# 1. 预览 (只读, 产物已就绪的任务会标 SKIP)
sched submit batch.json --dry-run

# 2. 提交
sched submit batch.json

# 3. 启动 daemon (必须在计算节点; 查询类命令登录节点即可)
sched daemon start

# 4. 观测
sched status                # 三视图: 批次 / 任务 / GPU+CPU
sched status --detail       # 含耗时与版本
sched log <批次>:<任务> -f   # 跟踪任务日志

# 5. 失败排查 (排雷首选)
sched diag <批次>:<任务>

# 6. 解锁 / 重跑
sched retry <批次>          # 批次级: 所有失败终态任务解锁重跑 (同 spec)
sched resubmit <批次>:<任务> # 新版本排队尾 (同 spec)
```

### 单任务一行提交

```bash
sched run --gpus 1 -- python train.py --seed 42     # 占 1 卡
sched run --cpu-only -- python prep_data.py         # CPU 任务
```

## batch.json 格式

```jsonc
{
  "name": "my_batch",                       // 批次名 (id 自动追加时间戳)
  "mode": "mix",                            // mix | gpu | cpu
  "cwd": "{ROOT}",                          // 工作目录, 支持 {ROOT}/{VENV:k} 模板
  "env": {"NN_NO_CUDNN": "1"},              // 批次级环境变量 (可选)
  "depends_on": ["other_batch_name"],       // 上游依赖, 上游未 done 则挂起 (可选)
  "tasks": [
    {
      "id": "train_s42",
      "stages": [                           // 多阶段顺序执行, 任一阶段失败即任务失败
        {
          "cmd": ["{VENV:k}", "nn/train_nn.py", "--seed", "42", "..."],
          "artifacts": {                    // 阶段产物指纹 (命中则 SKIP)
            "ckpt": {"path": "{ROOT}/out/checkpoint.pt", "rule": "min_bytes", "min_bytes": 100000}
          }
        },
        {"cmd": ["{VENV:k}", "nn/infer_nn.py", "..."]}
      ],
      "duration_min": 50,                   // 预估时长 (超时告警用)
      "max_retry": 1,                       // 自动重试次数 (0 = 失败即终态)
      "resources": {"gpu": 1, "vram": 8.0}  // vram 声明用于 co-location 装箱
    }
  ]
}
```

模板变量：`{ROOT}` = project root、`{VENV:<key>}` = config `venvs` 里的解释器。

## 状态机

```
任务:  pending → running → done / failed / blocked / cancelled / timed_out / interrupted
       pending → skip                                    (产物指纹命中, 成功等价)
批次:  active → done          (全部成功终态)
       active → blocked       (任一失败终态, 等人工 retry/resubmit)
卡:    free ⇄ assigned ⇄ releasing;  外部占用 → unmanaged;  需人工 → quarantined
```

## agent 操作契约

调度器保持"无意识"（不含任何 agent 逻辑），agent 只走 CLI：

- **禁止直接改 state.db**（WAL 并发协议，手工 SQL 属反模式）
- 脚本解析输出一律用 `--json`（`status --json`、`submit --dry-run --json`）
- 破坏性命令需 `--yes`（`cancel`、`gpu-free`）；缺 `--yes` 返回码 1 是未确认不是失败
- daemon 生命周期在计算节点执行；查询类命令登录节点可用（共享 state）
- agent 检查点：每次唤醒先 `sched notify-inbox` 查未读批次终态事件，处理后 `sched notify-ack`

完整契约见 [AGENTS.md](AGENTS.md)。

## 验收测试

```bash
bash tests/run_probes_accept.sh          # probe 探针语义
bash tests/run_colocate_accept.sh        # co-location 装箱
bash tests/run_gpu_mem_accept.sh         # 显存管理
bash tests/run_cancel_forward_accept.sh  # cancel 转发 kill
bash tests/run_daemon_heartbeat_accept.sh
bash tests/run_notify_accept.sh          # 通知渠道
bash tests/run_ux_accept.sh              # CLI UX
# ... 其余见 tests/
```

## 参考文档

历史设计文档保留在上游仓库 `Gczmy/veighna-trade`：

- `docs/04-scheduler/scheduler_design.md` — 设计全案
- `docs/04-scheduler/scheduler_decisions.md` — 定案记录（含 PEP 701 事故、PID namespace 等）
- `docs/archive/sched_code_review_2026-08-19.md` — 代码审查报告

## License

内部工具，随上游项目分发。
