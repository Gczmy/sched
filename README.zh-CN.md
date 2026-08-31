# sched

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-%E2%89%A53.10-blue)

[English](README.md) | **中文**

**sched**（包名 `gsched`）是一个面向多卡计算节点的 GPU/CPU 批量任务调度器。它用"单个 daemon + 纯 CLI 驱动"替代"screen 里 nohup 一把梭"的工作流，提供可重入、可断点续跑、失败自动诊断的批量执行层。

零第三方依赖：纯 Python 标准库（`>= 3.10`）。

## 功能特性

- **批次语义** — 一个 `batch.json` 提交 N 个任务；任一任务进入失败终态即阻塞批次等待人工处理，绝不静默吞错。
- **产物指纹与 SKIP** — 任务声明产物路径及校验规则（如 `min_bytes`）；只有 producer 指纹与产物仍匹配时才复用 job 私有 stage checkpoint。stage 成功返回后先校验自身产物再提交 checkpoint，任务最终收敛时再次校验任务级与全部 stage 产物。`force_rerun` 同时绕过任务与 stage 跳过。
- **每卡一任务为默认语义** — 需要更高利用率时，可通过声明式 `gpu_share` + `vram_gib`/CPU 配额做 co-location 共享装箱；自动识别外部占用（`unmanaged`），支持人工隔离（`quarantined`）。
- **多项目模式（B11c）** — 单 daemon 上的每项目 GPU 配额、优先级与硬亲和隔离。硬亲和意味着项目任务只落其专属卡——不外借、不被邻居挤占导致 OOM。未注册 `project` 的提交直接拒绝。
- **Sweep 参数矩阵** — 在批次级声明 `sweep.matrix`，其笛卡尔积展开任务模板，可用 `max_parallel` 限并发。
- **失败诊断** — `sched diag <批次完整ID或名称>:<任务>` 一步给出状态 + 完整命令 + git rev + 日志尾部。
- **通知** — 批次终态事件推送 file inbox / email / 用户脚本（webhook 预留）。
- **git 指纹** — 指纹覆盖 HEAD 与 tracked 文件的 staged/unstaged 内容，不纳入任意 untracked 输出；每个任务记录 `git_rev`，retry/resubmit 复用旧 spec 时警告代码漂移。

## 安装

```bash
# 方式 A: pip 安装 (console script)
pip install .

# 方式 B: 包装脚本 + PYTHONPATH (生产用法, 不污染环境)
#   ~/bin/sched:
#!/bin/bash
# export SCHED_STATE="/shared/sched"  # 可选: 显式覆盖 config.state_dir
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
  "user": "user",
  "default_project": "vision",
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

state 根目录优先级：`SCHED_STATE` > 已加载的 `config.state_dir` > `~/.sched`。

配置 `projects` 后，每份 batch JSON 必须包含 `"project": "<名称>"`（`sched run` 才使用 `--project`）；派发顺序为 `(项目优先级降序, 批次优先级降序)`，且每个项目不会超出自身配额。

## 快速开始

```bash
# 1. 预览 (只读; 产物已就绪的任务会标 SKIP)
sched submit batch.json --dry-run

# 2. 提交
sched submit batch.json

# 3. 启动 daemon (必须在计算节点上; 查询类命令登录节点即可)
sched daemon start

# 4. 观测
sched status                # 三视图: 批次 / 任务 / GPU+CPU
sched status --project nlp  # 按项目过滤
sched log <批次完整ID或名称>:<任务> -f  # 先精确 ID，否则同名最新批次

# 5. 失败排查 (出问题先跑这个)
sched diag <批次完整ID或名称>:<任务>

# 6. 解锁 / 重跑
sched retry <批次完整ID或名称>     # 解锁全部失败终态任务 (同 spec)
sched resubmit <批次完整ID或名称>:<任务>  # 新版本排队尾 (同 spec)
```

### 单条命令一次性提交

```bash
sched run --project vision --gpus 1 -- python train.py --seed 42   # 占 1 卡
sched run --project vision --cpu-only -- python prep_data.py       # CPU 任务
```

## batch.json 格式

```jsonc
{
  "name": "my_batch",                       // 1..128 位安全 ASCII 标识符
  "project": "vision",                      // 必填, 且须存在于 config.projects
  "mode": "mix",                            // 缺省；strict 仅限管理员冷 profile
  "priority": 5,                            // 项目内批次优先级 (可选)
  "cwd": "{PROJECT:vision}",                // 支持 {PROJECT:name}/{ROOT}/{VENV:key}
  "env": {"NN_NO_CUDNN": "1"},              // 字符串环境变量 (可选)
  "depends_on": ["other_batch_name"],       // 安全标识符, 上游 done 前挂起
  "sweep": {                                // 批次级笛卡尔积展开
    "matrix": {"seed": [42, 43, 44]},
    "max_parallel": 2
  },
  "tasks": [
    {
      "id": "train_s{seed}",
      "runtime": {"venv_alias": "dl"},       // 三种 runtime 通道须恰选一个且可解析
      "stages": [                           // 多阶段顺序执行, 任一失败即任务失败
        {
          "cmd": ["python", "train.py", "--seed", "{seed}"],
          "artifacts": {                    // min_bytes 校验产物大小
            "ckpt": {"path": "out/checkpoint-{seed}.pt", "min_bytes": 100000}
          }
        },
        {"cmd": ["python", "infer.py"]}
      ],
      "duration_min": 50,                   // 有限正数分钟, 到时硬超时
      "max_retry": 1,                       // 非负整数; 0 = 失败即终态
      "resources": {
        "gpu": 1, "gpu_share": true, "vram_gib": 8.0
      }
    }
  ]
}
```

模板变量：`{ROOT}` = `default_project` 根目录；`{PROJECT:<name>}` = 指定项目根目录；`{VENV:<key>}` = config `venvs` 中的解释器。

批次/任务/依赖标识符必须匹配 `[A-Za-z0-9][A-Za-z0-9._-]*`（且不能是 `.`/`..`）。`runtime` 必须在 `venv_alias`、`conda_env`、`prefix` 中恰选一个，并在提交时解析成功。批次级 `gpus`、任务/阶段级 `retry_transform`、阶段级 `probes` 均拒绝；GPU 需求写 `resources.gpu`，probe 只写任务级。

`mode: "strict"` 是部署级 direct-exec 通道。只有一个管理员冷配置
`config.native_exec_profiles` 项与
`(mode, project, batch_name, task_id, submitted_argv)` 五元组逐项精确匹配时才接受。
任务必须只有一个 `cmd`，首项是规范化绝对可执行路径，effective cwd 等于当前项目根，
resources 必须精确为 CPU-only `{"gpu":0,"cpus":1}`，batch/task `env` 必须为空；
不得声明 `stages`、`sweep`、`runtime`、scheduler artifact skip/cleanup 规则或日志 probe，并须显式设置
`git: false` 和 `max_retry: 0`。`git: false` 用于阻止 reviewed native verifier 之前通过
daemon `PATH` 启动 Git 子进程；code-byte 绑定改由 external verifier 负责。profile 会对全部
mix 入口（包括 `sched run`）保留其 batch name；
该名称第一次 strict 耐久入库即永久消费，不论后来是 done、blocked 还是
discarded，新 submit、retry、resubmit 与自动重放均拒绝。inbox 中同 batch id 的重投
只有在耐久 batch/task/job 不可变绑定逐项精确一致时才幂等；同名预占或持久态漂移会被拒绝。

scheduler 会持久化 profile digest 与 canonical project-root device/inode digest，将二者纳入
fingerprint，并在 running claim 前重新认证。profile registry 及其引用的 project root 均为冷配置，
热变更拒绝。native launch 不继承 daemon 环境，只加入 scheduler-owned identity/control 字段与空的
`CUDA_VISIBLE_DEVICES`，随后绕过 Bash/RC supervisor 直接启动原 argv。只有当前 daemon 持有的
exact `Popen` 退出码可形成成功；daemon 丢失权威后任务直接 blocked，不信任 RC sidecar，也不重放。

该通道目前只是 launcher 基础，**尚未**做到 pathname execution 的 byte/FD exact，也未关闭
reattest 到 `Popen` 的 TOCTOU 窗口。正式执行前仍必须完成 external retained-FD verifier/monitor 与
七字段 attestation。当前 strict schema 还会拒绝全部用户 env，因此专用七字段 poison-overwrite
probe 暂不可运行；其 cold-profile 精确例外须与 external verifier 一并实现。

## 状态机

```text
任务:  pending → running → done / failed / blocked / cancelled / timed_out / interrupted
       pending → skip                                    (产物指纹命中, 与成功等价)
批次:  active → done / blocked
       done / blocked → active                              (终态任务 resubmit)
卡:    free ⇄ assigned ⇄ releasing;  外部占用 → unmanaged;  需人工 → quarantined
```

## 自动化脚本建议

CLI 为可靠的脚本化使用而设计：

- 脚本解析一律使用 `--json`（`status --json`、`history --json`、`submit --dry-run --json`）。`status` 固定 `schema_version:1`，limit 缺省 200、钳制 1..1000；先有界选择当前态优先/较新的批次，只返回这些批次的 task 最新版本，并分别报告 `truncated.batches`/`truncated.jobs`。任务使用 `batch_id` + `batch_name`，状态使用 canonical `status` + `wait_reason`。`history` 固定 `schema_version:1`，limit 缺省 50、钳制 1..200，并有独立 `truncated`。
- 任务类命令必须使用 `<批次完整ID或名称>:<任务>`：完整 batch id 精确匹配优先，否则名称解析为同名最新批次；裸 task id 无效。
- 破坏性命令需 `--yes`（`cancel`、`gpu-free`）；缺 `--yes` 返回码 1 表示"未确认"而非"失败"。
- 在 `config.node` 之外查询时，CLI 不打开可写 SQLite：无论源目录当时是否有 WAL/SHM，都先复制稳定的私有 DB（及存在的 WAL）快照再只读打开；绝不把仍可能变化的 live DB 标为 immutable。daemon `start`、`stop`、`check` 仅限计算节点，除非显式设置 `SCHED_ALLOW_FOREIGN_WRITE=1`；`daemon status` 保持跨主机只读。
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
