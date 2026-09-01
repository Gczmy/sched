# sched

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-%E2%89%A53.10-blue)

[English](README.md) | **中文**

**sched**（包名 `gsched`）是一个面向多卡计算节点的 GPU/CPU 批量任务调度器。它用"单个 daemon + 纯 CLI 驱动"替代"screen 里 nohup 一把梭"的工作流，提供可重入、可断点续跑、失败自动诊断的批量执行层。

零第三方依赖：纯 Python 标准库（`>= 3.10`）。

## 功能特性

- **批次语义** — 一个 `batch.json` 提交 N 个任务；任一任务进入失败终态即阻塞批次等待人工处理，绝不静默吞错。
- **产物指纹与 SKIP** — 任务声明产物路径及校验规则（如 `min_bytes`）；只有 producer 指纹与产物仍匹配时才复用 job 私有 stage checkpoint。stage 成功返回后先校验自身产物再提交 checkpoint，任务最终收敛时再次校验任务级与全部 stage 产物。`force_rerun` 同时绕过任务与 stage 跳过。
- **每卡一任务为默认语义** — 需要更高利用率时，可通过声明式 `gpu_share` + `vram_gib`/CPU 配额做 co-location 共享装箱；自动识别外部占用（`unmanaged`），对无 PID 的利用率底噪防抖，并在健康探测连续失败时自动隔离为 `quarantined`，恢复后用 `gpu-ok` 解除。
- **多项目模式（B11c）** — 单 daemon 上按项目限制 GPU job 并发、排序与选卡。硬亲和只限制该项目任务可选择的 GPU，并不会反向阻止其他项目使用这些卡；需要独占隔离时，所有竞争项目都应配置互不重叠的硬亲和集合。未注册 `project` 的提交直接拒绝。
- **Sweep 参数矩阵** — 在批次级声明 `sweep.matrix`，其笛卡尔积展开任务模板，可用 `max_parallel` 限并发。
- **失败诊断** — `sched diag <批次完整ID或名称>:<任务>` 一步给出状态 + 完整命令 + git rev + 日志尾部。
- **通知** — 批次终态事件推送 file inbox / email / 用户脚本；webhook 仅预留，尚未实现。
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

初始化配置（交互引导默认生成 `~/.sched/config.json`；也可由 `--config`、
`SCHED_CONFIG` 或 `SCHED_STATE` 指定其他 bootstrap 路径）：

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

每份 batch JSON 都必须包含已注册在 `config.projects` 中的
`"project": "<名称>"`（`sched run` 使用 `--project`）。
`projects.<名称>.gpu_quota` 是非负整数：省略或设为 `0` 都表示**无限制**，
不是“0 卡”。正整数限制该项目同时 `running` 且已分配 GPU 的 job 数，而不是
不同物理卡数量；共享装箱的每个 job 分别计 1，CPU-only job 不计入。

项目和批次 `priority` 均默认为 `0`，可使用任意整数。ready job 依次按
项目 priority 降序、批次 priority 降序、入队顺序 FIFO 考虑。优先级不抢占：
已经运行的低优任务不会被驱逐；高优候选暂时不满足资源条件时，后续可运行任务可以补位。

## 快速开始

```bash
# 1. 预览 (只读; 产物已就绪的任务会标 SKIP)
sched submit batch.json --dry-run

# 2. 确认 daemon 健康（以下两条必须在计算节点执行）
sched daemon check
sched daemon start

# 3. 提交（推荐在登录/网关节点执行；daemon 从 inbox 收编）
sched submit batch.json
sched verify BATCH_ID   # 下一 tick 后确认；尚未收编时稍后重试

# 4. 观测（查询类命令登录节点即可）
sched status                # 三视图: 批次 / 任务 / GPU+CPU
sched status --project nlp  # 按项目过滤
sched log <批次完整ID或名称>:<任务> -f  # 先精确 ID，否则同名最新批次

# 5. 失败排查 (出问题先跑这个)
sched diag <批次完整ID或名称>:<任务>

# 6. 解锁 / 重跑
sched retry <批次完整ID或名称>     # 解锁全部失败终态任务 (同 spec)
sched resubmit <批次完整ID或名称>:<任务>  # 新版本排队尾 (同 spec)
```

### 单条命令一次性提交（仅计算节点）

```bash
sched run --project vision --gpus 1 -- python train.py --seed 42   # 占 1 卡
sched run --project vision --cpu-only -- python prep_data.py       # CPU 任务
```

`sched run` 是直接写计算节点状态的入口，不走网关提交 inbox。当前只支持一张 GPU：
省略 `--gpus` 或使用 `--gpus 1`；零 GPU 任务必须使用 `--cpu-only`。当前 parser 会把
未配合 `--cpu-only` 的非正 `--gpus` 当成省略参数，最终仍按默认一张 GPU 入队，因此
不要使用 `0` 或负数。快捷批次 priority 固定为默认值 `0`。未显式指定时，它使用配置中的
第一个 venv，并以 `{ROOT}`（即 `default_project` 根目录）为工作目录；`--project`
不会改变 `{ROOT}`。若要进入其他项目根目录，应显式传
`--cwd '{PROJECT:nlp}'`（把 `nlp` 替换为所选项目）。当前 `run --dry-run` 会在项目
成员校验前返回，所以预览成功不能证明 `--project` 已注册；真实提交仍会执行该校验。
它也不同于可无状态执行的 `submit --dry-run`：当前 run 预览仍要求已有且可读的 state DB。

## batch.json 格式

```jsonc
{
  "name": "my_batch",                       // 1..128 位安全 ASCII 标识符
  "project": "vision",                      // 必填, 且须存在于 config.projects
  "mode": "mix",                            // 当前唯一实现的批次模式
  "priority": 5,                            // 整数越大越先考虑, 默认 0
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

## 状态机

```text
任务:  pending → running → done / failed / blocked / cancelled / timed_out / interrupted
       pending → skip                                    (产物指纹命中, 与成功等价)
批次:  queued → active → done / blocked
       blocked → active                                  (retry 或 resubmit)
       done → active                                     (resubmit，或 clean 确有最新 skip 重排)
       blocked / queued → discarded                      (人工退役)
卡:    free → assigned → releasing → free; 外部占用 → unmanaged; 连续健康异常 → quarantined
```

物理占用探测采用 fail-closed。发现 compute PID，或 compute/topology/utilization 任一探测结果不确定时，
空闲卡会在首次采样立即转为 `unmanaged`。只有“compute 进程列表完整且为空、但利用率
大于 0”会等待连续 3 个 daemon tick 确认；确认期间注册表仍可能显示 `free`，但该卡已从
所有派发路径临时排除，任一干净采样会清零计数。`unmanaged` 连续 2 次干净采样后
自动回到 `free`；`quarantined` 仍须执行 `sched gpu-ok` 人工解除。

## 自动化脚本建议

CLI 为可靠的脚本化使用而设计：

- 脚本解析一律使用 `--json`（`status --json`、`history --json`、`submit --dry-run --json`）。`status` 固定 `schema_version:1`，limit 缺省 200、钳制 1..1000；先有界选择当前态优先/较新的批次，只返回这些批次的 task 最新版本，并分别报告 `truncated.batches`/`truncated.jobs`。任务使用 `batch_id` + `batch_name`，状态使用 canonical `status` + `wait_reason`。`history` 固定 `schema_version:1`，limit 缺省 50、钳制 1..200，并有独立 `truncated`。
- 任务类命令必须使用 `<批次完整ID或名称>:<任务>`：完整 batch id 精确匹配优先，否则名称解析为同名最新批次；裸 task id 无效。
- 破坏性命令需 `--yes`（`cancel`、`discard`、`clean`、`config set`、`gpu-free`）；缺 `--yes` 返回码 1 表示“未确认”而非“失败”。
- 数据库查询连接使用稳定的私有 DB/WAL 快照，不直接通过 SQLite 打开 NFS 源库。在 `config.node` 本机，仅当首次初始化、迁移或修复确有必要时，前置步骤才会以 writer 打开源库；实际查询仍在快照上使用 `mode=ro + query_only`。异机查询绝不初始化或迁移源库。快照获取有界重试，无法取得稳定、受支持的 WAL 快照时 fail-closed。纯文件查询 `markers`、`notify-inbox`、`daemon status` 以及 `config get` 不打开数据库。
- 在 `config.node` 之外，`sched submit` 会持久写入 `submit_inbox` 交由 daemon 消费；“已投递”不等于“已入队”，须用 `sched verify` 确认。其他 mutation（包括 `sched run`）默认拒绝；daemon `start`、`stop`、`check` 也仅限计算节点。`SCHED_ALLOW_FOREIGN_WRITE=1` 是显式安全覆盖，不是正常网关流程。
- 不要直接改 `state.db` —— 它采用 WAL 并发协议，临时手写 SQL 属反模式。

## 已知限制与下一步开发

当前没有项目级“禁止 GPU”设置。`gpu_quota: 0` 必须保持向后兼容，继续表示无限制；
`resources.gpu: 0` 和 `sched run --cpu-only` 只能把单个任务声明为 CPU-only。
计划行为与验收标准记录在 [下一步开发清单](docs/next-development.md)中。

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
