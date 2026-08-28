# sched 使用参考（权威版）

> 面向 AI 代理与用户的**功能与命令权威查阅文档**。改调度器行为时同步更新本文件。
> 版本基准：f424db8+（2026-08-24）。发现本文档与实际行为不符 = 调度器 bug，请报告。

---

## 1. 心智模型

```
批次实例 (batch, 提交时生成唯一 id = 名字-时间戳)
  └── 任务 (task, id + version)
        └── 作业 (job, version 对应的执行记录)
```

- **每次 `sched submit` 都生成新的批次实例**（即使同名）。同名旧实例不会被覆盖，
  会以 `[blocked]` 等状态留在列表里 —— 用 `sched discard` 退役，忽略即可。
- **项目隔离**：提交必须带 `"project"`；任务只落该项目的亲和卡（hard=true 不外借）。
- **共享装箱三要素**（想单卡多任务必读）：
  ```json
  "resources": {
      "gpu_share": true,      // ← 缺省 false=独占整卡！共卡必须显式声明
      "vram_gib": 0.5,        // ← 声明峰值显存 (共享任务必填)，装箱按此记账
      "profile_key": "..."    // ← 可选: 调度器学习的历史实测峰值, 自动取 max(声明, 实测)
  }
  ```
  ⚠️ 最常见错误：写了 vram_gib 但忘写 gpu_share → 任务独占整卡，单卡单任务。
- **指纹与重跑**：指纹 = cmd + git rev + dirty-tree + runtime。产物有效且指纹匹配 → SKIP。
  改代码后 **git commit 再 resubmit**（未提交改动也会触发重跑）；强制重跑见配方 R4/R5。

---

## 2. batch.json 字段参考

### 批次级

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | str | ✅ | 批次名（id 自动追加时间戳）|
| `project` | str | ✅ | 项目名，必须在 config.projects 注册 |
| `mode` | str | ✗ | mix(缺省) \| gpu \| cpu |
| `priority` | int | ✗ | 项目内派发优先级，大者先 |
| `cwd` | str | ✗ | 缺省 `{ROOT}`；支持 `{VENV:key}` 模板 |
| `env` | obj | ✗ | 环境变量映射（最终覆盖，优先级高于自动注入）|
| `depends_on` | [str] | ✗ | 上游批次名数组，上游 done 前挂起 |
| `force_rerun` | bool | ✗ | true = 全部任务跳过 SKIP 判定强制重跑 |
| `notify` | bool/obj | ✗ | 覆盖通知配置 |
| `sweep` | obj | ✗ | `{matrix:{参数:[值]},max_parallel:N}` 笛卡尔积展开 |

### 任务级

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `id` | str | ✅ | 任务名（批内唯一）|
| `cmd` | [str] | ✅* | 自由格式命令；`{VENV:x}`/`{ROOT}` 模板可出现在任意 token |
| `stages` | [obj] | ✅* | 多阶段替代 cmd：`[{cmd,artifacts}]` 顺序执行，失败即停，已成功 stage 断点续跑 |
| `cwd` | str | ✗ | 任务级目录覆盖（任意目录，J 类）|
| `env` | obj | ✗ | 任务级环境变量（最终覆盖）|
| `duration_min` | int | 推荐 | 预估时长，超时看门狗 kill |
| `max_retry` | int | ✗ | 自动重试次数（缺省 1；0=失败即 blocked）|
| `resources.gpu` | 0/1 | ✗ | 0=CPU-only；缺省占 1 GPU |
| `resources.gpu_share` | bool | ✗ | true=允许共享装箱（需全局 co_locate 开启）|
| `resources.vram_gib` | num | 共卡必填 | 峰值显存声明（GiB）；独占时用于容量校验 |
| `resources.profile_key` | str | ✗ | 历史实测峰值键，装箱取 max(声明, 实测) |
| `resources.cpus` | int | ✗ | CPU 核数声明 |
| `runtime` | obj | ✗ | 环境声明（cmd[0] 非 {VENV} 时建议声明）：`{conda_env:"名"}` ∥ `{venv_alias:"名"}` ∥ `{prefix:"路径"}` 三选一；参与指纹 |
| `progress_regex` | str | ✗ | 从日志尾部提取进度，status 展示 |
| `artifacts` | obj | ✗ | `{key:{path,rule}}`；规则：存在(缺省)/`"check":"json"`/`min_bytes:N`/`has_key:"键"`/`regex:"模式"`；命中→SKIP |
| `probes` | obj | ✗ | `{fail_on_log,ready_on_log}` 日志门控 |
| `_force_rerun` | 内部 | — | force_rerun 的落库形态，勿手写 |

\* cmd 与 stages 二选一。

### config.json 相关（代理只读，调参报告用户）

| 键 | 说明 |
|---|---|
| `projects[P].colocate / max_jobs` | 项目级共享开关 / 每卡打包密度上限（热更新）|
| `gpus[i].max_jobs` | 异构卡每卡打包上限（热更新）|
| `conda_envs_dirs` | runtime.conda_env 解析目录（热更新）|
| `task_default_env` | 部署级任务环境缺省值 `{k:v}`；batch/task env 可覆盖。典型用途：`{"PYTHONNOUSERSITE":"1"}` 隔离 ~/.local 用户站点污染 |

⚠️ config.json 为双项目共享配置，修改须经用户确认。

---

## 3. 常见场景配方（recipes）

### R1 单卡多任务（共享装箱）

```json
{"id": "t1", "duration_min": 30,
 "resources": {"gpu_share": true, "vram_gib": 0.5},
 "cmd": ["{VENV:k}", "train.py"]}
```
检查清单：全局 co_locate 开启 ✓ → 项目 colocate 未禁用 ✓ → `gpu_share:true` ✓ →
`vram_gib` 已声明且 ≤ safety×容量 ✓ → 单卡多任务生效。
验证：`sched list-gpus` 看 `packed=n/cap`。

### R2 独占整卡

不写 gpu_share 即可（或 `colocate:false` 项目级禁用）。声明 `vram_gib` 可避免被派到小卡。

### R3 改代码后重跑

```bash
git commit -m "fix"                      # 必须 commit（未提交改动也触发重跑）
sched resubmit <batch>:<task>            # 新版本排队尾
```

### R4 强制全部重跑（跳过 SKIP）

batch.json 加 `"force_rerun": true` 后重新 submit；或清指纹：
`sched clean <batch> --yes`（同时删除声明产物）。

### R5 重跑失败/全部任务

```bash
sched retry <batch>                    # 解锁失败终态 -> pending 重跑（同 spec, 批次回 active）
sched resubmit <batch> --failed        # 失败终态任务各生成新版本排队尾
sched resubmit <batch> --all           # 全部任务重跑
sched resubmit <batch> --failed --dry-run   # 预览将重跑的清单
sched resubmit <batch>:<task>          # 单任务精确定位
```
选择建议：改 spec 无效想换实现用 retry（同 spec）；代码已更新想以新代码重试用
resubmit（新指纹）。blocked 批次 resubmit 后自动回 active。

### R6 退役被取代的旧批次

```bash
sched discard <batch> --yes   # 仅 blocked/queued 且无 running；证据保留
```

### R7 批量取消项目队列

```bash
sched cancel --project selfdist --yes   # ⚠️ 连 blocked 批次的 pending 一并取消
```

### R8 失败排障流程

```bash
sched diag <batch>:<task>     # 一步到位: 状态+命令+git+日志尾+OOM事故快照判读
sched incidents               # 全部 OOM/硬件错误快照
sched log <batch>:<task> -n 50
```

### R9 查进度

status 自动展示；长任务声明 `progress_regex` 更精确。

### R10 新环境准备检查清单（提交前必做）

```bash
# 1. 创建环境后, 用绝对路径安装 (防 pip 解析到别的 env)
/miniconda3/envs/new_env/bin/pip install pkg1 pkg2

# 2. 模拟 daemon 执行方式验证 (无 conda activate), 并确认加载位置
PYTHONNOUSERSITE=1 /miniconda3/envs/new_env/bin/python -c "
import pkg1, numpy
print('import ok')
print('numpy from:', numpy.__file__)"

# 3. 在计算节点上重复步骤 2 (共享 NFS 但解释器/LD 可能不同)

# 4. 全部通过后再 sched submit; 提交时若见
#    "检测到用户站点包" 提示, 说明 ~/.local 可能有遮蔽风险
```
说明：daemon 启动任务的 env 构建顺序 = 继承 → 剥离 conda 污染键 → 注入
runtime/B13 关键子集 → task_default_env 缺省值 → batch/task env 覆盖。
若部署配置了 PYTHONNOUSERSITE=1，用户站点(~/.local)自动隔离。

---

## 4. CLI 命令大全

| 命令 | 说明 | 注意 |
|---|---|---|
| `submit <file> [--dry-run]` | 提交批次 | dry-run 只读预览含 SKIP 预测 |
| `run --project P [--gpus N|--cpu-only] -- cmd...` | 单条命令提交 | |
| `status [--project P] [--detail] [--json]` | 三视图 | |
| `history [batch] [--status] [--limit]` | 历史 | |
| `log <b>:<t> [-f] [-n N]` | 任务日志 | |
| `diag <b>[:t]` | 失败诊断（首选）| |
| `incidents [id] [--json] [--job --gpu]` | OOM 事故快照（--json 供看板/脚本）| |
| `retry <batch>` | 解锁失败终态重跑（同 spec）| |
| `resubmit <b>:<t>` / `<batch> [--failed\|--all] [--dry-run]` | 新版本排队尾；支持批次级批量 | discarded 批次守卫；blocked 自动回 active |
| `cancel <b>[:t] --yes` / `cancel --project P --yes` | 取消 | 后者连带 blocked 批次的 pending |
| `discard <batch> --yes` | 退役 blocked/queued 批次 | 仅拒 running；证据保留 |
| `clean <batch> --yes` | 清指纹+删产物 | 配合强制重跑 |
| `list-gpus` | GPU 视图（packed=n/cap）| |
| `config get` / `config set -f patch --yes` / `config reload` | 配置读取/补丁写入/立即热更 | 冷键拒绝；双项目共享需谨慎 |
| `daemon start/stop/status` | daemon 生命周期 | 计算节点执行；代理禁止操作 |
| `notify-test` / `notify-inbox` / `notify-ack` | 通知测试与检查点 | |
| `project list` | 项目配额/用量 | |

破坏性命令（cancel/gpu-free/discard/config set/clean）缺 `--yes` 时返回码 1 =
未确认而非失败。

---

## 5. 状态机速查

```
任务:  pending → running → done / failed / blocked / cancelled / timed_out / interrupted
       pending → skip (指纹命中, 与成功等价)
批次:  active → done | blocked (等 retry/resubmit) | cancelled
       discarded (退役终态, 不可 retry/resubmit)
GPU:   free ⇄ assigned ⇄ releasing; 外部占用 → unmanaged; 人工 → quarantined
```

## 6. 常见误区 TOP5

1. 忘写 `gpu_share: true` → 单卡单任务（R1 清单）
2. 改代码未 commit 就 resubmit → 正常现象是重跑；若 SKIP 见 R3/R4
3. 共享任务漏写 `vram_gib` → 校验直接拒绝（装箱必须有名数）
4. 同名重复 submit 被拒 → 这是定案行为；旧实例用 `discard` 退役
5. `min_bytes` 设得比真实结果大 → 有效结果被判无效；小 JSON 用 `check:"json"`
