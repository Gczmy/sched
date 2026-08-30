# sched 使用参考（权威版）

> 面向 AI 代理与用户的**功能与命令权威查阅文档**。改调度器行为时同步更新本文件。
> 版本基准：当前 source/tests（2026-08-29）。发现本文档与实际行为不符 = 调度器 bug，请报告。

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
- **指纹、stage checkpoint 与重跑**：指纹覆盖命令、git HEAD、tracked 文件的 staged/unstaged 内容与 runtime；任意 untracked 输出不参与 producer 指纹。stage 仅在产物有效且 job 私有 checkpoint sidecar 的 producer 指纹匹配时跳过；stage 命令返回 0 后必须先通过其完整产物规则才写 sidecar，任务最终收敛时会再次校验任务级与全部 stage 产物。`force_rerun:true` 绕过任务 SKIP 和 stage checkpoint。
  tracked 代码未提交改动也会改变指纹；untracked 源码不会，因此生产代码应纳入版本控制。强制重跑见 R4/R5。
  对任务操作一律使用 `<batch-id-or-name>:<task>`；先精确匹配完整 batch id，否则选同名最新批次，裸 task 无效。

---

## 2. batch.json 字段参考

### 批次级

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | str | ✅ | 批次名（1..128 位安全 ASCII 标识符；id 自动追加时间戳）|
| `project` | str | ✅ | 项目名，必须在 config.projects 注册 |
| `mode` | str | ✗ | `mix`（缺省；当前唯一实现的批次模式）|
| `priority` | int | ✗ | 项目内派发优先级，大者先 |
| `cwd` | str | ✗ | 缺省为该批次的 project root；支持 `{ROOT}`/`{PROJECT:name}`/`{VENV:key}` 模板 |
| `env` | obj | ✗ | 字符串环境变量映射（最终覆盖，优先级高于自动注入）；变量名/值必须安全，shell bootstrap 与动态加载注入变量会被拒绝 |
| `depends_on` | [str] | ✗ | 上游批次名数组；每项同样须为安全标识符，上游 done 前挂起 |
| `force_rerun` | bool | ✗ | true = 全部任务绕过任务 SKIP 与 stage checkpoint |
| `notify` | bool/obj | ✗ | 覆盖通知配置 |
| `sweep` | obj | ✗ | `{matrix:{参数:[值]},max_parallel:N}` 笛卡尔积展开 |

### 任务级

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `id` | str | ✅ | 批内唯一的 1..128 位安全 ASCII 标识符 |
| `cmd` | [str] | ✅* | 自由格式命令；`{VENV:x}`/`{ROOT}` 模板可出现在任意 token |
| `stages` | [obj] | ✅* | 多阶段替代 cmd：`[{cmd,artifacts}]` 顺序执行，失败即停；匹配 producer 指纹的私有 checkpoint 可跳过已成功 stage |
| `cwd` | str | ✗ | 任务级目录覆盖（任意目录，J 类）|
| `env` | obj | ✗ | 任务级安全字符串环境变量（最终覆盖）；与批次级采用同一注入变量拒绝规则 |
| `duration_min` | int | 推荐 | 预估时长，超时看门狗 kill |
| `max_retry` | int | ✗ | 自动重试次数（缺省 1；0=失败即 blocked）|
| `resources.gpu` | 0/1 | ✗ | 0=CPU-only；缺省占 1 GPU |
| `resources.gpu_share` | bool | ✗ | true=允许共享装箱（需全局 co_locate 开启）|
| `resources.vram_gib` | num | 共卡必填 | 峰值显存声明（GiB）；独占时用于容量校验 |
| `resources.profile_key` | str | ✗ | 历史实测峰值键，装箱取 max(声明, 实测) |
| `resources.cpus` | int | ✗ | CPU 核数声明 |
| `runtime` | obj | ✗ | `{conda_env:"名"}` ∥ `{venv_alias:"名"}` ∥ `{prefix:"路径"}` 必须且只能选一个；别名须注册、conda env/prefix 目录须在提交时存在；参与指纹 |
| `progress_regex` | str | ✗ | 从日志尾部提取进度，status 展示 |
| `artifacts` | obj | ✗ | `{key:{path,...}}`；规则：存在(缺省)/`"check":"json"`/`min_bytes:N`/`has_key:"键"`/`regex:"模式"`；内容校验有读取/执行上限，symlink 与特殊文件拒绝；命中→SKIP |
| `probes` | obj | ✗ | 仅任务级 `{fail_on_log,ready_on_log}` 日志门控 |
| `_force_rerun` | 内部 | — | `force_rerun` 的落库字段，输入中不要提供 |

\* cmd 与 stages 二选一。

标识符必须匹配 `[A-Za-z0-9][A-Za-z0-9._-]*`，且不能是 `.`/`..`。已移除或未实现的输入会拒绝：批次级 `gpus`、任务/阶段级 `retry_transform`、阶段级 `probes`；GPU 需求写 `resources.gpu`，probe 只写在任务级。

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
sched resubmit <batch-ref>:<task>        # batch-ref: 完整 id 优先，否则同名最新实例
```

### R4 强制全部重跑（跳过 SKIP）

batch.json 加 `"force_rerun": true` 后重新 submit；或清指纹：
`sched clean <batch-ref> --yes`（同时删除声明产物）。

### R5 重跑失败/全部任务

```bash
sched retry <batch-ref>                    # 解锁失败终态 -> pending 重跑（同 spec, 批次回 active）
sched resubmit <batch-ref> --failed        # 失败终态任务各生成新版本排队尾
sched resubmit <batch-ref> --all           # 全部任务重跑
sched resubmit <batch-ref> --failed --dry-run   # 预览将重跑的清单
sched resubmit <batch-ref>:<task>           # 单任务精确定位
```
选择建议：想按原 spec 重跑失败终态用 retry；想生成带当前代码/runtime 新指纹的新版本用 resubmit。单任务只能在该任务所有版本均为终态时 resubmit；`done` 或 `blocked` 批次都会自动重开为 `active`，并清除旧终态 marker。
历史落库 spec 在 resubmit 时会剥离任务/阶段 `retry_transform` 与阶段级 `probes`；新 batch.json 直接拒绝这些字段。

### R6 退役被取代的旧批次

```bash
sched discard <batch-ref> --yes   # 仅 blocked/queued 且无 running；证据保留
```

### R7 批量取消项目队列

```bash
sched cancel --project selfdist --yes   # ⚠️ 连 blocked 批次的 pending 一并取消
```

### R8 失败排障流程

```bash
sched diag <batch-ref>:<task> # 一步到位: 状态+命令+git+日志尾+OOM事故快照判读
sched incidents               # 全部 OOM/硬件错误快照
sched log <batch-ref>:<task> -n 50
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

下表的 `<batch-ref>` 先精确匹配完整 batch id，否则解析为同名最新批次。

| 命令 | 说明 | 注意 |
|---|---|---|
| `submit <file> [--dry-run]` | 提交批次 | dry-run 只读预览含 SKIP 预测 |
| `run --project P [--gpus N|--cpu-only] -- cmd...` | 单条命令提交 | |
| `status [batch-ref] [--project P] [--detail] [--json] [--limit N] [--cursor TOKEN] [--job-cursor TOKEN]` | 最新版本当前态 | 缺省 200，钳制到 1..1000；`--cursor` 翻批次页，`--job-cursor` 独立翻任务页 |
| `task <batch-ref>:<task> [--json]` | 单任务全部版本详情 | `--json` 输出单一、版本化 JSON 文档 |
| `history [batch-ref] [--status S] [--project P] [--json] [--limit N] [--cursor TOKEN]` | 终态历史（保留各版本） | 缺省 50，钳制到 1..200；cursor 用于 JSON 稳定键集分页 |
| `log <batch-ref>:<task> [-f] [-n N]` | 任务日志 | |
| `diag <batch-ref>[:task]` | 失败诊断（首选）| |
| `incidents [id] [--json] [--job --gpu]` | OOM 事故快照（--json 供看板/脚本）| |
| `retry <batch-ref>[:task]` | 解锁失败终态重跑（同 spec）| blocked 批次原子回 active 并在提交后清 `.blocked` marker；discarded 拒绝 |
| `resubmit <batch-ref>:<task>` / `<batch-ref> [--failed\|--all] [--dry-run]` | 新版本排队尾；支持批次级批量 | discarded/queued 守卫；done/blocked 自动回 active |
| `cancel <batch-ref>[:task] --yes` / `cancel --project P --yes` | 取消 | 后者连带 blocked 批次的 pending |
| `discard <batch-ref> --yes` | 退役 blocked/queued 批次 | 仅拒 running；证据保留 |
| `clean <batch-ref> --yes` | 清指纹+删产物 | 配合强制重跑 |
| `list-gpus` | GPU 视图（packed=n/cap）| |
| `config get` / `config set -f patch --yes` / `config reload` | 配置读取/补丁写入/立即热更 | 冷键拒绝；双项目共享需谨慎 |
| `request <request-id> --expect-revision N [--expect-kind ...] ... -- <mutation>` | 持久化幂等 mutation | revision 必填；request-id 与完整命令/前置条件永久绑定；GPU 还必须绑定完整 assignments JSON |
| `daemon start/stop/status/check` | daemon 生命周期 | start/stop/check 仅计算节点；显式 `SCHED_ALLOW_FOREIGN_WRITE=1` 才可覆盖 |
| `notify-test` / `notify-inbox` / `notify-ack` | 通知测试与检查点 | |
| `project list` | 项目配额/用量 | |

破坏性命令（cancel/gpu-free/discard/config set/clean）缺 `--yes` 时返回码 1 =
未确认而非失败。

### 稳定 JSON 与跨主机读取

- `status --json` 固定 `schema_version:1`，`limit` 缺省 200、钳制 1..1000；顶层含 `batches`、`jobs`、`gpus`、`cpu`、`daemon_health`、`truncated:{batches,jobs}`、批次分页 `next_cursor` 与独立任务分页 `next_job_cursor`。先按当前态优先、再按新旧顺序有界选择批次，任务只来自已返回批次且只含各 task 最新 version，因此每个 `jobs[].batch_id` 都能在 `batches` 中解析。等待态统一为 `"status":"pending"` 与独立 `"wait_reason":"quota"|"dependency"|null`，不要解析人类视图的装饰文本。`batches[].revision` 是整数；`gpus[].revision` 是整数，`gpus[].assignments` 按 `job_id` 排序且每项为 `{"job_id":...,"vram_gib":...}`。只有 `truncated.batches=true` 时才用 `next_cursor`；只有 `truncated.jobs=true` 时才用 `next_job_cursor`。指定一个批次时也可只翻其任务页，不会丢失该批次行。
- `task --json` 固定 `schema_version:1`，输出 `batch_id`、`batch_name`、`batch_revision`、`task` 与 `jobs` 版本时间线；每个版本含状态、运行结果/时间、resources、规范化 spec 与 log 路径。stdout 只含这一份 JSON。
- `history --json` 固定 `schema_version:1`，`limit` 缺省 50、钳制 1..200；顶层含 `history`、`truncated` 与 `next_cursor`，每项含 `batch_id`、`batch_name`、`task`、`status`、`version` 及运行结果/时间。
- 配置的 `node` 之外执行查询时，CLI 不初始化、不迁移、也不写源 DB；无论源目录当时是否存在 WAL/SHM，都先复制出稳定的私有 DB（及存在的 WAL）快照，再以 `mode=ro` 打开。绝不对仍可变化的 live DB 使用 `immutable=1`。`daemon status` 也可跨主机只读；`daemon start/stop/check` 默认拒绝。
- `request` 只包装 `submit`、`cancel`、`retry`、`resubmit`、`gpu-*`、`daemon start/stop` 与 `config set`。每次都必须提供非负 `--expect-revision`；无目标的 submit/daemon/config 使用 `0`。task/batch 绑定所属 batch 的 `revision`，GPU 绑定自己的 `revision`，且 GPU 必须额外传 `--expect-assignments-json`（与 status 返回的已排序数组完全一致）。revision 由 SQLite trigger 在批次状态、task/job 代际与 job 状态变化，以及 GPU 状态/quarantine/assignment、`gpu_jobs` membership/装箱值变化时递增，所以状态值绕一圈回到原值的 ABA 仍返回 65。task 示例：`sched request retry-42 --expect-kind task --expect-id batch-20260829-000000:train --expect-status failed --expect-version 1 --expect-revision 17 -- retry batch-20260829-000000:train`。GPU 示例：`sched request gpu-42 --expect-kind gpu --expect-id 0 --expect-status assigned --expect-quarantined 0 --expect-revision 9 --expect-assignments-json '[{"job_id":"batch-task-v1","vram_gib":1.5}]' -- gpu-free 0 --yes`。
- 对数据库 mutation，业务写入与 ledger 的 done/code/output 在一个外层事务中原子提交；嵌套 submit/retry/resubmit 的 `commit()` 被外层事务接管，daemon 唤醒与 resubmit marker 删除只在提交后发生。`daemon start/stop` 与 `config set` 不绑定 SQLite 事务，进程中断留下 started 时返回 75，拒绝猜测外部结果。相同 request-id 和完全相同绑定重放已保存退出码/输出而不重复执行；绑定变化返回 64，前置条件冲突返回 65。stdout/stderr 捕获各自最多 2 MiB；旧 done 输出定期压缩为 tombstone（清空输出但永久保留 argv 绑定与退出码），因此 tombstone 重放保持退出码且不重复 mutation，但不再重放旧文本。
- state 根目录优先级：`SCHED_STATE` > 已加载的 `config.state_dir` > `~/.sched`。相对 `config.state_dir` 以 bootstrap config 所在目录为基准解析；`node` 必须是安全的单一路径分量。共享 state 上的登录节点查询仍定位 `config.node` 的节点目录。

---

## 5. 状态机速查

```
任务:  pending → running → done / failed / blocked / cancelled / timed_out / interrupted
       pending → skip (指纹命中, 与成功等价)
批次:  active → done | blocked (等 retry/resubmit) | cancelled
       blocked → active (retry 或 resubmit); done → active (resubmit)
       discarded (退役终态, 不可 retry/resubmit)
GPU:   free ⇄ assigned ⇄ releasing; 外部占用 → unmanaged; 人工 → quarantined
```

## 6. 常见误区 TOP5

1. 忘写 `gpu_share: true` → 单卡单任务（R1 清单）
2. 改代码未 commit 就 resubmit → 正常现象是重跑；若 SKIP 见 R3/R4
3. 共享任务漏写 `vram_gib` → 校验直接拒绝（装箱必须有名数）
4. 同名重复 submit 会生成新实例；名称引用选择最新实例，要操作旧实例请使用完整 batch id
5. `min_bytes` 设得比真实结果大 → 有效结果被判无效；小 JSON 用 `check:"json"`
