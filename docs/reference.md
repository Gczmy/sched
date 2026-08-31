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
`sched clean <batch-ref> --yes`（删除每个最新 `skip` task spec 声明的产物）。
clean 会清除该批所有版本的指纹，但只把每个 task 的最新 `skip` 版本重新排队；
仅允许 `done/blocked` 终态批次，并且节点上不能有任何 running 任务或其他 `active`
批次（不同批次也可能声明同一路径，在尚无 producer/path ownership 元数据前按
fail-closed 处理）；依赖等待中的 `queued` 批次可以保留。
命令先提交指纹栅栏、再删除
这些 skip 产物，最后才公开可运行状态；最新 `done` 等未重排任务的产物不会删除。
若批次已 `done`，会原子恢复为 `active` 并确保 daemon 运行；daemon 在派发前根据
最新同名批次身份协调旧 `.done` marker。删除期间会持有全局提交栅栏；daemon 的
依赖解锁、终态 skip 产物复核和最终 skip/cleanup/Popen 派发也使用该栅栏，因此不会
在删除窗口启动或复用任务。若 phase 2 失败，`skip + fingerprint=NULL` 是持久
fail-closed 栅栏，不能解锁下游或重新发布 done。不存在的产物
视为已清理，路径策略、权限或 I/O 错误则 fail-closed，批次保持终态（此前已成功
删除的产物不回滚，修复后可重试）。历史版本不会复活。

### R5 重跑失败/全部任务

```bash
sched retry <batch-ref>                    # 解锁失败终态 -> pending 重跑（同 spec, 批次回 active）
sched resubmit <batch-ref> --failed        # 失败终态任务各生成新版本排队尾
sched resubmit <batch-ref> --all           # 全部任务重跑
sched resubmit <batch-ref> --failed --dry-run   # 预览将重跑的清单
sched resubmit <batch-ref>:<task>           # 单任务精确定位
```
选择建议：想按原 spec 重跑失败终态用 retry；想生成带当前代码/runtime 新指纹的新版本用 resubmit。单任务只能在该任务所有版本均为终态时 resubmit；`done` 或 `blocked` 批次都会自动重开为 `active`，终态 marker 由 daemon 在派发前按最新同名实例协调。
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
| `retry <batch-ref>[:task]` | 解锁失败终态重跑（同 spec）| blocked 批次原子回 active；marker 由 daemon 派发前协调；discarded 拒绝 |
| `resubmit <batch-ref>:<task>` / `<batch-ref> [--failed\|--all] [--dry-run]` | 新版本排队尾；支持批次级批量 | discarded/queued 守卫；done/blocked 自动回 active |
| `cancel <batch-ref>[:task] --yes` / `cancel --project P --yes` | 取消 | 后者连带 blocked 批次的 pending |
| `discard <batch-ref> --yes` | 退役 blocked/queued 批次 | 仅拒 running；证据保留 |
| `clean <batch-ref> --yes` | 清全部指纹+删最新 skip spec 产物 | 仅终态批次；仅重排最新 skip；done 自动回 active |
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
- 配置的 `node` 本机执行 `status/task/history/diag/log/list-gpus` 等数据库查询时，先以只读方式核验 schema 版本、必需对象/列与 WAL 文件头；schema 已是当前版本时，查询连接固定使用私有快照上的 `mode=ro + query_only`，不再对源库执行 `journal_mode=WAL`、`BEGIN IMMEDIATE` 或幂等迁移。快照遇到 daemon 写突发时最多重试 8 次并做有界退避（累计 sleep 上限 1.585 秒）；首次建库、旧 schema、非 WAL 库或权限漂移才进入带有界锁重试的 writer 初始化路径。高于当前版本的库在本机与网关查询都 fail-closed。纯文件查询 `markers`、`notify-inbox`、`daemon status` 不检查或打开数据库。
- `request` 只包装 `submit`、`cancel`、`retry`、`resubmit`、`gpu-free`、`gpu-ignore`、`gpu-ok`、`daemon start/stop` 与 `config set`；未列出的 mutation 有意 fail-closed。`gpu-set-mem` 是重启时会被 `config.gpus` 或硬件探测覆盖、且未纳入 revision/CAS 的临时 state/list-gpus 记录，不由 `request` 包装。每次 request 都必须提供非负 `--expect-revision`；无目标的 submit/daemon/config 使用 `0`。task/batch 绑定所属 batch 的 `revision`，其中目标必须使用完整 batch ID（不能用批次名）；GPU 绑定自己的 `revision`，且 GPU 必须额外传 `--expect-assignments-json`（与 status 返回的已排序数组完全一致）。被包装命令使用规范顺序：目标紧跟子命令，选项随后。revision 由 SQLite trigger 在批次状态、task/job 代际与 job 状态变化，以及 GPU 状态/quarantine/ignore 确认/assignment、`gpu_jobs` membership/装箱值变化时递增，所以状态值绕一圈回到原值的 ABA 仍返回 65。task 示例：`sched request retry-42 --expect-kind task --expect-id batch-20260829-000000:train --expect-status failed --expect-version 1 --expect-revision 17 -- retry batch-20260829-000000:train`。GPU 示例：`sched request gpu-42 --expect-kind gpu --expect-id 0 --expect-status assigned --expect-quarantined 0 --expect-revision 9 --expect-assignments-json '[{"job_id":"batch-task-v1","vram_gib":1.5}]' -- gpu-free 0 --yes`。
- 对数据库 mutation，业务写入与 ledger 的 done/code/output 在一个外层事务中原子提交；嵌套 submit/retry/resubmit 的 `commit()` 被外层事务接管，daemon 唤醒只在提交后发生，marker 由 daemon 按数据库权威状态协调。`retry`/`resubmit` 的最终事务、`clean` 的发布重跑阶段以及 `cancel` 的任务分类与写入，都会在读取权威状态前取得 SQLite writer claim，防止 daemon 在状态校验与首个 job/task mutation 之间收敛或派发任务。daemon 的节点重启恢复、接管终态判定与重试发布也使用同一 writer 顺序；已落库的 cancel request 或 `kill_reason=cancelled` 永远优先于自动重试/恢复回队。`daemon start/stop` 与 `config set` 不绑定 SQLite 事务，进程中断留下 started 时返回 75，拒绝猜测外部结果。相同 request-id 和完全相同绑定重放已保存退出码/输出而不重复执行；绑定变化返回 64，前置条件冲突返回 65。stdout/stderr 捕获各自最多 2 MiB；旧 done 输出定期压缩为 tombstone（清空输出但永久保留 argv 绑定与退出码），因此 tombstone 重放保持退出码且不重复 mutation，但不再重放旧文本。
- 本地 `submit` 与网关 inbox payload 都可在 submission gate 外做预览校验和计算指纹，但最终写入前必须在同一 gate 内按最新同名代际重验依赖存在性、完整依赖图、批次 ID 与同名终态，并原子提交批次/任务/job（inbox 同时提交请求回执）。因此并发提交不能分别基于旧快照发布 `A → B`、`B → A` 环，也不会在 `clean` 两阶段之间或 `retry`/`resubmit` 事务中途插入第二个同名非终态批次；反向顺序会在重开旧批次前拒绝已有的同名非终态实例，`clean` 在删产物前和发布重跑前各重验一次。daemon 退出门禁生效时 payload 与 pending 请求保留供恢复后重试。
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

空闲 GPU 的物理探测区分信号来源：发现 compute PID 或 compute/topology 探测不确定时，
仍在首次采样立即转 `unmanaged`（fail-closed）；只有“compute 列表完整且为空、但
`utilization.gpu > 0`”这一类易受驱动底噪影响的信号才做连续 3 次确认。确认期间该卡
保持 `free` 展示但从所有派发路径临时排除，任一干净采样立即清零并恢复派发；连续确认
成立后才持久转 `unmanaged`。因此 1–2% 的单次/短时空闲抖动不会制造状态和日志风暴，
也不存在把确认中的可疑卡分给新任务的窗口。

daemon 只派发 `active` 批次中每个 task 的最新版本；终态/queued 批次和旧版本即使因
历史数据遗留为 pending，也不会在 daemon 重启后重新执行，且不会阻塞批次收敛、
依赖解锁或 idle 退出。候选扫描后仍会在最终 `pending → running` 抢占时原子重验
批次状态与最新版本。旧版本若仍为 `running`，或任一代际还有未决 launch marker，
仍会 fail-closed 阻止批次收敛、依赖解锁及 daemon idle 退出。
每次真实启动会在 `Popen` 前先原子发布并锁定版本化 launch intent；优先使用
no-replace rename，共享文件系统不支持该 capability 时退回同目录 hard-link
no-replace，并验证目标与锁定 FD 的 inode 一致。hard-link 的 link→unlink 崩溃窗
允许 intent 暂时有两个名字，但 identity marker 始终只接受单链接。wrapper 继承该锁，
在执行任何用户命令前把 intent 替换为强进程身份 marker。daemon 崩溃时，只有能够
非阻塞取得原 inode 锁并再次证明路径/nonce 未变化的 abandoned intent 才可删除；
锁仍由 launcher/wrapper 持有或 marker 内容未知时一律保留 running 与资源，禁止二次启动。
周期 tick 会重新接管所有 `running + pgid=NULL` 行；即使某轮已 claim intent 后数据库
收敛失败，下一轮也会消费 cancel 或安全回队并释放 GPU，不依赖 daemon 再次重启。
`blocked` 批次不会因历史 pending/waiting 行被 daemon 自动重开；只有成功提交的
`retry`/`resubmit`/`clean` 能显式将目标批次改回 `active`。
依赖解锁在 submission gate 内的同一 SQLite writer 事务中读取上游最新代际并以
`status='queued'` CAS 发布 `active`；并发的上游 resubmit 与下游 discard
因此都有唯一串行化顺序，不会用陈旧成功快照解锁或复活已退役批次。上游 `done/skip`
都要现场复核该代所有声明产物仍有效，且 `skip` 还要求 fingerprint 非空，才算依赖
成功；共享路径被另一次 clean 删除后，原 producer 或其他 skip 别名都不会继续解锁
下游。尚未发布批次终态时发现名义 done/skip 的产物已失效，会把批次 fail-closed 为
`blocked`，避免留下无法恢复的 active 批次。
SQLite 状态是终态 marker 的权威来源；marker 属派生视图，daemon 只对本轮实际
可派发且确为最新同名实例的批次，在派发前清理旧 `.done/.blocked` marker，不扫描
无界历史，也不会误删较新同名终态批次的 marker。终态写入同样会在提交后持短
submission lock 重验最新同名实例与状态，并先删除相反 suffix，避免旧实例覆盖或
`.done/.blocked` 并存。终态通知的事件快照与 marker 所有权检查共享这一次
submission gate；批次已被 retry/resubmit 重开时不会把 `active` 误报为 blocked。

回滚到不认识 `intent-v1` 的旧 daemon 前，必须先用当前版本成功停止/收敛 daemon，
确认没有 `running + pgid=NULL`、没有 unresolved launch marker，也没有仍持锁的 wrapper；
条件不满足时禁止直接启动旧 daemon，否则旧恢复逻辑可能删除在途 intent 并重复执行任务。

## 6. 常见误区 TOP5

1. 忘写 `gpu_share: true` → 单卡单任务（R1 清单）
2. 改代码未 commit 就 resubmit → 正常现象是重跑；若 SKIP 见 R3/R4
3. 共享任务漏写 `vram_gib` → 校验直接拒绝（装箱必须有名数）
4. 同名重复 submit 会生成新实例；名称引用选择最新实例，要操作旧实例请使用完整 batch id
5. `min_bytes` 设得比真实结果大 → 有效结果被判无效；小 JSON 用 `check:"json"`
