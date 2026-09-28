# sched 使用参考（权威版）

> 面向 AI 代理与用户的**功能与命令权威查阅文档**。改调度器行为时同步更新本文件。
> 版本基准：本次提交的 source/tests（文档同步于 2026-09-01；核心调度行为截至 `e9a44d0`）。发现本文档与实际行为不符 = 调度器 bug，请报告。

---

## 1. 心智模型

```
批次实例 (batch, 提交时生成唯一 id = 名字-时间戳)
  └── 任务 (task, id + version)
        └── 作业 (job, version 对应的执行记录)
```

- **每次 `sched submit` 都生成新的批次实例**（即使同名）。同名旧实例不会被覆盖，
  会以 `[blocked]` 等状态留在列表里 —— 用 `sched discard` 退役，忽略即可。
- **项目归属与选卡**：提交必须带 `"project"`；`gpu_affinity_hard=true` 时，该项目任务只从其亲和卡中选卡。硬亲和不会反向为项目保留 GPU；需要项目间独占隔离时，所有竞争项目必须使用互不重叠的硬亲和集合。
- **共享装箱三要素**（想单卡多任务必读）：
  ```json
  "resources": {
      "gpu_share": true,      // ← 缺省 false=独占整卡！共卡必须显式声明
      "vram_gib": 0.5,        // ← 声明峰值显存 (共享任务必填)，装箱按此记账
      "profile_key": "..."    // ← 可选: 调度器学习的历史实测峰值, 自动取 max(声明, 实测)
  }
  ```
  ⚠️ 最常见错误：写了 vram_gib 但忘写 gpu_share → 任务独占整卡，单卡单任务。
- **指纹、stage checkpoint 与重跑**：指纹覆盖命令、真实工作目录、声明的合并环境（`task_default_env < batch.env < task.env`）、产物规则、git HEAD、tracked 文件的 staged/unstaged 内容与 runtime；任意 untracked 输出和未声明的宿主环境不参与 producer 指纹。stage 仅在产物有效且 job 私有 checkpoint sidecar 的 producer 指纹匹配时跳过；stage 命令返回 0 后必须先通过其完整产物规则才写 sidecar，任务最终收敛时会再次校验任务级与全部 stage 产物。`force_rerun:true` 绕过任务 SKIP 和 stage checkpoint。2026-09-07 指纹格式升级后，旧 producer/checkpoint 无法匹配，新提交或重开的任务会重新执行一次；已完成任务不会因此自动入队。
  tracked 代码未提交改动也会改变指纹；untracked 源码不会，因此生产代码应纳入版本控制。强制重跑见 R4/R5。
  对任务操作一律使用 `<batch-id-or-name>:<task>`；先精确匹配完整 batch id，否则选同名最新批次，裸 task 无效。

---

## 2. batch.json 字段参考

### 批次级

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | str | ✅ | 批次名（1..128 位安全 ASCII 标识符；id 自动追加时间戳）|
| `project` | str | ✅ | 项目名，必须在 config.projects 注册 |
| `mode` | str | ✗ | `mix`（缺省）或管理员冷 profile 精确授权的 `strict` |
| `priority` | int | ✗ | 项目内批次优先级，默认 0；整数越大越先考虑 |
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
| `duration_min` | num | 推荐 | 有限正数分钟；超时看门狗 kill |
| `max_retry` | int | ✗ | 自动重试次数（缺省 1；0=失败即 blocked）|
| `resources.gpu` | 0/1 | ✗ | 0=CPU-only；缺省占 1 GPU |
| `resources.gpu_share` | bool | ✗ | true=允许共享装箱（需全局 co_locate 开启）|
| `resources.vram_gib` | num | 共卡必填 | 峰值显存声明（GiB）；独占时用于容量校验 |
| `resources.profile_key` | str | ✗ | 历史实测峰值键，装箱取 max(声明, 实测) |
| `resources.cpus` | int | ✗ | 正整数 CPU 核数声明；缺省时 CPU-only=1，GPU job=`gpu_job_cpus`（默认 8）|
| `resources.host_mem_gib` | number | ✗ | 有限正数主机内存预留（GiB）；缺省 `host_mem_default_gib`，CPU/GPU 任务均计入 |
| `runtime` | obj | ✗ | `{conda_env:"名"}` ∥ `{venv_alias:"名"}` ∥ `{prefix:"路径"}` 必须且只能选一个；别名须注册、conda env/prefix 目录须在提交时存在；参与指纹 |
| `progress_regex` | str | ✗ | 从日志尾部提取进度，status 展示 |
| `artifacts` | obj | ✗ | `{key:{path,...}}`；规则：存在(缺省)/`"check":"json"`/`min_bytes:N`/`has_key:"键"`/`regex:"模式"`；内容校验有读取/执行上限，symlink 与特殊文件拒绝；命中→SKIP |
| `probes` | obj | ✗ | 仅任务级 `{fail_on_log,ready_on_log}` 日志门控 |
| `_force_rerun` | 内部 | — | `force_rerun` 的落库字段，输入中不要提供 |

\* cmd 与 stages 二选一。

标识符必须匹配 `[A-Za-z0-9][A-Za-z0-9._-]*`，且不能是 `.`/`..`。已移除或未实现的输入会拒绝：批次级 `gpus`、任务/阶段级 `retry_transform`、阶段级 `probes`；GPU 需求写 `resources.gpu`，probe 只写在任务级。

M2B 的可用范围、外部 bridge 依赖和测试入口见 [native-integration.md](native-integration.md)。

下述 `strict` 行为仅指 legacy V1，不是普通用户可自由选择的模式。它只允许一个单 `cmd` 任务，并要求：管理员冷配置
`native_exec_profiles` 与 `(strict, project, batch name, task id, submitted argv)` 唯一精确匹配；
`cmd[0]` 为规范化绝对路径；effective cwd 为当前项目根；不含 `stages`、`sweep`、`runtime`
或 scheduler artifact skip/cleanup 规则、日志 probe；batch/task env 为空；resources 精确为
`gpu=0, cpus=1, gpu_share=false`；显式 `git=false` 与 `max_retry=0`。`git=false` 阻止 reviewed
native verifier 之前启动 PATH-resolved Git 子进程，code-byte 绑定由 external verifier 负责。四项 scheduler-owned metadata
（profile id/digest、project-root identity digest、submitted argv）会落库，两个 digest 共同参与
指纹。profile 对全部 mix 入口（包括 `sched run`）保留 batch name，第一次 strict 耐久入库后该名称永久消费；任何终态后的 fresh submit、
retry/resubmit、失败重试或节点重启重放均拒绝。同 batch id 的 inbox 重投仅在耐久
batch/task/job 不可变绑定精确一致时幂等，同名预占或持久态漂移会 fail-closed。

dispatcher 在 running claim 前复核 cold profile、当前/持久 project-root canonical path 与
device/inode identity、空 env、CPU-only resources。通过后从空继承环境构造 scheduler-owned
control/identity 字段并 exact argv 直接 Popen，不走 Bash/RC supervisor。只有当前 daemon 持有的
Popen rc 可成功结算；权威丢失时 fail-closed blocked，RC sidecar 对 native 无成功权威。

边界：该实现仍按路径打开 executable/cwd，reattest→Popen 存在 TOCTOU；它不是 byte/FD-exact
执行证明。external retained-FD verifier/monitor、七字段 attestation 与专用 poison-overwrite probe
尚未实现，正式任务不得据此运行。

显式版本 `schema: "sched_native_exec_profile_v2"` 当前仅提供冻结批次兼容校验：精确绑定
root/task 公共 keyset，以及 raw `{PROJECT:...}` cwd、空依赖、`_protocol`、非空 batch env、
空 task env、prefix runtime、整数 duration、`max_retry=0`、raw CPU resources、空 artifacts、
公共 task `git` 缺席和未改写 logical argv。local submit 与 daemon inbox 均在依赖/指纹、
批次或名称耐久写入、running claim、进程创建之前 fail-closed。V2 当前不落库，也不授权
Popen。内部 task spec 构造器现可保留 schema 生成的冻结合同；启动前复核函数可对照
持久化 batch/task 字段与管理员冷 profile，但正常提交路径不会触发该函数。正式
retained/bootstrap launcher 与完整生命周期接入后，才能考虑解除提交闸门。

后续接口目前也仅是 fail-closed plan foundation：scheduler 私有的一次性
`NativeLaunchPlan` 持有并复核 retained launcher FD、全封印 request memfd、已连接
AF_UNIX stream control FD、retained project-root dirfd 与 append-only log FD。request
副本具有独立的零 offset；root 必须是非 `O_PATH` 的 `O_RDONLY` FD；单链接 log FD 必须
匹配 retained root 下 `logs/` 内由逐级 `O_NOFOLLOW` 解析的相对路径。actual argv 固定为
`m2b-exec-monitor[native-entry-v1] --native-entry-v1`，actual env 固定为空，child
request/control/root FD 固定为 3/4/5；logical submitted argv 只作证据，不会拼入 actual
argv。request body 仍 opaque 且无权威。私有 generated/no-data
`NativeStep5DNoDataLaunchOwner` adapter 现在自行创建 AF_UNIX socketpair，只将 native 端交给
plan factory 并立即关闭该源端；peer 端由创建进程私有保留，且不提供 raw-FD 或 transfer API。
这只固定预期的 endpoint 构造形状，不认证 scheduler role，也不声称 native 可推断 peer endpoint
独占。plan 只用 `request_frame_sha256` 命名完整 sealed frame 的 SHA-256，并另用
`request_body_sha256` 命名 opaque body 的 SHA-256；不存在歧义的 `request_sha256` alias。
`Executor.launch_native(plan)` 在 Linux FD-exec backend 接入前会关闭 plan 并在创建
进程前拒绝，V2 提交闸门因此仍未解除。adapter 不含 launch、control protocol、nonce、publication
或 daemon route；接入前仍必须完成原子的 final validate/map/FD-exec 与经过审查的真实
direct-parent lifecycle。
隔离式 `NativeStep5DRequestOwner.prepare()` 省略 `log_fd` 时，会在已复核的项目 root 下
以 `O_EXCL` 创建 `logs/` 内的私有 `0600` 日志；plan 保留独立 FD，调用方的 root FD
仍归调用方所有。后续失败保留已创建的日志。内部 `native_sessions` 预留目前只服务
`isolated_integration`，owner 固定为 `unbound`，不代表 M 所有权或正式执行授权。
`claim_native_session_candidate()` 在同一 SQLite writer 事务中对 active/latest/pending 的
V2 job 执行 `running` CAS 并插入唯一 session；调用方必须先完成冻结合同复核，并在
任何 FD 或子进程创建前提交该事务。随后 `_create_bound_native_session_log()` 只从已
提交的预留行读取项目 root 与 `logs/sched-native-<session-id>.log`，经 retained root FD
以 `O_EXCL` 创建日志，把实际 `st_dev/st_ino` 写为 `log_bound` 并提交后才返回 FD。
`reserved` 与 `log_bound` 都是已消费且未决的状态；缺失目录、路径占用、创建失败、
回写失败或重启均不得换 session、覆盖日志、回到 pending 或推断成功。已创建的日志
在后续失败时保留。正式 dispatcher 不调用这些 helper；其 attempt 日志查询路径、
native cancel/timeout/崩溃接管、启动前 one-shot intent 与可信终态证据仍未接入。
CLI `task/log/diag` 仍按既有 state 目录版本路径查询，不能用其读取 native session 日志。
内部 DB `user_version=2` 与公开 CLI JSON 的 `schema_version:1` 是不同版本号。

`gsched.native_step5d_alignment.foundation_alignment_projection()` 提供只读、可 JSON 序列化的
`digest_and_direct_parent_endpoint_foundation_only` 声明；其值从生产 launch 常量、
plan/owner slots、私有 factory 签名与 authority flags 派生。声明明确把 canonical request、
owner-lifetime nonce、control protocol 和 isolated runtime 标为 `unimplemented`，并固定
`step5d_complete=false`。它不导入项目侧协议、不创建 endpoint，也不新增
launch/send/daemon API；主仓验证器会独立重建并追踪生产 foundation 后才接受相等。
因此它只能用于基础对齐，不能作为 Step 5D 完成证据。

### config.json 相关（代理只读，调参报告用户）

`sched init` 要求显式填写 daemon 的计算节点名，新配置使用 `example` 项目和
`python` 解释器别名；已有配置不自动改名。使用 `{ROOT}` 的配置必须设置
`default_project`，不再回退到某个特定部署的项目名。

| 键 | 说明 |
|---|---|
| `projects[P].gpu_enabled` | 布尔值，省略为 `true`；`false` 禁止新 GPU 提交与手动 GPU 重跑，暂停排队 GPU 派发，运行中任务和 CPU-only 不受影响（热更新） |
| `projects[P].gpu_quota` | 项目并发 running GPU job 上限；省略或 `0` = 无限制，CPU-only 不计 |
| `projects[P].priority` | 项目优先级，默认 0；整数越大越先考虑（热更新）|
| `projects[P].gpu_affinity` | 该项目的亲和卡列表；软亲和可在必要时外借其他卡 |
| `projects[P].gpu_affinity_hard` | `true` 时只允许从非空 affinity 选卡；不反向保留这些卡 |
| `projects[P].colocate / max_jobs` | 项目级共享开关 / 每卡打包密度上限（热更新）|
| `gpus[i].max_jobs` | 异构卡每卡打包上限（热更新）|
| `gpus` | 卡号或 `{idx,mem_gib,max_jobs}` 数组；省略/空数组时由 daemon 探测 |
| `co_locate / co_locate_safety / co_locate_max_jobs / co_locate_freeze_pct` | 共享总开关与阈值；默认 `false / 0.7 / 3 / 85` |
| `cpus_total / gpu_job_cpus / max_cpu_jobs` | CPU 配额；默认 `0 / 8 / 2`。`cpus_total=0` 时仅以 `max_cpu_jobs` 限 CPU-only 并发 |
| `host_mem_total_gib / host_mem_reserve_gib / host_mem_default_gib` | 主机内存准入；默认 `0 / 16 / 8` GiB。total=0 关闭；其余有限非负，default 必须大于 0；支持热更新 |
| `idle_timeout_min` | daemon 空闲自动退出分钟数；默认 360，`0` = 禁用 |
| `notify` | 省略时关闭；可配置 batch done/blocked 的 file/email/command 渠道 |
| `conda_envs_dirs` | runtime.conda_env 解析目录（热更新）|
| `task_default_env` | 部署级普通任务环境缺省值 `{k:v}`；batch/task env 可覆盖。legacy V1 strict/native 忽略该键并从空继承环境启动；V2 当前不启动。典型普通任务用途：`{"PYTHONNOUSERSITE":"1"}` 隔离 ~/.local 用户站点污染 |
| `native_exec_profiles` | 管理员持有的 strict exact-profile 映射；legacy V1 exact keys 为 `mode/project/batch_name/task_id/submitted_argv`；V2 用显式 schema 并增加冻结 batch/task 声明。batch_name 在 registry 内唯一且保留。profile registry 与引用项目的 root 均为冷配置；热更新拒绝，必须重启 daemon |

`gpu_quota` 的计数单位是 job，不是不同物理卡或显存：正整数 `N` 表示该项目最多
同时运行 `N` 个已经分配 GPU 的 job；独占和共享任务均每个 job 计 1，共享时多个
计数单位可能落在同一张物理卡上。`resources.gpu:0` 的 CPU-only job 不占该配额。
项目级禁止 GPU 使用 `gpu_enabled:false`，不能用 `gpu_quota:0` 或空 affinity 模拟。
切回 `true` 后原排队版本继续运行，无需重新提交。配置写入与最终派发共用 submission
gate；开关读取失败时暂停 GPU 派发，CPU-only 使用最后有效配置。入口、重试和并发
行为详见 [`project-gpu-access.md`](project-gpu-access.md)。

ready 候选按 `(项目 priority 降序, 批次 priority 降序, job 入队 rowid 升序)`
逐一尝试，两个 priority 都默认为 `0` 且可为任意整数。项目 priority 是第一排序键；
低优项目的批次 priority 再大也不能越过高优项目。该机制不抢占、不预留容量，也没有
aging/fair-share：运行中的低优任务不会被驱逐；高优候选因 quota、CPU、GPU 或
`max_parallel` 暂时不可启动时，后续候选可以补位。

启用主机内存准入后，所有 running 版本的声明预留总和加上新任务必须不超过
`min(host_mem_total_gib, MemTotal-host_mem_reserve_gib)`。分配 CPU/GPU 之前，还检查
计算节点 `MemAvailable`，扣除运行任务尚未使用的预留和本 tick 新启动任务的预留，
保留系统余量。运行进程树采用 PSS，读取失败的部分按未使用预留保守处理，不累加 RSS。
采样或配置读取失败时暂停派发。预留不是 cgroup 硬限制；任务仍应保留运行期内存保护。
`status.cpu.used` 和 `host_memory.used_gib` 都是声明预留，不是实时 CPU/PSS 用量。
节点可用内存来自 daemon 最近 90 秒内的采样，未知时为 null，不在网关采样替代。

安全维护使用 `sched daemon drain` 暂停新派发，已有任务自然结束，pending 保留。
`sched daemon drain --stop-when-idle` 还会在 running 和未决启动标记清空后退出。
排空请求跨 daemon 重启保留；`sched daemon resume` 解除，daemon 已退出时再
`sched daemon start`。这些写操作只在计算节点执行，也可由 `sched request` 包装。

维护请求使用 `--expect-revision 0`，不虚构 daemon revision。例如：

```bash
sched request drain-01 --expect-revision 0 -- daemon drain --stop-when-idle
sched request resume-01 --expect-revision 0 -- daemon resume
```

每次新的逻辑操作使用新的 request-id；重放同一操作必须保留完整命令与参数。
已完成的 drain 请求在 resume 后重放不会重新排空；已完成的 resume 请求也不会
解除后来发起的排空。`daemon drain` 只接受可选 `--stop-when-idle`，`daemon resume`
不接受附加参数。控制文件和所在目录同步后才报告成功；进程中断留下未完成请求时
返回 `75`，禁止自动换 ID 重试。resume 只解除排空，不启动已停止的 daemon。

`daemon status --json` 与 `status.daemon_health` 增加可选 `request_actions` 字段，
当前值为 `daemon-start`、`daemon-stop`、`daemon-drain`、
`daemon-drain-stop-when-idle`、`daemon-resume` 的字符串数组。这是执行查询的 CLI
所支持的请求操作，不证明其他主机上的 writer 或正在运行的 daemon 版本相同。
插件接入维护按钮时需同时核验查询结果和实际 writer 的能力、节点身份与快照时效；
字段缺失的旧 CLI 仍可展示健康状态，但不能据此启用新增维护操作。
`daemon stop` 仍会取消运行任务，不用于无损排空。

⚠️ config.json 为多项目共享配置，修改须经用户确认。

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
`sched clean <batch-ref> --yes`（删除每个最新 `skip` task spec 声明的产物）；
一次性 `strict` native 批次不允许 clean。
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
sched cancel --project <project> --yes   # 覆盖 queued/active/blocked 批次的排队或运行任务
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
| `init [--config PATH]` | 交互生成 config.json | 默认写 bootstrap state 下的 config |
| `verify <batch-ref>` | 确认批次已持久化 | 网关 submit 返回“已投递”后确认入库；完整 batch ID 可查询消费拒绝原因 |
| `submit <file> [--dry-run [--json]]` | 提交批次 | 网关推荐入口；异机写 submit_inbox，daemon 下一 tick 收编；dry-run 只读 |
| `run --project P [--gpus 1\|--cpu-only] [--cpus N] [--duration MIN] [--cwd DIR] [--out PATH] [--venv NAME] [--dry-run] -- cmd...` | 单条命令提交 | 非 dry-run 仅计算节点；不用 inbox；批次 priority 固定 0 |
| `status [batch-ref] [--project P] [--detail] [--json] [--limit N] [--cursor TOKEN] [--job-cursor TOKEN]` | 最新版本当前态 | 缺省 200，钳制到 1..1000；`--cursor` 翻批次页，`--job-cursor` 独立翻任务页 |
| `task <batch-ref>:<task> [--json]` | 单任务全部版本详情 | `--json` 输出单一、版本化 JSON 文档 |
| `history [batch-ref] [--status S] [--project P] [--json] [--limit N] [--cursor TOKEN]` | 终态历史（保留各版本） | 缺省 50，钳制到 1..200；cursor 用于 JSON 稳定键集分页 |
| `markers` | 批次终态 marker 一行查看 | 纯文件查询，不打开数据库 |
| `log <batch-ref>:<task> [-f] [-n N]` | 任务日志 | |
| `diag <batch-ref>[:task]` | 失败诊断（首选）| |
| `incidents [id] [--limit N] [--job ID] [--gpu N] [--json]` | OOM/硬件事故快照 | `--json` 供看板/脚本 |
| `retry <batch-ref>[:task]` | 解锁失败终态重跑（同 spec）| blocked 批次原子回 active；marker 由 daemon 派发前协调；discarded 拒绝 |
| `resubmit <batch-ref>:<task>` / `<batch-ref> [--failed\|--all] [--dry-run]` | 新版本排队尾；支持批次级批量 | discarded/queued 守卫；done/blocked 自动回 active |
| `cancel <batch-ref>[:task] --yes` / `cancel --project P --yes` | 取消 | 后者遍历该项目 queued/active/blocked 批次 |
| `discard <batch-ref> --yes` | 退役 blocked/queued 批次 | 仅拒 running；证据保留 |
| `clean <batch-ref> --yes` | 清全部指纹+删最新 skip spec 产物 | 仅非 strict 终态批次；仅重排最新 skip；done 自动回 active |
| `list-gpus` | GPU 视图（packed=n/cap）| |
| `gpu-set-mem <idx> <GiB>` | 临时覆盖 state/list-gpus 中的 GPU 容量 | 重启时会被 config 或硬件探测覆盖 |
| `gpu-ok <idx>` / `gpu-ignore <idx>` | 解除 quarantine / 静默 unmanaged 告警 | 未知 idx 拒绝；不等同于强制释放 |
| `gpu-free <idx> --yes` | 强制 GPU 回 free | 破坏性操作；操作者须先确认无受管或外部任务；自动化应使用 request/CAS 绑定 assignments |
| `config get` / `config set -f patch --yes` / `config reload` | 配置读取/补丁写入/立即热更 | 冷键拒绝；多项目共享需谨慎 |
| `request <request-id> --expect-revision N [--expect-kind ...] ... -- <mutation>` | 持久化幂等 mutation | revision 必填；request-id 与完整命令/前置条件永久绑定；GPU 还必须绑定完整 assignments JSON |
| `daemon start/stop/status/check/drain/resume` | daemon 生命周期 | start/stop/check/drain/resume 仅计算节点；`--fake` 仅供 start/check 测试；显式 override 才可跨主机 |
| `notify-test` / `notify-inbox [--all] [--json]` / `notify-ack <file>` | 通知测试与检查点 | inbox 为纯文件查询；ack 是写操作 |
| `project list` | 项目 GPU 访问策略、配额/用量 | `--json` |

破坏性命令（cancel/gpu-free/discard/config set/clean）缺 `--yes` 时返回码 1 =
未确认而非失败。

`sched run` 当前只有单 GPU 与 CPU-only 两种受支持的资源形态。GPU 任务省略
`--gpus` 或显式写 `--gpus 1`；零 GPU 必须用 `--cpu-only`。拒绝其他 GPU 数量，
也拒绝同时指定 `--gpus` 与 `--cpu-only`。
它默认采用 config 中第一个 venv，工作目录为 `{ROOT}`（`default_project` 根目录）；
`--project` 只决定归属、quota、priority 与 affinity，不改变 `{ROOT}`。需要其他项目
目录时显式传 `--cwd '{PROJECT:P}'`。`run --dry-run` 与真实提交都校验项目成员，
并要求显式提供的 `--cpus` 与 `--duration` 为正整数。命令按 argv 保留引号，
使用非登录 Bash 保留所选 venv 的 `PATH`；需要 shell 管道时显式使用 `bash -c`。
与可无状态执行的 `submit --dry-run` 不同，当前 run 预览仍要求已有且可读的 state DB。

`config set` 将读取、深合并、校验和原子替换置于同一个 submission gate，
并发补丁按顺序读取前一个已提交配置，避免相互覆盖。`notify-ack` 只接受当前
节点 `notify_inbox` 内的普通 `.json`／`.json.acked` 文件；损坏事件在查询中标记
为不可读，不中断其余事件。

### 稳定 JSON 与跨主机读取

- `project list --json` 输出 `{"schema_version":1,"projects":[...]}`；每项含 `name`、有效布尔值 `gpu_enabled`、整数 `gpu_quota`（省略或 null 归一为 0）、`gpu_access:"disabled"|"unlimited"|"limited"`、`gpu_used`、`priority`、`colocate`、`max_jobs`、`gpu_affinity`、`root`。禁用不清空原配额，`gpu_used` 仍显示运行中的 GPU job 数。
- `project_gpu_disabled` 仅用于排队 GPU 任务，优先于 quota/dependency 等待原因；任务状态仍为 `pending`。这扩展了 schema 1 的等待原因枚举，启用此功能前应同步更新 `dsh-node-sched`，旧插件的严格校验会拒绝新值。

- `status --json` 固定 `schema_version:1`，`limit` 缺省 200、钳制 1..1000；顶层可含 `host_memory:{used_gib,total_gib,reserve_gib,default_job_gib,available_gib}`（启用内存准入时），`daemon_health.draining` 表示持久排空；顶层含 `batches`、`jobs`、`gpus`、`cpu`、`daemon_health`、`truncated:{batches,jobs}`、批次分页 `next_cursor` 与独立任务分页 `next_job_cursor`。先按当前态优先、再按新旧顺序有界选择批次，任务只来自已返回批次且只含各 task 最新 version，因此每个 `jobs[].batch_id` 都能在 `batches` 中解析。等待态统一为 `"status":"pending"` 与独立 `"wait_reason":"project_gpu_disabled"|"quota"|"dependency"|"cpu"|"host_memory"|"gpu"|"parallel"|"draining"|"batch_blocked"|null`，不要解析人类视图的装饰文本。`batches[].revision` 是整数；`gpus[].revision` 是整数，`gpus[].assignments` 按 `job_id` 排序且每项为 `{"job_id":...,"vram_gib":...}`。只有 `truncated.batches=true` 时才用 `next_cursor`；只有 `truncated.jobs=true` 时才用 `next_job_cursor`。指定一个批次时也可只翻其任务页，不会丢失该批次行。
- `task --json` 固定 `schema_version:1`，输出 `batch_id`、`batch_name`、`batch_revision`、`task` 与 `jobs` 版本时间线；每个版本含状态、运行结果/时间、resources、规范化 spec 与 log 路径。stdout 只含这一份 JSON。
- `history --json` 固定 `schema_version:1`，`limit` 缺省 50、钳制 1..200；顶层含 `history`、`truncated` 与 `next_cursor`，每项含 `batch_id`、`batch_name`、`task`、`status`、`version` 及运行结果/时间。
- 配置的 `node` 之外执行查询时，CLI 不初始化、不迁移、也不写源 DB；无论源目录当时是否存在 WAL/SHM，都先复制出稳定的私有 DB（及存在的 WAL）快照，再以 `mode=ro` 打开。绝不对仍可变化的 live DB 使用 `immutable=1`。`daemon status` 也可跨主机只读；`daemon start/stop/check/drain/resume` 默认拒绝。
- `daemon status --json` 是不打开数据库的只读查询，输出 `schema_version:1` 与和 `status.daemon_health` 相同的健康字段：`node`、`query_host`、`pid`、`observed_at`（Unix 秒）、`process_state`（`running/stopped/unknown`）、`health_state`（`healthy/delayed/stalled/stopped/unknown`）、`heartbeat_age_s`、`tick_ok_age_s`、`frozen`、`draining`、`read_error`。年龄不可读或不存在时为 null；`read_error` 为 null、`health_file_unreadable` 或 `timestamp_in_future`。同物理节点且 lease 与进程启动标识一致才确认进程存活，跨节点不探测本机同号 PID。原进程退出/被复用或目标本机确认 lease、PID、心跳均不存在才确认 stopped。健康要求心跳 <60 秒且成功 tick ≤90 秒；tick >90 秒为 stalled，心跳新鲜不能掩盖 tick 停滞；已确认 stopped 与读错误优先。draining 是独立派发状态，不覆盖健康故障。这个查询结果只用于展示，不改变生命周期、租约或写操作校验。
- 看板 daemon 提示必须消费结构化健康数据；不解析中文展示文本。SSH/API 错误、格式错误、缓存过期或浏览器本地 TTL 到期均显示状态未知并禁用 daemon 操作；旧采样可保留供查看。只有新鲜且确认 stopped 的状态可启用 start，不能把心跳过期当成启动依据。旧 CLI 不支持该 JSON 命令时提示未知，需配套升级查询 CLI。
- 配置的 `node` 本机执行 `status/task/history/diag/log/list-gpus` 等数据库查询时，先以只读方式核验 schema 版本、必需对象/列与 WAL 文件头；schema 已是当前版本时，查询连接固定使用私有快照上的 `mode=ro + query_only`，不再对源库执行 `journal_mode=WAL`、`BEGIN IMMEDIATE` 或幂等迁移。快照遇到 daemon 写突发时最多重试 8 次并做有界退避（累计 sleep 上限 1.585 秒）；首次建库、旧 schema、非 WAL 库或权限漂移才进入带有界锁重试的 writer 初始化路径。高于当前版本的库在本机与网关查询都 fail-closed。纯文件查询 `markers`、`notify-inbox`、`daemon status` 以及 `config get` 不检查或打开数据库。
- `request` 只包装 `submit`、`cancel`、`retry`、`resubmit`、`gpu-free`、`gpu-ignore`、`gpu-ok`、`daemon start/stop/drain/resume` 与 `config set`；未列出的 mutation 有意 fail-closed。`gpu-set-mem` 是重启时会被 `config.gpus` 或硬件探测覆盖、且未纳入 revision/CAS 的临时 state/list-gpus 记录，不由 `request` 包装。每次 request 都必须提供非负 `--expect-revision`；无目标的 submit/daemon/config 使用 `0`。task/batch 绑定所属 batch 的 `revision`，其中目标必须使用完整 batch ID（不能用批次名）；GPU 绑定自己的 `revision`，且 GPU 必须额外传 `--expect-assignments-json`（与 status 返回的已排序数组完全一致）。被包装命令使用规范顺序：目标紧跟子命令，选项随后。revision 由 SQLite trigger 在批次状态、task/job 代际与 job 状态变化，以及 GPU 状态/quarantine/ignore 确认/assignment、`gpu_jobs` membership/装箱值变化时递增，所以状态值绕一圈回到原值的 ABA 仍返回 65。task 示例：`sched request retry-42 --expect-kind task --expect-id batch-20260829-000000:train --expect-status failed --expect-version 1 --expect-revision 17 -- retry batch-20260829-000000:train`。GPU 示例：`sched request gpu-42 --expect-kind gpu --expect-id 0 --expect-status assigned --expect-quarantined 0 --expect-revision 9 --expect-assignments-json '[{"job_id":"batch-task-v1","vram_gib":1.5}]' -- gpu-free 0 --yes`。
- 对数据库 mutation，业务写入与 ledger 的 done/code/output 在一个外层事务中原子提交；嵌套 submit/retry/resubmit 的 `commit()` 被外层事务接管，daemon 唤醒只在提交后发生，marker 由 daemon 按数据库权威状态协调。`retry`/`resubmit` 的最终事务、`clean` 的发布重跑阶段以及 `cancel` 的任务分类与写入，都会在读取权威状态前取得 SQLite writer claim，防止 daemon 在状态校验与首个 job/task mutation 之间收敛或派发任务。daemon 的节点重启恢复、接管终态判定与重试发布也使用同一 writer 顺序；已落库的 cancel request 或 `kill_reason=cancelled` 永远优先于自动重试/恢复回队。`daemon start/stop/drain/resume` 与 `config set` 不绑定 SQLite 事务，进程中断留下 started 时返回 75，拒绝猜测外部结果。相同 request-id 和完全相同绑定重放已保存退出码/输出而不重复执行；绑定变化返回 64，前置条件冲突返回 65。stdout/stderr 捕获各自最多 2 MiB；旧 done 输出定期压缩为 tombstone（清空输出但永久保留 argv 绑定与退出码），因此 tombstone 重放保持退出码且不重复 mutation，但不再重放旧文本。
- 本地 `submit` 与网关 inbox payload 都可在 submission gate 外做预览校验和计算指纹，但最终写入前必须在同一 gate 内按最新同名代际重验依赖存在性、完整依赖图、批次 ID 与同名终态，并原子提交批次/任务/job（inbox 同时提交请求回执）。因此并发提交不能分别基于旧快照发布 `A → B`、`B → A` 环，也不会在 `clean` 两阶段之间或 `retry`/`resubmit` 事务中途插入第二个同名非终态批次；反向顺序会在重开旧批次前拒绝已有的同名非终态实例，`clean` 在删产物前和发布重跑前各重验一次。daemon 退出门禁生效时 payload 与 pending 请求保留供恢复后重试。
- state 根目录优先级：`SCHED_STATE` > 已加载的 `config.state_dir` > `~/.sched`。相对 `config.state_dir` 以 bootstrap config 所在目录为基准解析；`node` 必须是安全的单一路径分量。共享 state 上的登录节点查询仍定位 `config.node` 的节点目录。

---

## 5. 状态机速查

```
任务:  pending → running → done / failed / blocked / cancelled / timed_out / interrupted
       pending → skip (指纹命中, 与成功等价)
批次:  queued → active → done | blocked (等 retry/resubmit)
       blocked → active (retry 或 resubmit)
       done → active (resubmit，或 clean 确有最新 skip 重排)
       blocked/queued → discarded (退役终态, 不可 retry/resubmit)
GPU:   free → assigned → releasing → free; 外部占用 → unmanaged; 连续健康异常 → quarantined
```

空闲 GPU 的物理探测区分信号来源：发现 compute PID，或 compute/topology/utilization
任一探测不确定时，
仍在首次采样立即转 `unmanaged`（fail-closed）；只有“compute 列表完整且为空、但
`utilization.gpu > 0`”这一类易受驱动底噪影响的信号才做连续 3 次确认。确认期间该卡
保持 `free` 展示但从所有派发路径临时排除，任一干净采样立即清零并恢复派发；连续确认
成立后才持久转 `unmanaged`。因此 1–2% 的单次/短时空闲抖动不会制造状态和日志风暴，
也不存在把确认中的可疑卡分给新任务的窗口。已经处于 `unmanaged` 的卡连续 2 次
干净采样后自动恢复 `free`；`quarantined` 仍须 `sched gpu-ok` 人工解除。

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
`retry`/`resubmit` 能将 blocked 批次改回 `active`。`clean` 仅在 done 批次确有最新
`skip` job 被重新排队时重开该 done 批次。
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

## 6. 常见误区 TOP6

1. 忘写 `gpu_share: true` → 单卡单任务（R1 清单）
2. 改代码未 commit 就 resubmit → 正常现象是重跑；若 SKIP 见 R3/R4
3. 共享任务漏写 `vram_gib` → 校验直接拒绝（装箱必须有名数）
4. 同名重复 submit 会生成新实例；名称引用选择最新实例，要操作旧实例请使用完整 batch id
5. `min_bytes` 设得比真实结果大 → 有效结果被判无效；小 JSON 用 `check:"json"`
6. 把 `gpu_quota:0` 当成禁用 GPU → 实际是无限制；项目级禁用使用 `gpu_enabled:false`

## 7. 下一步开发（尚未实现）

尚未实现的能力记录在 [`next-development.md`](next-development.md)，不能作为当前
config/API 使用。项目级 GPU 开关 ND-01 已实现，行为与验收依据见
[`project-gpu-access.md`](project-gpu-access.md)。
