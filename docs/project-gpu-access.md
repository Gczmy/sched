# 项目 GPU 访问开关（ND-01）

2026-09-07 实现。配置和 CLI 的完整参考见 [reference.md](reference.md)。
本次验证均在本地完成，未连接、部署或操作 HPDC。

## 配置与行为

`projects.<name>.gpu_enabled` 为布尔值，省略时为 `true`；`null`、数字和字符串
均为非法值。它独立于 `gpu_quota`：配额省略、`null` 或 `0` 仍表示无限制，
正整数仍限制并发 running GPU job 数，CPU-only 不计。

将以下补丁保存为 state 目录外的 JSON 文件，在配置指定的计算节点通过
`sched config set -f <patch.json> --yes` 应用；将 `false` 改为 `true` 可恢复 GPU。

```json
{"projects":{"my-project":{"gpu_enabled":false}}}
```

| 场景 | 禁用后的行为 |
| --- | --- |
| 新 GPU submit/run（含 dry-run） | 明确拒绝；不写入 batch/task/job |
| CPU-only 提交与执行 | 正常允许，仍受既有 CPU 配额和依赖约束 |
| 混合 CPU/GPU 批次提交 | 整批拒绝，不部分入库；可单独提交 CPU-only 批次 |
| 已排队 GPU job | 保持 pending，暂停派发，不占新 GPU 资源 |
| 正在运行的 GPU job | 正常执行至终态，不被开关取消 |
| 手动 GPU retry/resubmit | 拒绝；批量目标只要含需重跑的 GPU 任务，整个操作不写入 |
| 单独指定 CPU-only retry/resubmit | 正常允许 |
| 自动重试／恢复已有 GPU job | 可回到 pending，但最终派发仍暂停 |
| 重新启用 GPU | 原排队版本继续候选调度，无需重新提交；仍受配额、依赖等限制 |

默认 GPU 请求、stages 和 sweep 展开后的任务均适用同一规则。任务 GPU 请求仍仅
支持 `resources.gpu` 为 `0` 或 `1`，本项不引入多卡单任务或物理设备访问隔离。

## 写入、热更新与 inbox

`schema.validate_project_gpu_access` 统一提交和手动重跑的校验。
本机 submit/run/resubmit 可在锁外计算指纹，但最终写入前必须在 submission gate
内重读配置；retry 在同一 gate 内检查所有实际重跑目标后才修改状态。
配置深合并、校验和原子替换也使用该 gate。

daemon 最终派发在同一 gate 内重读磁盘配置，不依赖缓存、mtime 或 reload 请求
何时被消费。开关禁用返回后不会再依据旧配置启动新的 GPU job；先取得 gate 的
派发决定可先完成，随后禁用生效。配置不可读或校验失败时暂停 GPU 派发，CPU-only
仍可使用 daemon 最后有效的配置；未注册项目不会获得 GPU 许可。

网关 submit 的“已投递”不是入库承诺。daemon 消费 inbox 时，在最终写入前按最新
GPU 策略校验；拒绝时保留 control request 结果并移除已处理 payload，不产生部分
batch/task/job。使用 `sched verify <full-batch-id>` 可查看拒绝原因。该回执沿用
已有 control request 保留策略（最近 1000 条已完成请求），不是永久审计记录。
配置暂时不可读时请求与 payload 保持待处理，供下一轮重试。

已经成功入库的相同 inbox 投递即使在禁用后重复到达，也仍返回原来的成功确认，
不重新创建批次；这保持了既有投递幂等性。

## CLI 与看板契约

`sched project list --json` 输出 schema 1 的项目列表；`gpu_enabled` 为有效布尔值，
`gpu_access` 区分 `disabled`、`unlimited`、`limited`。禁用时保留原配额并显示仍在
运行的 `gpu_used`，不会把使用量伪装为零。

`sched status --json` 中受禁用影响的 GPU 任务保持 `status:"pending"`，
`wait_reason:"project_gpu_disabled"` 优先于 quota/dependency。运行中、终态和
CPU-only 任务不使用该原因；重新启用后重新按普通等待条件展示。

配套 `dsh-node-sched` 校验接受该等待原因；看板在项目配置中提供 GPU 开关及
禁用／无限制／配额标签，在任务提示和展开的批次中展示暂停原因。保存补丁保留
显式 `false` 和零配额。开关通过既有配置写入接口转发，调度语义仍由 sched 实现。

schema_version 仍为 1，但等待原因枚举增加了一个值。启用功能前需同步更新插件，
旧版严格校验会拒绝包含 `project_gpu_disabled` 的状态响应。

## 验收依据

`tests/test_project_gpu_enabled.py` 覆盖布尔类型、默认兼容、任务展开、提交无部分写入、
指纹准备期间禁用、inbox 拒绝回执与重复投递、CPU-only 例外、混合批次手动重跑原子性、
配置不可读、自动重试／恢复和配置更新与派发互斥。

`tests/run_project_gpu_enabled_accept.sh` 使用真实本地 daemon、fake GPU 和公共 CLI：
运行 GPU 任务期间禁用，确认运行任务完成、排队 GPU 不执行、CPU-only 新任务完成；
启用后确认原队列执行且 version 不变。脚本使用专用临时根目录，退出时通过公共 CLI
停止 daemon 并清理本次测试资源。

可通过 `SCHED_GPU_ACCESS_STATUS_OUT=<path>` 保存该验收中的真实 CLI 状态 JSON，
再交给插件 `canonicalStatusDocument` 和看板 `collectStatusPages` 联调；本次已验证
新的等待原因、展示文本和完整快照写入资格均保留。

本地回归命令：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_*.py'
bash tests/run_project_gpu_enabled_accept.sh
bash tests/run_hotreload_accept.sh
bash tests/run_affinity_config_accept.sh
bash tests/run_cpu_quota_accept.sh
bash tests/run_submit_inbox_accept.sh
bash tests/run_docs_refs_accept.sh
```

配套仓库运行全部 Node 测试并重新构建看板；插件和看板各有
`test/project-gpu.test.js` 覆盖枚举、分页、配置补丁和显示语义。

2026-09-07 本地验证结果（WSL Ubuntu 22.04 / Python 3.10.12 / Node 22.23.2）：

| 检查 | 结果 |
| --- | --- |
| Python 全量回归 | 376 项：375 通过，1 项 Darwin 专用测试按平台跳过 |
| Node 全量回归 | 269 项全部通过；41 个被测源文件、测试及产物与当前工作区逐字节一致 |
| 项目 GPU 端到端验收 | 通过；真实 daemon、fake GPU、CPU-only 和原版本恢复 |
| 既有相关验收 | hotreload、affinity_config、cpu_quota、submit_inbox 全部通过 |
| 跨仓库真实 CLI JSON | 插件校验、摘要、看板分页、等待文本和写入资格均通过 |
| 文档 | AGENTS 引用验收及变更 Markdown 的相对文件链接通过 |
| 看板构建 | Windows 与 Linux bundle 及 source map 的 SHA-256 相同 |

产物 SHA-256：`client.js` 为
`46f94941642482a3744589b4d49d0abba30d81e497584beae47427644d2fd2d0`，
`client.js.map` 为
`bb36c89737569a386931ad167284f755f0052107c59b1f714cbf1cc3caf2c055`。
