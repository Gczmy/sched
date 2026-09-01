# sched 下一步开发内容

> 本文只记录尚未实现的开发项，不是当前配置/API 参考。当前可用行为以
> [`reference.md`](reference.md) 和实际 CLI 为准。

## ND-01：项目级禁止使用 GPU

**状态：待设计、未实现。** 当前版本没有项目级“0 卡”或“仅允许 CPU”开关。

### 当前契约（必须保持兼容）

- `projects.<name>.gpu_quota` 省略或设为 `0` 表示不设置项目级上限，不是禁止 GPU。
- 正整数 `N` 限制该项目同时处于 `running` 且分配了 GPU 的 job 数量；它不是不同
  物理卡的去重数。GPU 共享时，同一卡上的多个 job 会分别计数。
- CPU-only job（`resources.gpu: 0`）不占用 `gpu_quota`。
- `resources.gpu: 0` 只约束单个任务，不能阻止同一项目的其他任务申请 GPU。

不能把 `gpu_quota: 0` 改成“禁止 GPU”，否则会破坏现有配置中“0 = 无限制”的稳定
语义。

### 目标行为

为项目增加一个与配额独立、含义明确的 GPU 访问开关。字段名称和迁移方案在实现前
另行定案；在此之前，不应在配置中试写尚不存在的字段。完成后应满足：

1. 项目禁用 GPU 时，任何申请 GPU 的任务都必须在 batch/task/job 写入 state DB 前被明确拒绝；网关 inbox payload 可以先安全暂存，但消费拒绝必须留下可诊断回执。CPU-only 任务仍可提交和运行。
2. `sched submit` 本机路径、网关 `submit_inbox` 消费路径和 `sched run` 使用同一校验语义，不能绕过。
3. `sched project list` 和相关机器可读接口能清楚区分“GPU 禁用”“无限制”和正整数配额。
4. 明确该开关是否支持热更新，以及对已排队、已运行任务的处理；默认必须 fail-closed，且不得暗中终止运行任务。
5. 增加 schema/config 单元测试、网关提交契约测试、CPU-only 例外测试及调度器回归测试。

### 非目标

- 不改变 `gpu_quota` 的现有零值语义。
- 不借用空 `gpu_affinity` 或非法负配额表达禁用状态。
- 本项不引入多卡单任务；当前任务 GPU 请求仍只支持 `resources.gpu` 为 `0` 或 `1`。
