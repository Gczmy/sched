# 2026-09-22 HPDC 资源修复记录

2026-09-23 05:05 UTC 恢复完成：daemon 已无损重启为 PID 635428，`draining=false`、心跳及 tick 正常；22 个实验全部持久化，2 个 running、20 个因项目配额 pending。运行的是 `electricity_h336_seed45/46`，CPU 声明预留 60/120 核、主机内存声明预留 72/96 GiB；GPU 0/1 实测利用率 98%/95%，各约 7.3 GiB 显存，GPU 2/3 空闲。两项 migration/profile 检查通过，已存在实际 GPU 计算进程。

- 调度器功能提交：`e2db9cccea50a87ed63a21c7a95d7ad8dcb6e184`，已推送 GitHub 并同步 HPDC 源码仓库。生产 wrapper 与 daemon 的 `PYTHONPATH` 均为 `/home/zzhang54/zzhang54/sched-resource-release-e2db9cc`。发布目录由该提交归档生成，102 个文件与计算节点验证的候选包一致；后续仅补充本记录不要求重启 daemon。
- 配套插件提交：`9e2845577f12f39ea75434c95eea62933fb7e98e`，在 GitHub `78a9bcd` 上整合并重建，已推送。尚未定位实际看板运行主机/profile，因此这里确认的是代码和生成物同步，不代表运行中看板已更新。
- 验证：Python 共 388 项（387 通过、1 跳过），排空/恢复验收通过，CPU 配额 6/6，文档引用检查通过；修订后的恢复脚本 3 项回归和排空验收再次通过。UI 41/41、相关后端契约 11/11；真实生产 CLI 的 3 页、2927 条任务通过整合后的严格 JSON 校验。
- 首个批次保持 `waveletmixer_sched_20260922_native_electricity_h336_seed45-20260923015708674`，使用原请求 ID 重放，没有重复入队。其余 21 项逐一提交并 verify，所有批次 active 后才 resume。旧成功结果、6 个预检失败和 4 个暂缓实验保持原有处理结论；EntoKids 的产物路径问题仍独立阻塞。
- 部署审计仍在下文 OPS 目录：`restart-e2db9cc.json`、`activation.failed-before-e2db9cc.json`、`activation-recovery-e2db9cc.log`、`activation.json` 与 `receipts.json`。生产回执共 22 条，最终阶段为 `active`。

以下为恢复前检查和准备记录。

2026-09-23 04:43 UTC 复查：旧训练 `traffic_h96_seed42` 已于 01:55 UTC 成功完成，新 daemon PID 547232 于 01:57 UTC 启动。切换脚本在首个批次提交后，因把短暂的 `queued/dependency` 状态误判为异常而退出；当前保持 `draining`，22 个新批次仅 1 个已入库，其余 21 个未提交。完整 CLI 分页无 running，4 张 GPU 无计算进程，`daemon check` 全部通过。

修复后的 `activate_once.py` 接受无依赖批次在一次 daemon tick 前的暂态，并在所有批次变为 active 后才 resume。`--continue-submission --release-path <verified-release>` 用原 request-id 和原始提交路径继续已配置、已暂停的部署，原子保存回执，避免重跑旧切换步骤或重复提交。该选项必须在计算节点执行，且 wrapper 已指向经验证的 release。

本地与运行发布包的 91 个清单文件一致。正式同步前，sched 基础为 `583ae86`；配套插件需整合 `78a9bcd` 的 DSH 0.1.6/SSH 取消更新，再重建生成物。后续生产状态以 CLI 和部署审计 `activation.json` 为准。

以下保留 2026-09-22 21:10 UTC 的准备记录，描述的是当时状态，不是当前运行状态。

- 入口：`ssh HPDC`，screen `318061.ambior1`，计算节点 `ambiorix`。
- Slurm 租约：2333，120 CPU，至 2026-09-28 22:11:15 UTC。
- 已通过 CLI 取消空闲的 `gpu_group_0/1/3`，释放 90 核声明和 3 张 GPU；项目 `wmpp_ett_trial_seed42.gpu_quota=2` 已热更新。
- 当前 `gpu_group_2` 仅运行 `native_traffic_h96_seed42`。CPU 声明为 30/120，GPU 0/1/2 free，GPU 3 assigned；最近 tick 6.3 秒，无冻结。

## 实现与验证

调度器增加 `resources.host_mem_gib`、96 GiB 声明预算、16 GiB 系统余量、缺省 8 GiB。预算在分配 GPU 前检查，结合 MemAvailable、运行进程树 PSS 和尚未实际使用的预留；同轮启动也计入，读取失败保守暂停。预留不等于 cgroup 硬限制。

新增 `daemon drain [--stop-when-idle]` / `daemon resume`，暂停新派发、保留 pending、允许 running 自然完成。暂停状态跨重启保留。CPU/内存/配额/依赖/批次并发/排空等等待原因可见；CPU 和内存 used 明确表示声明预留。

- 最终发布包：385 项 Python 回归（384 通过、1 跳过）；CLI 排空/退出/恢复验收通过；CPU 配额验收 6/6；文档引用检查通过。
- dsh：UI 33/33，本次后端与 UI 契约 3/3；新版 CLI 的真实生产分页（437 批次、1000 任务、明确标为截断）通过新后端严格校验。
- dsh 原生 Windows 完整后端测试受 POSIX 主机约束，不能作为完整后端通过记录。浏览器 bundle 已构建；原 esbuild 二进制 CRC 读取错误通过下载锁文件指定的同版本包并验证 SHA-512 后解决。
- 未找到运行中的 dsh 实例，其主机/profile 已向用户询问。配套仓库代码和生成物已就绪，实际看板部署仍待定位，旧插件不能读取新增等待原因。

## 新实验队列

远程目录：`/home/zzhang54/zzhang54/Error_checking/trials/waveletmixer_sched_20260922`。

原池共 102 项：69 项已完成、6 项预检失败、1 项仍运行、22 项准备迁移、4 项因同形状 GPU 预检失败暂缓。迁移不重跑完成项，不重试失败项，不与当前训练重叠。

每个实验独立一个批次、一个 sched task，避免单项失败封锁整个队列。`batch.json` 仅用于汇总审查，正式提交以 `batches/index.json` 的 22 个独立文件为准。科学源码、argv、随机种子、batch size、DataLoader 数量均保持；复用已冻结的完整 batch 预检证据。新 runner 内存声明与 sched 一致，最后一道内存保护改为立即失败，不持卡等待。

| 类型 | 完整运行最大 PSS（GiB） | 新预留（GiB） |
| --- | ---: | ---: |
| electricity h96 | 5.83 | 16 |
| electricity h336 | 19.11 | 36 |
| electricity h720 | 32.85 | 56 |
| traffic h192 | 19.05 | 36 |
| traffic h336 | 30.03 | 52 |
| traffic h96 | 暂用较大 horizon h192 的证据 | 36 |

规则为峰值 × 1.5 + 4 GiB，向上取 4 GiB 整数，最低 16 GiB。未改变 80% GPU 预算：electricity h192 五项原失败，以及新增 traffic h720 seed42 失败保留；traffic h720 seeds43–46 暂缓，不通过削减科学参数绕过预检。

## 一次性切换流程

审计目录：`/home/zzhang54/zzhang54/Error_checking/trials/sched_migration_20260922`。

1. 已运行的旧池排空辅助进程 PID 465002 持有**实验池自己的** queue.lock；当前实验写完结果且子进程退出后，仅通过 `sched cancel` 结束空闲外层 worker。未直接改动 sched state。
2. 已运行的一次性切换进程 PID 478302，当前阶段 `waiting_for_old_training`。辅助进程均最多等待 8 小时，超时记录失败，不中断训练或强行重启。
3. 确认旧 worker 均 cancelled，完整 CLI 分页无 running 且无其他 active pending，然后 `sched daemon stop`。
4. 将 `$HOME/bin/sched` 的 PYTHONPATH 原子切换到独立发布目录，原 wrapper 保存在审计目录。旧源码仓库保持原状。
5. 用新 CLI 持久排空，设置内存预算，通过 `daemon check`，在暂停状态启动 daemon。
6. 以独立、固定 request-id 提交并 verify 22 个批次，再 resume。确认任务启动、并发不超过 2、CPU/内存预算未超限后写入 `activation.json` 的 `active` 阶段。

发布目录：`/home/zzhang54/zzhang54/sched-resource-release-20260922`；代码清单见其 `RELEASE_MANIFEST.json`，验证记录见 `VERIFIED.json`。发布包 SHA-256：`2f1891c109240939871159e2881d35183b9b29b5fbbf8164282959222515c74f`。基础提交：`583ae863fbd5bc63a8b917da7110d4a0379513cd`。

流程阶段用审计目录的 `activation.json`、`activation.log` 查看，提交结果写入 `receipts.json`。这些是部署审计文件；实际任务与 daemon 状态仍以 sched CLI 为准。若阶段为 failed，先核对已保存 request-id 与回执，不重新生成 ID 或盲目重复提交。

本目录保留准备与切换脚本副本，供审查，**不要从本地直接重复执行**。

## EntoKids 的独立阻塞

setup 在释放 CPU 后运行约 24 分钟，命令 rc=0，依赖安装、下载、doctor 已完成，ready 文件包含 `ready:true`。失败是产物路径：spec 的 cwd 为 `/mnt/ldap_user_homes/.../EntoKids`，artifact 却使用 `/home/.../EntoKids/...`；`paths_escape:false` 的词法目录边界检查拒绝这个别名路径。

其后两个 pilot 仍等待 setup。修复应在新提交 spec 中将产物写成相对 cwd 的 `manifests/4fd41cb2f5347b10-ready.json`（并核对两个 pilot 的产物路径），不能通过改 DB 将任务标 done。本轮未修改 EntoKids 的 spec 或绕过产物校验；对应通知已用 CLI 确认。
