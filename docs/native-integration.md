# M2B 分支整合与实现边界

2026-09-27 将 `codex/m2b-step5e-external-anchor` 的 `3cd3987` 整合到
主线 `b097fbc`。前两条 M2B 分支均为此分支的祖先，不需要分别合并。
三个原始分支头通过以下 annotated tags 保留，可恢复对应历史：

- `archive/m2b-native-exec-profile-20260927` → `0d5d8e3`
- `archive/m2b-step5d-control-runtime-20260927` → `eaea20e`
- `archive/m2b-step5e-external-anchor-20260927` → `3cd3987`

## 当前可用范围

普通 `mix` 提交仍使用主线的 schema 2 指纹、提交事务、取消优先级、项目
GPU 开关、主机内存预留、drain 和 daemon 健康检查。没有配置
`native_exec_profiles` 时，不会启用 strict 通道或导入 native C bridge。
运行时仍无第三方 Python 依赖。

V1 `strict` 只接受管理员冷 profile 精确绑定的单个 CPU 任务；已消费的
批次名称不能再次提交、retry 或 resubmit。运行中任务需要当前 daemon 的
原始 `Popen` 权威才能成功结算。它受 drain 控制，项目禁用 GPU 不阻止
此 CPU 通道；任务不继承 daemon 或 `task_default_env` 环境。
`SCHED_BATCH_ID` 沿用既有环境契约，值是批次名称；CLI JSON 的 `batch_id`
才是持久化批次 ID。完整限制见 [reference.md](reference.md)。

V2 profile 只有校验器，提交入口在持久化和启动前拒绝。
`Executor.launch_native(plan)` 仍抛出 `NativeLaunchUnavailable`，不会回退到
路径执行。Step 5D/5E/5F 的协议、传输和生命周期组件作为实验基础保留，
不构成正式 MPC_OTSF 执行链，也没有接入常规 dispatcher 的 V2 调度路径。

## 外部依赖

`Executor.reserve_native_monitor()` 等显式 native monitor 方法需要
`gsched._m2b_scheduler_native` 扩展。其 C 源码和专用构建/验收脚本位于
配套研究仓库 `MPC_OTSF`，不随 `sched` 包构建或安装；普通 daemon 不依赖它。
整合不修改该仓库的冻结合同，也不把历史 native 运行记录当作当前验证。

Step 5E 的跨仓协议测试从 `M2B_STEP5E_CONTRACT_ROOT` 读取冻结向量、合同和
勘误，校验其 SHA-256。未设置此变量时仅跳过外部向量测试；一旦设置，
路径错误、文件缺失或摘要不符均导致失败。Step 5F 中的项目专用绑定保持
冻结值，没有泛化成任意项目 API。

## 本地验证与复现

在 Linux/POSIX 开发环境使用独立虚拟环境；以下命令不访问 HPDC。
安装可选测试依赖不会增加调度器的运行时依赖：

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e '.[test]'
M2B_STEP5E_CONTRACT_ROOT=/path/to/MPC_OTSF python3 -m pytest -q tests
python3 tests/run_native_integration_accept.py
bash tests/run_mode_consistency_accept.sh
bash tests/run_submit_inbox_accept.sh
bash tests/run_project_gpu_enabled_accept.sh
python3 tests/run_host_resources_accept.py
bash tests/run_daemon_heartbeat_accept.sh
bash tests/run_docs_refs_accept.sh
```

跨仓向量验证使用的本地 `MPC_OTSF` 提交为
`8405c879f5b30982510d4e82c64770530e2820db`。它只提供冻结测试数据，
并未执行该仓库的生产或集群脚本。新增 native 验收使用临时目录和真实
CPU 子进程，检查 drain、GPU 禁用时的 CPU 执行、环境隔离、完成与禁止重放。

本次没有构建或运行外部 C bridge，没有完成正式 FD-exec 后端和研究项目
全链路验收，也没有连接、部署或重启远程 HPDC。
