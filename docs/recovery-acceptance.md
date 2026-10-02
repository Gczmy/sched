# 恢复候选验收

本页验收已合并的协议/smoke、新尝试/OOM FIFO、显存准入/分级和前台守护。
不包含客户科学算法或特定客户仓库操作；不访问远程生产节点。

## 本地 Linux 与 CI

使用隔离临时 state，真实 CPU 子进程和显式构建的 native backend：

```bash
python -m pip install -e '.[test]'
python -m pytest -q -rs tests
SCHED_BUILD_NATIVE=1 python -m pip install -e '.[test]'
SCHED_REQUIRE_NATIVE=1 python -m pytest -q -rs tests
python tests/run_execution_accept.py
python tests/run_host_resources_accept.py
bash tests/run_docs_refs_accept.sh
python scripts/check_execution_boundary.py
python scripts/check_repository.py --all
```

CI 原有矩阵自动收集新增 test_*.py：默认 Python 3.10–3.14；native Python 3.10/3.14
与 Ubuntu 22.04/24.04；候选构建等待全部前置验证通过。未显式构建 native 的默认安装
可以跳过对应 native 故障用例；设置 SCHED_REQUIRE_NATIVE=1 后禁止这类跳过。
Darwin 专用用例及 native 已构建时的缺失安装用例按原契约跳过。

| 行为 / 故障 | 可复现证据 |
| --- | --- |
| smoke 需真实成功退出和 receipt，绑定代码、输入、配置、资源、时限和 probes | [test_recovery_protocol.py](../tests/test_recovery_protocol.py)、[test_recovery_queue.py](../tests/test_recovery_queue.py) |
| 独立 group 周期原子保存，catchable OOM 保存，坏文件/替换失败保留旧进度 | [test_recovery_protocol.py](../tests/test_recovery_protocol.py)、[recovery_worker.py](../tests/fixtures/recovery_worker.py) |
| A OOM 后 B/C 先完成，重复 A OOM 到队尾，FIFO/冷却/次数跨重启 | [test_recovery_queue.py](../tests/test_recovery_queue.py) 的实际 ordinary/linux_fd/linux_fd_owner 场景 |
| 所有恢复使用新 job version/attempt，旧 wait/未知事实保持 | [test_recovery_queue.py](../tests/test_recovery_queue.py)、[test_supervisor.py](../tests/test_supervisor.py) |
| 结构化 OOM 无关键词日志仍入队，但 checkpoint 不提供退出权威 | [test_recovery_queue.py](../tests/test_recovery_queue.py) |
| 固定 12 GiB，不同总容量不取 50%；外部占用必须显式许可 | [test_gpu_admission.py](../tests/test_gpu_admission.py) |
| 未知 topology/compute/util/free、残留、quarantine、releasing、ignore 拒绝 | [test_gpu_admission.py](../tests/test_gpu_admission.py) |
| 同 tick / outstanding / 缓存峰值预留、启动前重检、热更与崩溃前预留重接 | [test_gpu_admission.py](../tests/test_gpu_admission.py) |
| 分级等待、相同 checkpoint 不算进展、持久通知与可选停止 | [test_gpu_admission.py](../tests/test_gpu_admission.py) |
| 实际 SIGKILL daemon 后 supervisor 重启，persistent owner 同 attempt 原 wait | [test_supervisor.py](../tests/test_supervisor.py) |
| 实际 SIGKILL worker 从最近 durable checkpoint 恢复，原 rc=-9 与新版本保留 | [test_supervisor.py](../tests/test_supervisor.py) |
| stop 在 restart gap、signal、drain 正常退出；stale heartbeat 不抢活 lease | [test_supervisor.py](../tests/test_supervisor.py) |
| 原始持久 owner 丢失且 group 未清理时资源保留，group 消失后未知 wait 不伪造 | [run_execution_accept.py](../tests/run_execution_accept.py) persistent_loss / restart 场景 |
| 事务发布失败后的幂等收敛、取消和新代际胜出 | [test_supervisor.py](../tests/test_supervisor.py)、[test_recovery_queue.py](../tests/test_recovery_queue.py) |
| 完整旧 schema 只读不迁移、schema 9 原子升级、旧 retry 兼容 | [test_recovery_queue.py](../tests/test_recovery_queue.py)、[test_gpu_admission.py](../tests/test_gpu_admission.py) |

## 非生产真实 GPU 验收

上述 fake GPU 与真实 CPU/native 证据不证明真实 CUDA 压力表现。选择独立的非生产计算
节点及隔离 state，先运行 daemon check 和 capabilities，再用客户自行提供的 GPU 程序
完成匹配 code/config/input 的 smoke。客户程序需采用公开 checkpoint 协议，不导入客户
模块到 daemon。

使用可控的另一个进程占用部分显存，分别验证 allow_external_occupancy=false/true。
记录 nvidia-smi 同次 topology/compute/free/util 与 recovery 查询：11.9 GiB 不启动，
12 GiB 且预算满足才启动；不同总容量卡仍用固定阈值。设置声明峰值大于 12 GiB、同时
排队两组、延迟实际显存分配，核对预留与启动前重检拒绝超卖。外部程序继续分配可以
导致 OOM，因此此测试不承诺共享外部进程时永不 OOM。

让一个 group 在周期保存后 OOM 或被 SIGKILL，核对普通组先过完、新版本恢复且无重复
已完成结果；显式配置较高 tiers 后应等待显存改善。终止该隔离 daemon 并观察原 owner
wait/新 version 的区分，再验证 stop、drain 和项目 GPU 禁用。保存外部实验记录即可，
不把个人节点、任务名称、部署路径或生产记录提交到公开仓库。

手动 CUDA 验收入口（只在已授权、没有其他 compute 用户且至少有 20 GiB 空闲的 GPU 上运行）：

```bash
SCHED_BUILD_NATIVE=1 python -m pip install -e .
python tests/run_real_gpu_accept.py --gpu <index> --work-dir <new-private-directory>
```

该入口使用标准库 ctypes 调用 CUDA driver，执行 PTX kernel，并通过实际分配失败
产生可捕获 OOM；不依赖训练框架。它使用新的独立配置/state，测试进程均由自身持有，
只通过 CLI 操作调度器；结束后保留私有 state 和 evidence.json，不删除运行记录。
覆盖外部占用许可、12 GiB 门槛、20 GiB 恢复门槛、普通组优先/FIFO、预留、真实
worker/daemon SIGKILL、GPU 禁用、drain/resume/stop。节点、路径和完整原始输出只保存在私有证据中。

## 2026-10-02 真实 GPU 结果

上述手动入口已在隔离的 RTX A5000 / Python 3.12.3 / Linux x86_64 环境通过全部 8 项。
实测空闲显存为 11.898/12.148 GiB，分别阻止/允许派发；真实 CUDA 分配 OOM、20 GiB
恢复门槛、FIFO 无重复、预留、daemon/worker SIGKILL 和控制流程均符合断言。
测试完成后其独立 daemon 已停止，测试显存占用已释放。所用 runtime/native/验收
源码逐文件与已提交源码核对一致。私有配置、状态、路径和原始日志不进入公开仓库。
该源码构建证据不扩大正式 CPython 3.10/3.14 二进制资产矩阵，也不证明任意训练框架的科学正确性。

## 发布边界

四组分支按 A → B → C → D 审查/合并。本次 PR 完成不等于已发布新版本或已升级生产。
正式发布需独立版本号、固定最终提交、通过的 CI 和安装资产证据；不能覆盖既有 v0.2.2。
候选写库 schema 9，0.2.2 的 schema 7 二进制不能回接新写库，回退见
[execution-rollout.md](execution-rollout.md)。应用 checkpoint 保留策略见
[recovery-policy.md](recovery-policy.md)。
