# sched 开发范围与后续工作

本文只记录通用调度器工作。配置和 CLI 以 [reference.md](reference.md) 为准，
execution 以 [execution-api.md](execution-api.md) 及其同仓验收为准。
候选代码、验证、正式发布和生产部署分别记录，不能互相替代。

## 历史版本 0.2.1

[GitHub Release](https://github.com/Gczmy/sched/releases/tag/v0.2.1) 保存正式来源、
源码、默认/native wheel、安装说明、哈希与 CI 证据。运行时无第三方依赖；
默认安装要求 Python >= 3.10，原 native 包覆盖 CPython 3.10/3.14、
Linux x86_64、glibc 2.39。

该版本包含通用 FD 执行与原始 wait、持久 owner、认证重连、有限确认队列、
冷配置保留期、execution 详情/列表、本机 capabilities 和结构化 daemon 检查。
写库 schema 7，只读 schema 1–7；未知尝试和旧 session 不重放。
职责见 [execution-boundary.md](execution-boundary.md)。生产现状未查询，
不能把发布记录当作生产已升级的证据。

## 当前已发布 0.2.2

[v0.2.2 Release](https://github.com/Gczmy/sched/releases/tag/v0.2.2) 于 2026-10-01 发布，
来源为 `8aa2559dc64e74acd2cec6bcb2f5481d1d9fbc1d`。
[最终来源 CI](https://github.com/Gczmy/sched/actions/runs/36881160410) 的 14 项检查通过；
四份原始 ZIP 和三个说明/证据/校验和文件已从公开 Release 下载核对。
native 资产覆盖 CPython 3.10/3.14 × glibc 2.35/2.39 / Linux x86_64。

- 按 backend ID 预检 executable 和项目 root：有界 SHA-256、ELF 标识、
  读取漂移、文件类型与目录权限。错误码不输出私有路径，不授予后续执行权。
- 默认安装和回归覆盖 Python 3.10–3.14；native/真实执行与候选构建覆盖
  CPython 3.10/3.14、Ubuntu 22.04/24.04 / Linux x86_64。
  真实 syscall 拒绝用例验证不可用能力不会标为 verified，不弱化执行方式。
- 同仓脚本和手动 workflow 从最终 main 的成功 CI 校验四份原始 ZIP，
  生成总校验和、说明与 evidence，可续传匹配的草稿资产，不自动发布。

见 [0.2.2 Release 说明](releases/0.2.2.md) 与
[发布准备](release-preparation.md)。验收以独立选定的完整提交、最终 CI run
和实际产物为准；只有通过的矩阵才构成支持证据。正式发布不代表生产已切换。
写库仍为 schema 7，状态、FD4 identity、原始 wait 和未知结果守卫保持不变。

## 后续候选

- 更多 Linux ABI/架构须先有对应构建、探测、独立安装和实际执行验收，
  再扩大兼容范围；不把当前 x86_64 证据用于未验收环境。
- 配套客户端的 execution 展示归其仓库独立安排，继续消费公共 CLI，
  不重写 scheduler 语义或将实时多页浏览当作完整当前态。

维护与回退见 [execution-rollout.md](execution-rollout.md)。客户协议、
科学验收和研究部署由客户仓库独立维护，不作为 sched 发布前置条件。
已有 GPU 策略、资源准入、drain/resume 和幂等维护请求继续独立维护。

## 恢复工作流候选

当前候选协议见 [recovery-policy.md](recovery-policy.md)。候选已实现独立新尝试授权与持久
OOM FIFO；后续实现固定 12 GiB 默认剩余显存准入与分级、前台 daemon/supervisor 和完整故障验收。
这些后续配置暂不可用，不属于已发布 0.2.2 的能力。
