# sched execution 候选发布与部署清单

本清单用于交付准备。当前生产版本、配置和运行状态尚未查询；执行本清单前须取得对应部署授权。

## 发布验收

- 确认 PR 的 Repository、Python 3.10、Python 3.14 和 Optional Linux native 检查全部通过。
- 默认 wheel 不含 native extension，也没有第三方运行时依赖；native wheel 从本仓源码独立构建。
- 记录合并提交、wheel SHA-256、Python ABI 和构建方式。配套客户端仍通过公开 CLI 使用调度器。

候选产物必须从固定提交的独立源码副本构建，忽略本地 `.so`、测试状态与运行配置。
默认产物为 `sched-0.2.0-py3-none-any.whl`；native wheel 的 ABI/平台以实际构建结果为准。
0.2.1 运维候选使用不同版本与独立目录，不能将未合并分支包标为 0.2.0 合并发布。
候选目录保存 `manifest.json`、源码归档、两种 wheel 和安装说明；manifest 记录完整
commit、构建 Python/平台、各文件 SHA-256。发布标签应指向最终验收提交，不能只凭包版本
判断新旧源码；本轮只准备候选，不创建发布标签或切换实例。

在具备开发构建工具的本地 Linux 环境运行以下命令，可从指定完整提交生成并验证候选：

```bash
python scripts/build_release_candidate.py --commit <full-40-character-commit> --native
```

脚本通过 `git archive` 获取固定源码，分别构建和独立安装两种 wheel，并实际验证
持久 owner 的原始 wait。产物位于 `dist/candidate-<commit-prefix>`，已有目录拒绝覆盖。
在 manifest 中附加该提交对应的 CI 证据后，再由维护者明确授权正式发布。
默认 wheel 适用于 Python >= 3.10；本地已验证的 native 候选为 CPython 3.10/Linux x86_64，
不声明 manylinux 通用兼容。其他 ABI 须单独构建、安装验收并记录；不能复用 cp310 wheel。

安装后从无关目录验证 `sched --version`、`sched --help` 和 native 能力。默认安装不需要
编译器；native 安装须选择匹配解释器 ABI 的 wheel。两种安装都不需要客户仓库。

## 切换前核对

通过已部署 CLI 的 `--version`、`config get`、`status --json`、`history --json`、
`task <full-batch-id>:<task-id> --json` 和 `daemon status --json` 核对版本、配置和任务。
真实配置、节点、任务名称与检查结果保存为私有运行记录，不加入本仓。

旧 `strict` 和 native metadata 不会在新版本继续执行。切换前须处理旧队列与未解决启动，
不能通过 retry、clean、删除记录或重建名称绕过兼容守卫。如果现有 CLI 无法证明旧 session
状态，应先补充只读查询能力，再安排切换；不得直接读取共享源库或修改 state 文件。

## 维护窗口

登录统一使用 `ssh HPDC`；进入经实查确认的计算节点会话，核对主机与 `config.node`。
按项目操作规则由获授权的维护者执行 `daemon drain --stop-when-idle`，等待 running 与未决启动
清空、daemon 自然退出。`stop` 会取消任务，不能用它代替排空。

在独立版本目录安装已验收产物。仅在计算节点执行 `daemon check`；通过后再执行
`daemon resume` 和 `daemon start`，使用 CLI 检查健康与队列，运行另行授权的隔离 CPU 验收。
配置 backend 前审查 ELF、固定 argv/env、项目 root 与输入 slot，注册值均为冷配置。

## 回退边界

切换前保留旧安装、原配置和经正式备份流程取得的恢复点。
0.2.0 将写库 schema 升至 6，新增不可变 owner binding；0.2.1 候选升至 7，
新增确认队列与已记录健康。对应只读范围分别为 1–6 和 1–7。
旧版本不能被假定为兼容新写库。回退须在 daemon 排空后，
依据实际 schema 兼容性与已审查恢复方案执行，不能直接将旧代码覆盖到新库上。
任何已消费或结果未知的 execution attempt 都必须继续保留，不能因回退再次启动。
不能在持久 owner 仍运行或清理未决时移除原安装；存活服务仍使用原提交的代码。
默认配置不会自动启用持久 backend；启用前先验证客户程序的 FD4 owner identity 支持。

客户协议和科学验收由客户仓库独立发布；进程退出 0 不代表科学验证通过。
