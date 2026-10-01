# sched execution 发布与部署清单

本清单用于交付准备。当前生产版本、配置和运行状态尚未查询；执行本清单前须取得对应部署授权。

当前正式版本为 [v0.2.2](https://github.com/Gczmy/sched/releases/tag/v0.2.2)，
发布来源为 `8aa2559dc64e74acd2cec6bcb2f5481d1d9fbc1d`；
[最终来源 CI](https://github.com/Gczmy/sched/actions/runs/36881160410) 的 14 项检查全部通过。
七个原始资产已从公开 Release 下载核对，生产切换独立安排。

## 发布验收

- 确认最终提交的 Repository、所有 Python/native 矩阵及 Candidate 检查全部通过。
- 默认 wheel 不含 native extension，也没有第三方运行时依赖；native wheel 从本仓源码独立构建。
- 记录合并提交、wheel SHA-256、Python ABI 和构建方式。配套客户端仍通过公开 CLI 使用调度器。

候选产物必须从固定提交的独立源码副本构建，忽略本地 `.so`、测试状态与运行配置。
当前默认产物为 `sched-0.2.2-py3-none-any.whl`；native wheel 的 ABI/平台以实际构建结果为准。
候选目录保存 `manifest.json`、源码归档、两种 wheel、安装说明和 `RELEASE_NOTES.md`；
manifest 记录完整 commit、构建 Python/平台、各文件 SHA-256 与独立安装证据。
发布标签应指向最终验收提交，不能只凭包版本
判断新旧源码。候选构建本身不创建发布标签、Release 或生产切换。
manifest 的 candidate/published/deployed 记录构建时状态；之后的正式发布事实由
Release 与 tag 记录，发布时保留已经验收的包与 manifest 原始字节。

## CI 产物与下载验证

所有 job 使用同一源码提交：PR 使用 head SHA，push 和手动运行使用事件 SHA。
只有 Repository、Python 和 native 三组检查全部成功后，Candidate job 才开始。
0.2.2 按 Python 3.10/3.14 与 Ubuntu 22.04/24.04 保存四份 CI 产物，
名称含完整提交、Python 版本、runner target 和运行 attempt，
保留 30 天，不覆盖已有 artifact。每份包含默认 wheel 与对应 ABI 的 native wheel；
各份中的默认 wheel 不要求逐字节一致，分别以各自 manifest 的哈希为准。

manifest 的 `ci` 只保存公开的仓库、事件、源码/工作流提交、run 链接和前置 job 结果，
不会把构建中的整次 workflow 标为成功。整次 run 完成后，另查最终结果；上传成功的
artifact ID、URL、归档 SHA-256 和 manifest SHA-256 记录在 job summary。
下载归档时先与 GitHub 记录的 artifact digest 核对；解包后使用已审查源码中的校验器：

```bash
python scripts/verify_release_candidate.py <candidate-directory> --commit <reviewed-full-commit> --require-ci
```

预期提交必须从已审查的 PR 或最终 `main` 独立选定，不能从待验 manifest 自行取值。
校验器只读文件，不要求 Linux 或 native 模块；检查文件集合、哈希、归档提交、源码版本/
schema、wheel 元数据/ABI 与安装证据。它不替代对 GitHub run 最终结果的核对。
PR 产物对应该分支提交；合并后须使用新 `main` 提交的 CI 产物准备正式发布。
CI 产物会过期，正式发布前须取得独立授权并保存已验收包、摘要与对应 CI 记录。

0.2.2 增加同仓发布准备脚本和手动 workflow，验证来源完整成功矩阵，保留原始 ZIP，
生成总校验和与 evidence，可选择上传来源一致的草稿。中断后只复用经过哈希核对的
匹配资产；不覆盖已发布版本，不自动发布。入口与离线元数据契约见
[release-preparation.md](release-preparation.md)。

## 本地候选

在具备开发构建工具的本地 Linux 环境运行以下命令，可从指定完整提交生成并验证候选：

```bash
python scripts/build_release_candidate.py --commit <full-40-character-commit> --native
python scripts/verify_release_candidate.py dist/candidate-<commit-prefix> --commit <full-40-character-commit>
```

脚本通过 `git archive` 获取固定源码，分别构建和独立安装两种 wheel，并实际验证
持久 owner 的原始 wait。产物位于 `dist/candidate-<commit-prefix>`，已有目录拒绝覆盖。
本地构建不冒充 CI 产物，正式发布仍须核对该提交对应的完整 CI 并取得明确授权。
开发构建工具版本见 workflow；它们不成为运行时依赖。
默认 wheel 要求 Python >= 3.10，调度执行仍要求 Linux/POSIX。
0.2.2 CI 默认安装/回归覆盖 Python 3.10–3.14；native 候选为 CPython 3.10/3.14 /
Linux x86_64，runner 为 Ubuntu 22.04/24.04；真实 seccomp 故障验收覆盖 execveat 缺失、
close_range、memfd 和 UNIX socket 权限拒绝。尚未通过的矩阵不扩大已发布支持范围。
实际 libc 和 SOABI 写入各自 manifest，不声明 manylinux 通用兼容。
其他 ABI 或 Linux 基础环境须单独构建、安装验收并记录；不能复用不匹配的 wheel。
已发布 0.2.1 的 native 产物仍只覆盖原来的 CPython 3.10/3.14 与 glibc 2.39，
0.2.2 正式资产覆盖 CPython 3.10/3.14 与 glibc 2.35/2.39，
不改变旧 Release 的产物或兼容性声明。

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
0.2.0 将写库 schema 升至 6，新增不可变 owner binding；0.2.1 升至 7，
新增确认队列与已记录健康，0.2.2 保持 schema 7。对应只读范围分别为 1–6 和 1–7。
旧版本不能被假定为兼容新写库。回退须在 daemon 排空后，
依据实际 schema 兼容性与已审查恢复方案执行，不能直接将旧代码覆盖到新库上。
任何已消费或结果未知的 execution attempt 都必须继续保留，不能因回退再次启动。
不能在持久 owner 仍运行或清理未决时移除原安装；存活服务仍使用原提交的代码。
默认配置不会自动启用持久 backend；启用前先验证客户程序的 FD4 owner identity 支持。

客户协议和科学验收由客户仓库独立发布；进程退出 0 不代表科学验证通过。
