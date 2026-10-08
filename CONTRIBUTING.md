# 开发与提交检查

调度器运行时仍只依赖 Python 标准库。测试需要 Python >= 3.10 和 pytest，
执行型测试在本地 Linux／WSL 或 CI 的 Linux runner 运行，不连接 HPDC。

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[test]'
python -m pytest -q -rs tests
python tests/run_execution_accept.py
```

公共 CI 在 Python 3.10–3.14 上运行完整公共回归和默认 wheel 独立安装，
并在 3.10 上运行 execution、mode、项目 GPU 开关和文档引用验收。
反馈修复的真实 CPU/CLI 隔离验收为 `python tests/run_feedback_accept.py`，
候选批次失败隔离为 `python tests/run_batch_policy_accept.py`；
同样纳入 3.10 CI。它使用临时配置/state 和 fake GPU，不读取生产配置，
不证明真实 GPU 或跨主机网关投递；集群手动运行必须进入既有计算节点租约。
native 矩阵覆盖 CPython 3.10/3.14 与 Ubuntu 22.04/24.04，显式设置
`SCHED_BUILD_NATIVE=1` 从本仓源码构建，运行 native/调度器回归和真实 syscall 拒绝验收。
默认安装不需要编译器，固定 FD backend 不可用时必须明确拒绝。

全部检查成功后，Candidate job 在两种 Python ABI 和两种 Linux runner 上构建四份
默认/native wheel 并独立安装，保存 30 天的候选 artifact。产物包含源码、manifest、
安装证据和 Release 草稿；不自动创建标签或发布。下载校验与 ABI 限制见
[execution-rollout.md](docs/execution-rollout.md)。开发构建工具固定版本在 workflow 中，
不影响运行时零第三方依赖。
Release 草稿准备流程和中断恢复验收见 [release-preparation.md](docs/release-preparation.md)；
常规 CI 不发布 Release 或部署生产。
两条路径均不读取客户合同路径，不要求另一个仓库存在。
客户 adapter、研究协议、科学加载和研究 gate 由客户自己的 CI 验证，
不能因其未通过把 sched 的通用公共验收跳过。

```bash
SCHED_BUILD_NATIVE=1 python -m pip install -e '.[test]'
python -m pytest -q -rs tests
python tests/run_execution_accept.py
```

## 提交前检查

以下入口仅依赖 Python 标准库和 Git，Windows 也可用 `python` 执行：

```bash
python3 scripts/check_repository.py --all
python3 scripts/check_execution_boundary.py
git add <files>
python3 scripts/check_repository.py --staged
```

`--all` 检查已跟踪文件的当前工作区内容；新文件先 `git add` 才纳入。
`--staged` 读取 Git index 中的原始 blob，不会用工作区的新内容替换暂存内容；
删除或重命名目标时，也检查未修改 Markdown 的引用。两种模式都检查本仓库
Markdown 文件／目录链接与 AGENTS 中的文档路径，不请求外部网站或校验标题锚点。
暂存模式检查暂存差异空白；全量模式检查工作区差异，CI 另传 `--base <full-sha>`
检查提交范围。返回码 `0` 为通过、`1` 为发现问题、`2` 为无法完成检查。

可启用版本控制内的提交钩子，在提交前自动检查暂存内容：

```bash
git config core.hooksPath .githooks
```

若已有其他 hooks，先合并调用，不覆盖已有配置。CI 在 push／pull_request 时运行
同一检查；它发生在上传之后，因此提交前的本地检查仍有必要。

## 隐私规则与例外

检查个人绝对家目录、常见服务令牌、私钥头、带密码的 URL、固定 screen 会话，
以及误入 Git 的本地配置、运行日志、状态数据库和生产诊断目录。测试、示例和
已跟踪生成文件同样检查；`user`、`example`、`tester`、`test`、`runner` 是通用
路径占位名称。超过 8 MiB 的文件和子模块不会被静默跳过，而会报告需处理。
输出只有文件、行号和规则名，不打印命中的内容。

任意任务名称、任意格式秘密以及编码后的内容无法仅凭这些规则可靠判断，仍需
审查新增配置和记录。本检查不扫描或净化 Git 历史。信息边界见
[repository-hygiene.md](docs/repository-hygiene.md)。

确需保留的虚构测试数据可以添加 `.repository-check.json`，顶层固定为
`schema_version: 1` 与 `exceptions: []`。每条例外必须指定已有的完整 `path`、
单条内容 `rule`、该行 UTF-8 字节（不含换行）的 `line_sha256` 和具体 `reason`。
不支持文件夹、通配符或关闭整个规则；改变样例内容会使旧摘要失效。
不要把真实敏感值写入例外说明，也不要为真实部署资料增加例外。

```bash
python3 -m unittest discover -s scripts -p test_repository_check.py -v
python3 -m unittest discover -s scripts -p test_execution_boundary.py -v
python3 -m unittest discover -s scripts -p test_release_candidate.py -v
```

检查器、测试和 `.githooks/pre-commit` 在 `sched` 与 `dsh-node-sched` 中保持逐字节
一致；修改时同步两份，并在各自独立 checkout 验证。两个仓库的公共 CI 不要求
另一仓库或私有研究仓库存在。GitHub Actions 固定到已核对的提交，只有读取权限。

`check_execution_boundary.py` 是 sched 独立的源码边界检查，不属于上述两仓共享的
隐私扫描器。它使用 AST 检查 runtime import、协议常量和阶段枚举，检查必要测试
的外部合同与搜索路径，以及向 `gsched` namespace 写入模块/路径的行为。
同时检查 native 源码、构建定义与 CI。历史文档和源码注释不定义 runtime 依赖；
旧持久态字段在兼容守卫中允许，但不能借历史键重新加入客户协议执行代码。
边界检查读取已跟踪和未忽略的新源码，删除的源文件不会继续视作当前实现。
