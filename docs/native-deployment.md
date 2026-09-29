# 实验 native 协议的部署绑定

Step5D／Step5E／Step5F 不再在公开源码中保存部署专用解释器路径、项目、批次和
任务名称。可信 bootstrap 在创建 owner 前加载独立的私有文件，将同一个不可变
`NativeDeployment` 显式传给协议和 owner。配置缺失或摘要不符时失败，不读取
环境变量、当前目录或请求中的配置，也不提供默认部署。

这次新增的是配置格式 `sched_native_deployment/v1` 和显式 Python 参数。
冻结的 wire schema、协议摘要和报文布局保持不变；加载原部署的完整绑定后，
原请求、Step5D 投影和 Step5E 冻结向量保持一致。更换部署值不等于旧 native
verifier 已支持新部署；它仍须独立持有并检查同一组经审查的绑定。

## 配置与信任来源

格式见 [虚构示例](../examples/native-deployment.example.json)。五个顶层字段全部
必需，拒绝额外字段、重复字段和未知版本。`logical_argv_profiles` 必须完整覆盖
三个阶段；每个 argv 使用绝对解释器路径和 `-I -S`。两个名称模板各包含一个
`{phase}`，展开后逐项精确匹配，不是请求可选择的通配模式。脚本路径后的
空字符串参数按 Step5D wire 原样保留，不在 launch plan 中丢弃或拒绝。

```python
from gsched.native_deployment import load_deployment
from gsched.native_step5d_control import NativeStep5DRequestOwner

# Both values come from the administrator's reviewed bootstrap configuration.
deployment = load_deployment(private_file, expected_sha256=reviewed_file_sha256)
owner = NativeStep5DRequestOwner(deployment=deployment)
```

SHA-256 覆盖文件原始字节，包括换行。文件路径和预期摘要必须来自独立管理员
配置，不能取自 batch、请求、anchor，也不能在每次请求时自行计算摘要后批准。
加载后只保留不可变数据；修改磁盘文件不会改变已创建 owner 的绑定。

这是独立实验 API 的冷配置，不是 `sched config set` 的新配置项，不增加 daemon
热加载入口，不启用 V2，也不授权任何逻辑 Python 或 native 进程执行。

## 从原冻结合同迁移

`scripts/export_native_deployment.py` 只解析已审查、已固定文件摘要的 Step5D
合同，不执行研究仓库代码。输出文件必须不存在，父目录必须已存在。示例：

```bash
mkdir -p .local
python3 scripts/export_native_deployment.py \
  --contract /path/to/private-contract.json \
  --contract-sha256 <reviewed-contract-file-sha256> \
  --output .local/native-deployment.json
```

脚本仅输出生成配置的 SHA-256；审查并将该摘要固定到可信 bootstrap。
私有文件放在仓库外或已忽略的 `.local/`，不要覆盖公共示例。
迁移不改写原冻结合同、向量、历史提交或归档标签。

## 调用方迁移表

| 入口 | 必需变更 |
| --- | --- |
| Step5D `build_request_body`、`validate_request_body`、`encode_request_frame`、`request_digests`、`scheduler_prefix`、`alignment_projection` | 显式传入 `deployment=deployment` |
| Step5D `NativeStep5DRequestOwner` | 构造时传入同一份绑定，后续准备与交换沿用它 |
| Step5E `parse_anchor`、`parse_request`、`parse_embedded`、`request_envelope`、`prefix`、`root_identity` | 显式传入绑定；内嵌 Step5D 请求也按该绑定校验 |
| Step5E `startup_message`／`validate_startup` 的 `INIT` | 必须提供绑定，其他 startup 消息没有部署字段 |
| Step5E `NativeStep5ESender` | 构造时传入绑定，在发送 READY 前检查 INIT |
| Step5F `prefix`、`validate_request`、`canonical_request`、`parse_request`、`RequestOwner` | 显式传入绑定；nonce 消费与单次使用规则保持不变 |
| 原 `LOGICAL_ARGV_PROFILES[phase]` | 改为 `deployment.argv(phase)` |
| Linux 内部 `_project_root_expectation` | 显式传入绑定，root 的项目名称取自该配置 |

仓库内调用和测试已迁移。外部按 revision／源码摘要固定的隔离启动器继续使用
各自冻结版本；升级到新源码时需要采用以上 API，并把 `gsched/native_deployment.py`
及其导入依赖纳入审查后的源码清单。不要只改 scheduler revision 后运行旧启动器，
也不要从请求中反推配置来让旧调用通过。该外部启动器升级与 C bridge 执行验收
不由本次纯协议迁移测试替代。

## 验证

公共测试不需要部署配置或研究仓库，使用虚构路径与身份：

```bash
python3 -m pytest -q tests/test_native_deployment.py tests/test_native_step5e_transport.py
```

`tests/fixtures/native-deployment-v1-vectors.json` 固定三阶段的 Step5D body／frame、
Step5E anchor／request 和 Step5F request 摘要。它们是公共配置的测试向量，不是
旧部署冻结向量的替代品。

可选的旧部署兼容性检查仍使用 `M2B_STEP5E_CONTRACT_ROOT`。测试只从摘要固定的
外部合同提取预期绑定，独立验证原向量，并检查迁移前 Step5D 投影摘要：

```bash
M2B_STEP5E_CONTRACT_ROOT=/path/to/private-research-checkout \
  python3 -m pytest -q tests/test_native_step5e_protocol.py
```

此项验证不连接 HPDC，不编译或执行外部 C bridge，不启动正式 V2 调度路径。
