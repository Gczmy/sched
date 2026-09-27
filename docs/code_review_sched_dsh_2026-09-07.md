# sched / dsh-node-sched 联合代码审查（2026-09-07）

本文是当时版本的修复与验证记录，测试计数、构建摘要和部署说明不代表当前结果。
现行文档见[文档索引](README.md)。

本轮审查发现并修复了产物误复用、并发配置覆盖、写操作重试绑定变化、旧 SSH
连接继续复用等问题，同时补齐 CLI 输入、通知、分页与本地传输的边界检查。
本轮仅修改代码与文档，未部署到 HPDC。

## 范围

覆盖两个仓库的手写实现、构建入口、配置与文档契约，并结合现有 Python、Node
回归和 shell 验收交叉核对。生成的前端 bundle 由源码重建。

| 范围 | 核对内容 |
| --- | --- |
| `gsched/cli.py`、`state.py` | 主机守卫、只读快照、提交门禁、revision、幂等请求、配置写入 |
| `daemon.py`、`dispatcher.py`、`allocator.py`、`executor.py` | daemon 身份、恢复、取消与超时、依赖解锁、配额／共享 GPU、进程组与产物收敛 |
| `config.py`、`schema.py`、`templates.py`、`fingerprint.py`、`artifacts.py`、`notify.py` | 输入边界、运行环境、缓存指纹、路径约束、通知 |
| `dsh-node-sched/packages/node-sched/lib/` | SSH 与连接池、主机信任、浏览器认证、上传与写转发、分页、日志与资源释放 |
| `dsh-node-sched/packages/node-sched-ui/src/` | 认证恢复、请求持久化、状态新鲜度、分页、任务操作绑定 |
| 两侧测试、README、reference、implementation notes、构建脚本 | 可复现性与跨仓库契约一致性 |

## 修复记录

P1 表示可能影响任务结果、写操作语义或目标身份；P2 表示输入、可用性或诊断问题。

| 级别 | 问题与触发条件 | 修复与验证入口 |
| --- | --- | --- |
| P1 | 相同命令改变 `batch.env`／`task.env`／默认环境、工作目录或产物声明后，旧指纹仍可能匹配并 SKIP | 指纹纳入声明的合并环境、真实 cwd、任务与 stage 产物规则；预览、提交、inbox、resubmit、派发共用相同输入。`test_review_execution_inputs.py`，B13 实际执行计数与 checkpoint 验收 |
| P1 | 两个 `config set` 同时读取旧配置并共用 `.tmp-set`，可能丢失其中一个补丁或相互覆盖临时文件 | 整个读／合并／校验／替换过程使用 submission gate。`test_review_config_concurrency.py` 并发补丁保留两个修改 |
| P1 | 看板只持久化 request ID，刷新后重试改用新 revision；代理在 CLI 读取旧回执前拒绝状态变化 | 持久化完整请求并原样重放；代理继续核验 writer 与有效快照，CLI 在回执查询之后原子比较前置条件。两侧 `review-*-contracts.test.js` |
| P1 | SSH `255` 或信号退出被当作明确失败，浏览器清除 ID、后端删除上传内容，后续可能以新 ID 重发 | 不确定退出与不完整成功响应保留绑定和上传内容。Node 回归覆盖 `-1`、`75`、`137`、`255` 和失败的 `code:0` 响应 |
| P1 | 外部进程修改／删除 SSH 主机配置后，缓存连接或独立日志／终端入口仍可能使用旧目标／旧 pin | 每次复用或建立连接前刷新主机存储代际并使旧连接失效。`host-store.test.js` 验证已删除 alias 不执行命令 |
| P2 | `sched run` 接受非正资源参数、preview 绕过项目校验；有效的带引号参数被错误拆解，登录 Bash 可覆盖 venv PATH | 统一输入校验，保留 argv 引号，使用非登录 Bash。`test_review_execution_inputs.py` 实际执行临时解释器并检查参数 |
| P2 | GPU 容量接受 NaN／Infinity、卡号为负、打包数被静默取整 | 配置与 `gpu-set-mem` 提前拒绝非法值。`test_review_config_numeric.py` 与 GPU／配额验收 |
| P2 | dry-run 产物检查允许父目录符号链接，可能宣称 SKIP，而执行期拒绝 | 预览复用具有路径边界的 `check_artifacts`。`test_review_execution_inputs.py` |
| P2 | `notify-ack` 可重命名任意传入文件；合法 JSON 数组等损坏事件使整个 inbox 查询报错；interrupted 缺少失败明细 | 限制当前节点事件目录与普通文件，逐条报告错误，补 interrupted 明细。`test_review_notification_boundary.py` |
| P2 | 合法长 job ID／GPU 任务引用被代理拒绝；依赖数量错误地受页面 limit 约束；截断页缺 cursor 被前端误收敛为完整结果 | 调整字段边界并拒绝无有效后续 cursor 的截断页。两侧分页契约回归 |
| P2 | 本地日志在消费者挂接前无限缓冲；shell 退出但管道未关闭时，timeout／dispose 仍保留 PID 供发送信号 | 限制早期缓冲，进程退出后停止使用其 PID，流关闭补 SIGKILL 收尾。`transport.test.js` |
| P2 | Windows 构建使用 URL pathname 产生错误盘符路径；文档误称原生 Windows 可运行 host；CRLF 和测试时钟使跨平台验证失败 | 构建使用 `fileURLToPath`，明确 WSL2 运行边界并在创建凭据前报错；用 `.gitattributes` 固定 LF，修复验收解释器路径和设备过期测试时钟 |
| P2 | 构建依赖 `esbuild 0.24.2` 命中开发服务器跨域读取漏洞；本仓库只使用 build，未启动受影响的 serve 功能 | 升级至 `0.25.12`，更新锁文件与生成 bundle；全依赖 `pnpm audit --json` 返回 0 个已知漏洞，双平台构建一致 |

依赖问题依据上游 [GHSA-67mh-4wv8-2f99 安全公告](https://github.com/evanw/esbuild/security/advisories/GHSA-67mh-4wv8-2f99)；修复版本从 `0.25.0` 开始。

## 兼容影响

指纹格式升级后，旧 producer／stage checkpoint 不再匹配。新提交或重开的任务
会重新执行一次；不会仅因升级把已完成任务自动入队。未声明的宿主环境和 untracked
源码仍不参与指纹，影响结果的环境应通过配置、batch 或 task 显式声明。

旧浏览器数据库中的未决 ID-only 请求没有原始前置条件，不能安全自动补建。
遇到此记录会要求先核对结果，保留原 ID；新请求完整持久化。

原生 Windows host 尚无 POSIX 私有权限与目录 fsync 的等效实现。本次明确使用
WSL2 并将凭据保存在 Linux 文件系统，未通过放宽权限校验宣称支持原生运行。
原生 Windows 可以构建浏览器 bundle。

## 验证

运行环境为 WSL Ubuntu 22.04 / Python 3.10.12 / Node 22.23.2；原生 Windows
使用 Node 24.16.0 检查构建与平台提示。

| 检查 | 最终结果 |
| --- | --- |
| Python 全量回归 | 357 项：356 通过，1 项 Darwin 专用检查在 Linux 跳过；最后的提示文案修改另通过 5 项执行输入回归 |
| Node 全量回归 | 266 项全部通过，无失败、取消或跳过 |
| `tests/run_*_accept.sh` | 51 个脚本逐项通过，无遗漏 |
| Windows / Linux 前端构建 | bundle 与 source map 的 SHA-256 分别一致 |
| 全依赖 `pnpm audit --json` | 包含开发依赖，0 个已知漏洞 |
| 静态检查 | 两仓库 `git diff --check`、本次相关文档的本地链接、指定源文件 LF 检查通过 |

首轮 B13 与 unmanaged 恢复脚本超过外层 240 秒总时限，改用 900 秒上限后完整通过；
两项 cancel 验收的解释器路径问题修复后也已重跑通过。上表采用每个脚本的最终结果。

构建产物 SHA-256：

```text
client.js      743ab97f002b4f11d62b955526489644bd624e05226d7a9c0ecd35e792ff141b
client.js.map  a39c2ff945b2ff016b7e7004c20be75c0c8307a6181e1a8c7f5d0a655aefb931
```

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_*.py'
for testfile in tests/run_*_accept.sh; do bash "$testfile" || exit; done

# 在 dsh-node-sched 仓库内
node --test packages/node-sched/test/*.test.js packages/node-sched-ui/test/*.test.js
node scripts/build-client.mjs
pnpm audit --json
git diff --check
```

没有连接 HPDC 或操作生产任务。真实 GPU、NFS、远程 screen 和实际 SSH 端点未做
部署验收；本地回归覆盖 fake GPU、进程生命周期以及可控的 SSH／WebSocket 替身。
