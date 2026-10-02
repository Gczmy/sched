# SQLite 锁保持与事务失败处理

本页描述 0.3.1 的锁修复；0.3.0 不包含本补丁。正式发布事实以 Release 为准。
补丁不修改数据库 schema 或公开 CLI JSON 契约。生产安装必须另行安排维护窗口。

## 锁保持

SQLite 的 Unix VFS 使用 POSIX 锁。同一进程关闭同一 inode 的普通文件
描述符，会释放这个进程在该文件上的 POSIX 锁，即使锁属于其他 SQLite
连接。参见 [SQLite 官方说明](https://www.sqlite.org/howtocorrupt.html#posix_advisory_locks_canceled_by_a_separate_thread_doing_close)。

因此，SQLite 的 DB、WAL、SHM 不能使用通用的
`ensure_private_file()`（open/fchmod/close）修正权限：

- `ensure_private_sqlite_file()` 使用 lstat 和不跟随符号链接的 chmod，
  保留普通文件、inode 身份与 0600 权限检查，不创建缺失文件。
- 缺失数据库由 SQLite 在已校验的 0700 目录中创建，连接后立即修正权限。
  第二个连接的准备和关闭后的 sidecar 检查也使用专用逻辑。
- 私有只读快照的源 DB/WAL 拷贝在独立 Python 子进程中完成，避免同进程
  其他线程或连接的锁被源文件 close 释放。子进程不打开 SQLite；查询只打开
  私有快照。原有文件签名核验、WAL 配对和有界重试保持不变。

快照增加一次本地 Python 进程启动；部署验收需观察查询延迟。
此修复不证明任一历史事故的唯一根因，也不代替底层文件系统锁语义的验证。

## 失败诊断与重试

连接报告失败阶段 prepare/body/commit/rollback，以及进程 PID、SQLite 错误码、
错误名和 in_transaction。诊断不输出 SQL、任务名、业务 payload 或部署路径。
回滚失败另行记录，原始异常保留。业务事务不会自动重放。

只允许在 COMMIT 返回精确 SQLITE_BUSY（5）、且事务仍活动时重复 COMMIT，
最多追加三次，退避为 0.05/0.15/0.3 秒；SQLite 自身 busy_timeout 的等待另计。
这不会再次执行事务体或外部副作用。依据见
[SQLite COMMIT 语义](https://www.sqlite.org/lang_transaction.html)。

SQLITE_PROTOCOL、SQLITE_IOERR、SQLITE_BUSY_SNAPSHOT、事务已结束或缺少
错误码时均不重试。Python 3.10 缺少错误码属性时保守失败，不按错误文本猜测。
结果未知时继续使用原 request-id 和现有占位，通过 CLI 查证；不能换 ID 重发。

## 验证与回补

Linux 隔离回归位于 `tests/test_sqlite_locks.py`，覆盖 DB/SHM 锁保持、
第二个连接、同进程私有快照、竞争写入、符号链接/FIFO、提交/回滚失败和
COMMIT 重试边界；CLI/state 回归继续验证请求重放和初始化兼容性。

旧 schema 安装应基于实际部署源码准备单独回补，校验原始来源摘要，只改
权限、快照和事务处理，核验 schema 与无关函数保持一致。不能直接复制当前
state.py 到旧安装，也不能用当前版本 writer 打开生产旧库来验证回补。

维护窗口先使用实际部署 CLI 确认任务自然结束和未决启动收敛；支持 drain
时使用 `daemon drain --stop-when-idle`。旧版没有 drain 时等待自然空闲，
不能用 stop 代替排空。备份按现有正式流程完成；安装后通过 CLI 核对
schema、健康和请求结果，再运行已授权的独立 smoke。不得直接修改 state.db。
