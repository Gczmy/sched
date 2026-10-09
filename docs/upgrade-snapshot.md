# 升级窗口恢复点（源码候选）

命名合同 `sched-upgrade-snapshot/v1`。只提供同一实例、尚未恢复写入/启动任务的
升级窗口回退，不支持任意历史回滚，不自动启动 daemon，不恢复外部研究产物。
CLI 写库仍为 schema 25；完整 schema 10–25 且已有原 instance 的库可创建恢复点。
低于 10、身份缺失、不完整或较新的库拒绝，不在创建/验证时自动迁移或补身份。

## 操作顺序

在实际计算节点，先停用旧版写入端并无损排空生产 daemon。旧客户端不认识新增门禁，
不能声称它们已被强制隔离。确认旧 CLI、看板 writer、前台 supervisor 和后台写脚本
不再运行；`--writers-quiesced` 是对这个实际操作的显式确认，不是系统自动证明。
`daemon drain --stop-when-idle` 后等待自然退出；不能用会取消 running 的 stop 代替排空。

使用候选 CLI（配置与 state 必须仍指向原实例）：

```sh
sched snapshot create --writers-quiesced --yes --json
sched snapshot verify <snapshot-id> --json
sched snapshot migrate <snapshot-id> --yes --json
```

create 返回唯一 32 位小写十六进制 ID，打开持久维护窗口。所有认识门禁的写入、
网关投递、初始化、后台 start、foreground/supervise 和直接 dispatcher 入口被拒绝。
普通只读查询仍可读原 DB/WAL 私有快照；缺库或未完成替换不会初始化替代库。
`snapshot status --json` 只报告窗口，不打开数据库。verify 只校验保存镜像，
`rollback_authorized:false`，不证明当前状态仍可回退。migrate 是窗口内唯一的数据库写入，
只运行候选 schema 迁移，并在前后核对原记录与审计过的默认新增对象。
`daemon check` 涉及可写目录探测，需在关闭维护窗口后、实际计算节点执行。

升级接受后：

```sh
sched snapshot close <snapshot-id> --yes --json
sched daemon check --json
sched daemon resume
sched daemon start
```

若升级失败且窗口仍未关闭：

```sh
sched snapshot rollback <snapshot-id> --yes --json
```

rollback 返回 restored 后窗口仍然开启，数据库为原 schema，配置/其他节点文件不变。
先切回兼容原 schema 的旧独立安装，再用候选恢复点 CLI 关闭窗口；关闭本身不迁移数据库。
随后才由旧安装检查、resume/start。不能使用新 CLI 的普通写命令代替关闭，或在回退后
用新 writer 的自动初始化再次升级。close 永久消费该 ID 的回退权；再次创建窗口也不能
激活旧点。关闭不等于启动；没有自动启动或自动重放任务。

## 保存和拒绝边界

恢复点位于 state 根的私有 `.snapshot-control/<node>/points/<id>`，目录 0700、文件 0600。
稳定共享/独占门禁位于节点树外，不随 DB 替换改变 inode；锁获取有界，忙时拒绝。
源数据库通过已有私有 DB/WAL 复制协议读取，再将完整已提交页备份成自包含 SQLite 镜像；
绝不单独复制 DB 丢弃 WAL。原 instance、schema、逐表/逐列/完整行多重集合摘要、配置原字节、
节点树的普通文件与目录清单一起绑定。源路径、目录 dev/inode、UID 和实际计算主机固定。
镜像 SHA256、SQLite quick_check 与重新计算的数据库事实均须通过。

节点 DB/WAL/SHM 之外的文件/目录必须保持不变，包括 inbox、回执 ticket、通知确认、marker
和日志。配置变化、新增/删除文件、追加回执（含 unknown）、任务/版本/执行/资源事件变化、
未经审计的新表/列均拒绝回退。只允许空新增审计表和原记录上的迁移默认列；不删除未知记录。
限制为 10,000 文件/目录、单文件 128 MiB、节点文件总量 512 MiB、数据库事实 500,000 行/
256 MiB；扫描/备份有界。超限不返回部分成功，也不能据此降低保护条件。

daemon/前台 owner 或启动文件存在、running、GPU/CPU 活动分配、未决 execution/owner cleanup、
CPU/device scope 未终结时拒绝创建。旧 native session 没有充分清理证明时也拒绝；不依据
当前 PID 不存在、日志或产物猜测原执行已完成。历史 unknown 请求事实可保留，但旧写入端
必须实际停止，不能把恢复点当成隔离同 UID 不合作进程的安全沙箱。

rollback 在实际替换前保存原 DB/WAL/SHM 摘要和持久 intent，将回退前文件逐一原样移至
`before-rollback`，最后原子安装完整镜像。原始文件和 point 都保留，不把新库直接删掉。
中断后窗口保持 restoring，重复同 ID rollback 只按原 journal/digest 续接，不重新提交任务。
中断恢复须仍满足文件/实例绑定；发生漂移则保留现场并拒绝，不猜测或自动覆盖。
restoring 时禁止 close。创建失败保持 creating 并冻结写入，可显式 close 放弃未完成点；
不允许拿未完成点回退。恢复点清理/保留期 CLI 尚未实现，不手工删除受管恢复记录。

## 验收范围

[私有回归](../tests/test_snapshot.py) 覆盖完整 schema 10→25→10、unknown 回执保留、
原身份/文件绑定、镜像损坏、写门禁、关闭权消费和故障注入替换续接；
[数据库事实回归](../tests/test_snapshot_facts.py) 核对追加/修改/替换记录和扫描上限。
[Linux CLI 验收](../tests/run_snapshot_accept.py) 检查独立进程写入/启动门禁、
pending 保留、显式关闭/恢复后 CPU 执行一次和旧点不可复活。加入 CI 不等于已经验收，
合成迁移/计算节点私有测试也不等于生产恢复点已创建、正式发布或生产已切换。

2026-10-09：计算节点 Python 3.12 的独立临时 state 通过上述 3 组 CPU/CLI 验收，
191 条相关回归全部通过，其中 29 条为新恢复点/数据库事实回归。中断续接使用私有
文件事务故障注入，不冒充真实断电或 NFS 故障验收。生产 daemon 未修改/重启；
新的固定源码完整 CI、发布产物与生产恢复点仍须后续验证。
