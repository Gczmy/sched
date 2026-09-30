# 旧实验部署绑定记录

历史 `sched_native_deployment/v1` 用于 M2B/Step5 专用身份、三阶段 argv 与
部署路径绑定。该格式、`export_native_deployment.py` 和配套冻结向量已归研究仓库
维护，不再是 sched 配置、导入 API 或安装依赖。

旧冻结文件的原始字节和摘要需要由研究项目自己的兼容记录保留；移动代码不代表
重新授权其运行。不得把项目模块安装进 `gsched` namespace，也不得通过
`PYTHONPATH`、`sys.path` 或 daemon 动态 import 恢复旧接口。

新的通用注册字段为 `config.execution_backends`，任务通过 `execution.backend`
选择管理员允许的可执行程序，输入相对项目 root 并绑定 SHA-256。
配置与使用见 [execution-api.md](execution-api.md)。

项目内部的 runtime、协议授权、业务身份和科学成功判据由外部 adapter 负责。
私有部署资料仍保留在仓库外，使用通用占位路径的边界见
[repository-hygiene.md](repository-hygiene.md)。旧持久态的保护见
[native-integration.md](native-integration.md)。
