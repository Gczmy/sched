# 发布准备

`scripts/prepare_release.py` 仅依赖 Python 标准库。它准备原始 CI 包、总校验和和
来源证据，可选择上传 Release 草稿；不发布 Release，不部署生产。
0.2.2 的历史发布事实以 [0.2.2 Release](https://github.com/Gczmy/sched/releases/tag/v0.2.2) 为准。
0.3.0 的正式来源、资产和发布事实以其 Release/tag/evidence 为准。
0.2.2 来源为 `8aa2559dc64e74acd2cec6bcb2f5481d1d9fbc1d`，七个原始资产已经公开下载核对。
发布后的文档提交不改变该标签或资产；本脚本拒绝修改已发布版本。

## 在线准备

先独立选定已审查的完整 main commit 和该提交完成的 main push CI run。
整套 14 项矩阵必须成功，且四个候选 artifact 未过期。使用相同来源提交中的脚本：

```bash
python scripts/prepare_release.py --commit <full-reviewed-commit> --run-id <successful-ci-run-id> \
  --version <new-package-version> --output dist/release-prepared
```

`GH_TOKEN` 可通过现有环境提供，下载和草稿上传仅在内存使用；脚本不保存 token、
认证 header 或临时存储 URL。匿名公开读取可能受到 GitHub API 限制。
版本号须与该来源的候选包一致，且不能覆盖已发布版本；当前源码包版本与已发布
0.4.0 相同，须先确定新的正式来源/版本，不能直接以 0.4.0 准备或替换旧资产。
默认 repository 为 `Gczmy/sched`；其他正式仓库须显式传 `--repository owner/name`。

脚本核对 repository、当前 main、完整 commit、workflow、run/attempt、全部 job、
artifact ID/name/digest 与其包内 CI/ABI/libc/安装证据；先校验原 ZIP 哈希，再按有界
平面布局解包验证，不接受路径穿越或 symlink。输出四份原始 ZIP，以及
`release-evidence.json`、`RELEASE_NOTES.md`、`SHA256SUMS`。不重新打包 wheel 或修改 manifest。
默认 wheel 可能逐字节不同，分别保留各自 manifest，不能跨包混用哈希。
新增 target 时须同步 CI 和脚本的完整矩阵，不能仅凭版本标签扩大兼容范围。

目录在全部验证通过后原子创建。完全相同的准备可以重复执行；已有目录的文件集合或
字节不同会拒绝覆盖。失败不留下部分产物目录，也不改已有 Release/tag。

## 手动 workflow 与草稿

在 `main` 上手动运行 `.github/workflows/prepare-release.yml`，填写完整 commit、
成功 CI run ID 和 package version。`upload_draft` 默认为 false，仅保存已验收资产。
选择 true 时，在重新核对 main、CI attempt 和 artifact identity 后上传草稿。
workflow 不取消执行中的准备，以免自动中断写请求；常规 CI 保持只读权限。

本地对应参数为 `--upload-draft`，需 `GH_TOKEN`。脚本使用来源与 evidence 摘要绑定
草稿；同一操作中断后，重复运行复用完全匹配的草稿和已上传资产。
响应丢失不代表上传失败；已有资产必须处于 uploaded 且 size/SHA-256 一致，才可跳过。
其他草稿、已发布 Release、未知上传状态、额外资产或摘要冲突均拒绝覆盖，
不会删除重建资产、强移标签或自动转为正式发布。

## 离线核验与保存

可从独立 GitHub 查询保存公开 metadata：`repository`、完整 `run`、`main_sha`、
可空 `tag_sha`、该 run attempt 的 `jobs` 和该 run 的 `artifacts`。原 ZIP 按
`<artifact-id>.zip` 保存到一个目录，然后传 `--offline-metadata <file>` 与
`--artifacts-directory <directory>`，其余来源参数不变。

离线 evidence 明确标记 `metadata_source:offline`，只验证所提供的数据和原字节，
不证明 GitHub 的当前状态，不能用于草稿上传。正式发布前保存全部已验收资产和
CI 来源记录，并另核对最终 Release/tag；CI artifact 保留 30 天。
安装和 schema 回退边界见 [execution-rollout.md](execution-rollout.md)。
