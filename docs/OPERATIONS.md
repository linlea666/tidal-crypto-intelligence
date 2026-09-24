# 运行与更新

## 服务器目录

`/opt/tidal/data` 是持久数据；`secrets/password_hash` 保存 bcrypt，不使用 SSH 登录密码；`current` 指向版本配置；`state.env` 是不可变镜像摘要。`previous-image`、`backups` 和 `deployed-version` 用于恢复与审计。

应用用户 UID 10001，容器只读根目录、无额外 Linux capabilities，内存上限 768MB；Nginx 上限 96MB。应用端口仅绑定环回，HTTPS 网关对外提供 8443，80 用于 ACME 验证和跳转。

正式版本发布前，在 GitHub 仓库的 production 环境 / Secrets 配置：`DEPLOY_HOST`、`DEPLOY_SSH_KEY`、`DEPLOY_KNOWN_HOSTS`。不把这些值放入本仓库。受限部署用户不属于 Docker 组；SSH 禁止端口转发和交互 shell，只可 sudo 执行经参数检查的发布入口。

## 更新

1. 本地修改并跑检查，提交 / PR 到 GitHub。
2. 合并 `main`，创建并发布正式 Release，如 `v1.0.1`。
3. 查看 `Release and deploy` 工作流。成功后可在数据健康页核对版本。
4. 首次 GHCR 包默认为私有时，先将该包设为 Public，再重跑部署作业。服务器不保存 GHCR 登录凭据。

发布脚本检查剩余磁盘、备份 `state.sqlite`（SQLite 在线 backup，包括 WAL 中已提交事务）、核对镜像提交、拉取摘要和检查真实市场数据就绪。失败时恢复原镜像与 Nginx / Compose 配置。历史数据库采用追加式兼容 schema；将来的破坏性 schema 变更必须单独设计迁移，不能只回退镜像。

## 日常维护

```bash
docker compose --env-file /opt/tidal/state.env -f /opt/tidal/current/compose.yaml ps
cat /opt/tidal/deployed-version
systemctl list-timers tidal-renew.timer tidal-metrics.timer
```

证书使用 Let’s Encrypt IP shortlived profile（6 天）；`tidal-renew.timer` 每日检查两次，续期后重载 Nginx。端口 80 必须持续允许外部 ACME 验证。可通过 `journalctl -u tidal-renew.service` 检查续期失败。不要停止续期定时器。

`tidal-metrics.timer` 每 5 分钟保存资源、磁盘和健康信息到 `metrics/YYYY-MM-DD.jsonl`，7 天自动删除，同时把镜像、日志和备份占用计入应用预算。Docker 日志总量有上限；宿主机维护任务清理超过 7 天的日志。

## 数据保护与降级

设置页只能选择 30 / 90 天。数据清理先完成 15 分钟汇总，再删除旧 1 分钟分区；重启从持久化汇总进度续算。独立巨鲸分区受 2GB 子预算约束。磁盘不足时实时看板继续，细历史暂停并标记缺口。已发生的数据缺口不自动伪造补齐。

更改访问密码：在可信本地终端使用 `tidal hash-password` 生成新的 bcrypt 文件，替换服务器 `secrets/password_hash`，保持仅应用 UID 和 root 可读，重建 app 容器；同时清除 `state.sqlite` 的 sessions 表以撤销现有会话。不要在公开工单、日志或命令行参数里粘贴明文密码。

## 验收

72 小时内查看容器无重启 / OOM、CPU 和内存趋势、日增长、有效盘口数、FX 连续性、巨鲸刷新延迟、API 查询时间与错误。压测、清理回放、部署回滚和证书续期验证单独记录；短测成功不能代替完整 72 小时。
