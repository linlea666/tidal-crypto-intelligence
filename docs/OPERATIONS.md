# 运行与更新

## 服务器目录

`/opt/tidal/data` 是持久数据；`secrets/password_hash` 保存 bcrypt，不使用 SSH 登录密码；`current` 指向版本配置；`state.env` 是不可变镜像摘要。`previous-image`、`backups` 和 `deployed-version` 用于恢复与审计。

应用用户 UID 10001，容器只读根目录、无额外 Linux capabilities，内存上限 768MB；Nginx 上限 96MB。应用端口仅绑定环回，HTTPS 网关对外提供标准 443（容器内 8443），80 用于 ACME 验证和跳转。

正式版本发布前，在 GitHub 仓库的 production 环境 / Secrets 配置：`DEPLOY_HOST`、`DEPLOY_SSH_KEY`、`DEPLOY_KNOWN_HOSTS`。不把这些值放入本仓库。受限部署用户不属于 Docker 组；SSH 禁止端口转发和交互 shell，只可 sudo 执行经参数检查的发布入口。

## 更新

1. 本地修改并跑检查，提交 / PR 到 GitHub。
2. 合并 `main`，创建并发布正式 Release，如 `v2.0.1`。
3. 查看 `Release and deploy` 工作流。成功后可在数据健康页核对版本。
4. 首次 GHCR 包默认为私有时，先将该包设为 Public，再重跑部署作业。服务器不保存 GHCR 登录凭据。

发布脚本检查剩余磁盘、在线备份 `state.sqlite` 和已有的V2 `hub.sqlite`（包括WAL中已提交事务）、核对镜像提交、拉取摘要和检查真实市场数据就绪。失败时恢复原镜像与Nginx/Compose配置。历史数据库采用追加式兼容schema；将来的破坏性schema变更必须单独设计迁移，不能只回退镜像。

## 日常维护

```bash
docker compose --env-file /opt/tidal/state.env -f /opt/tidal/current/compose.yaml ps
cat /opt/tidal/deployed-version
systemctl list-timers tidal-renew.timer tidal-metrics.timer
```

证书使用 Let’s Encrypt IP shortlived profile（6 天）；`tidal-renew.timer` 每日检查两次，续期后重载 Nginx。定时器已提供随机错峰，Certbot 使用 `--no-random-sleep-on-renew` 避免再次等待超过服务超时。端口 80 必须持续允许外部 ACME 验证。可通过 `journalctl -u tidal-renew.service` 检查续期失败。不要停止续期定时器。

`tidal-metrics.timer` 每 5 分钟保存资源、磁盘和健康信息到 `metrics/YYYY-MM-DD.jsonl`，7 天自动删除，同时把镜像、日志和备份占用计入应用预算。Docker 日志总量有上限；宿主机维护任务清理超过 7 天的日志。

本地可运行 `python3 scripts/monitor.py`，使用已忽略的 `secrets/monitor_key` 读取受限服务器报告，并用 `secrets/access.json` 中的看板密码采样API延迟。输出不含密码、Cookie或地址明细。有 `secrets/soak-v2-start.json` 时，详细报告写入 `secrets/soak-v2-latest.json`，按固定V2起点筛选样本；旧 `soak-start.json` 不覆盖。采样包含有效盘口、FX/价格、各数据集时效、额度、巨鲸逐条有效数、内存及磁盘增长。受控故障和启动预热与正常运行区间分开记录。

## 数据保护与降级

设置页只能选择30/90天。清理按数据集确认下一档汇总完成，再删除对应旧细数据；盘口、足迹、成交和持仓的粒度/汇总语义不同，见数据字典。重启从各自持久进度续算。巨鲸明细受2GB子预算约束。磁盘不足先暂停补齐，再清理已完成汇总的细数据，必要时暂停细写入；实时服务继续，历史缺口不补零。

更改访问密码：在可信本地终端使用 `tidal hash-password` 生成新的 bcrypt 文件，替换服务器 `secrets/password_hash`，保持仅应用 UID 和 root 可读，重建 app 容器；同时清除 `state.sqlite` 的 sessions 表以撤销现有会话。不要在公开工单、日志或命令行参数里粘贴明文密码。

## 验收

72 小时内查看容器无重启 / OOM、CPU 和内存趋势、日增长、有效盘口数、FX 连续性、巨鲸刷新延迟、API 查询时间与错误。压测、清理回放、部署回滚和证书续期验证单独记录；短测成功不能代替完整 72 小时。

## V2

数据契约与接入说明见 [V2-DATA-LAYER](V2-DATA-LAYER.md)。在正式Release前预置 `/opt/tidal/secrets/coinglass_key`，属主10001:10001、权限400；Compose只读挂载为 `/run/secrets/coinglass_key`。不要把密钥放在镜像构建参数、GitHub公开变量或浏览器。

V2只启动共享CoinGlass调度、币安价格/K线、Kraken FX；旧历史保留独立目录，不混入评级。V2的hub.sqlite和各日期精度分区保存在持久化 `/data/v2`。升级前使用SQLite在线备份，新增 `tidal backup-db SOURCE NEW_DESTINATION` 命令（引擎3.51.3）；目的文件必须不存在。上线脚本后续升级会备份hub.sqlite，历史分区结构保持向前/向后兼容，回退不清理V2数据。

调度器状态持久化；不要同时在本地和生产使用同一代理密钥持续采集。生产发布前停止本地采集，避免共享额度被双倍消耗。CI启用 `TIDAL_OFFLINE=true` 和空的测试secret文件，只验证启动、TLS、认证、API、SQLite版本及没有行情时readyz=503，不伪造生产行情。

`python3 scripts/monitor.py` 优先读取V2独立起点；仅通过受限monitor SSH报告和HTTPS查询。旧起点文件不得覆盖。文档变更走普通提交，无需发布生产版本。
