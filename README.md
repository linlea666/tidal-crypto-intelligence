# TIDAL V2 潮汐 · 加密市场情报

面向中文用户的 BTC / ETH 现货买卖墙、实际成交、合约背景、清算分布与公开巨鲸看板。CoinGlass统一采集并存入共享数据层，各页面和统计复用本地API。默认用横向柱子表示美元挂单金额；买卖共用线性比例尺，热力图作为进阶历史视图。

首次使用请阅读 [看板使用说明](docs/USAGE.md)。

![V2沿用的柱状布局，图为早期真实运行界面](docs/qa/production-desktop.png)

## 数据说明

- 五家现货：Binance、OKX的USDT市场，加Coinbase、Kraken、Bitfinex的USD市场，共10个BTC/ETH市场；约2分钟快照。按有效Kraken汇率换算美元，失效FX不按1美元处理。币安仅提供即时价格和已完成5分钟K线。
- 支撑/阻力均为候选区域。历史金额分位、采样稳定程度和成交证据分别展示；不足7天/500个同类价位样本不评级。金额大不是必然反弹，采样间变化不能还原同一笔订单或确定撤单。
- 主动净买入=主动买入额−主动卖出额，不是充值/提现。CVD显示固定起算点和缺口；三家足迹不冒充五家盘口全部成交。
- CoinGlass覆盖交易所的OI、资金费率和已发生清算，保留实际单位、计费周期及来源时间。聚合OI不与分所重复相加；没有来源时间时明确标为仅有获取时间。
- 清算地图与热力图显示模型相对强度，不与已发生清算或账户清算参考价相加，不称为必然爆仓金额。
- Hyperliquid已覆盖大仓默认前50，可切前100及临近清算排序；分布使用全部有效已覆盖持仓。逐条检查180秒有效期，均价不是第一笔开仓价，设置杠杆不等于实际杠杆，空清算价保留为空。榜单消失不等于确认平仓。
- 仅手工价格标注，不提交交易订单。

## 本地运行

需要Go 1.25、Node.js 22，以及授权的CoinGlass代理密钥。没有交易执行权限。不要同时用同一代理账号在本地和生产持续采集。

```bash
npm --prefix web ci
npm --prefix web run build
go build -o bin/tidal ./cmd/tidal
# 将至少 12 位的独立访问密码放在 TIDAL_PASSWORD 环境变量中：
./bin/tidal hash-password > /absolute/path/password_hash
# password_hash 必须放在仓库外或已忽略的 secrets/ 内，权限设为 600。
COINGLASS_API_KEY_FILE=/absolute/path/coinglass_key \
TIDAL_PASSWORD_HASH_FILE=/absolute/path/password_hash \
TIDAL_COOKIE_SECURE=false ./bin/tidal
```

两个secret文件都应在已忽略目录或仓库外，权限600，不进入命令参数或镜像。本地访问 `http://127.0.0.1:8080`。开发时运行 `npm --prefix web run dev`，Vite代理到Go API。离线测试可设 `TIDAL_OFFLINE=true`；无行情时明确显示缺失，readyz返回503。仅开发模式的 `?demo=1` 用于布局对照，生产构建移除该分支。

## 验证

```bash
go test -race ./...
go vet ./...
npm --prefix web run typecheck
npm --prefix web run build
npm --prefix web test
bash -n deploy/*.sh
```

测试覆盖：100次本地查询零上游请求、并发请求合并、滚动额度/429/重启恢复、重复/乱序/修正、单位/FX/时效、空值、成交/VWAP/存量/模型汇总、分区删除保护、在线备份、认证与并发。旧采集器回归测试保留，但生产不再启动旧采集器。

## 发布

本地提交 → GitHub 检查 → `main` → 发布正式 `vX.Y.Z` Release → GitHub Actions 构建 Linux amd64 镜像 → GHCR → 服务器按摘要部署。草稿、预发布和普通提交不更新生产。项目镜像包已公开；后续保持 GHCR 的 Public 可见性，部署会验证匿名拉取。

服务器部署入口只接受 `deploy sha256:<digest> <commit> vX.Y.Z`，核对公开正式 Release、提交与镜像来源。生产 SSH 私钥仅存在 GitHub Secrets 和受保护的本地配置中；不使用仓库内凭据。更新保留数据卷和上一镜像，健康失败回退。详见 [运维说明](docs/OPERATIONS.md)。

## 存储与资源

Go 采集 / API + React / TypeScript + 日期 / 精度分区 SQLite + Nginx / Docker Compose。目标为 2 CPU / 约 2GB 内存的小服务器。

| 数据 | 保留 |
| --- | --- |
| 盘口分钟观察 / 5分钟 / 小时 | 7天 / 30天 / 至90天，不伪造连续分钟样本 |
| 足迹5分钟 / 15分钟 / 小时 | 7天 / 30天 / 至90天 |
| 主动买卖、OI、资金费率实际粒度 | 30天，长期小时降采样 |
| 巨鲸分布分钟 / 地址仓位5分钟快照 | 30天；分布可长期降采样 |
| 清算模型 | 完整最新缓存，历史仅保存分布摘要 |
| 日志、运维采样、元数据备份 | 最长 7 天，同时限制日志大小 |

项目预算20GB，巨鲸明细2GB，磁盘至少留8GB。先暂停历史补齐，仅清理已完成汇总的旧细数据；必要时暂停细写入并保留实时服务。内存目标600MiB，容器768MiB。旧历史独立保留、不参与新评级，按原策略清理。关闭页面不停止采集。

## API

除 `/healthz`、`/readyz`、登录与会话状态外均需会话 Cookie：

- `GET /api/v2/data/catalog`、`data/{dataset}`、`data-status`：共享目录、最新/历史事实、采集状态。
- `GET /api/v2/overview`、`levels`、`history`、`candles`、`flow`、`derivatives`：页面组合查询。
- `GET /api/v2/whales`：`limit=50|100`、方向及清算距离排序；`large-orders`：当前/结束记录。
- `GET /api/v2/liquidations`：24小时默认模型、7天/30天缓存；`/api/v2/stream`：共享实时推送。
- `POST /api/v2/data-requests`：未缓存的历史窗口或钱包详情申请，统一去重、排队和限流。
- 会话、设置和手工标注沿用 `/api/v1` 接口。旧行情接口保留兼容别名。

GET只读取本地，不触发上游。上游任意滚动60秒最多12次、最少错开5.1秒，失败也计数；本地查询有并发/时长/8MiB响应限制。金额采用十进制字符串和整数美分，随响应保留来源、源时间/获取时间、原报价、覆盖、有效状态及实际粒度。完整字典、复用关系与新增功能规范见 [数据层说明](docs/V2-DATA-LAYER.md)。

## 接口来源

[CoinGlass](https://docs.coinglass.com) · [Binance](https://github.com/binance/binance-spot-api-docs) · [Kraken](https://docs.kraken.com/api/)

验收状态见 [STATUS](docs/STATUS.md)。持续运行 72 小时的资源验收与短测分开记录。

当前已发布v2.0.0，V1连续验收按迁移计划中止；V2从2026-09-24 12:14:41 UTC单独开始72小时验收，尚未得出完整结论。
