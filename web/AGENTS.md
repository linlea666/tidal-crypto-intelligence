# Prototype Instructions

Approved source: `../docs/design/selected.png`. The user chose horizontal price-zone bars for beginners. Keep a shared linear USD scale for bids/asks. Preserve the current-price separator, selected-zone evidence inspector, source contributions and compact history chart. Heatmap is an advanced historical view; whale costs and liquidation exposure live on their own page. Production has real authenticated API data; the dev-only `?demo=1` fixture is solely for visual comparison.

Run the local server yourself and open the preview in the browser available to this environment. Do not give the user server-start instructions when you can run it.

Before making substantial visual changes, use the Product Design plugin's `get-context` skill when the visual source is unclear or no longer matches the current goal. When the user gives durable prototype-specific design feedback, preferences, or decisions, record them in `AGENTS.md`.

When implementing from a selected generated mock, treat that image as the source of truth for layout, component anatomy, density, spacing, color, typography, visible content, and hierarchy.

Build app UI in `src/`. Keep `.openai/hosting.json`, `worker/index.js`, `scripts/prepare-sites-build.mjs`, and `tests/sites-worker.test.mjs` intact so the same local prototype can be handed to Sites. Before a Sites handoff, run `npm run build` and `npm run test:sites`; the build must leave `dist/client/index.html`, `dist/server/index.js`, and `dist/.openai/hosting.json`.

V2.1: spot bars show current per-bucket stock; range totals include all returned buckets independently of collapse and persistence filters. The separate 大资金动向 page uses spot orders plus derivatives context, text evidence cards and a timeline. No trade circles without verified per-trade data. Keep snapshot cadence, source coverage and flow windows explicit.

V2.3: 大额挂单 is an independent BTC/ETH page (50 rows by default); move its lists/events/duration views out of spot walls and activity. Never add large-order values to ordinary walls or signals. Activity leads with plain buy/sell balance, price response and data coverage; CVD/footprints are advanced. Signals, forward observation and studies are BTC only; other ETH features remain.

V2.4: desktop large orders use equal bid/ask columns with independent filters/pages and one shared snapshot/USD scale. Mobile switches sides while retaining both summaries and each side’s filters. Snapshot amounts stay immutable; newer local lifecycle facts may invalidate their status. Native-quote liquidity changes never imply confirmed cancellation or spoofing.

VIX: independent US-equity observation page at #vix, after ETF. No BTC/ETH switch, USD price, or spot coverage banners on this page. Sina timestamps are Beijing time and the source declares at least 15 minutes of delay; Cboe daily closes have their own source/date labels. 30–40 inclusive is a buy-observation alert and >40 is priority observation, not a proven bottom or crypto signal. Preserve gaps and nulls; notification and market state are separate. First valid detection alerts immediately, once per band per episode; below 30 for 30 observed minutes re-arms, gaps >5 minutes break that continuity.

V2.7: BTC 资金预警 leads with a plain current conclusion, closed 15m/1h/4h net flow, independent interval bars, then six evidence facets and a compact timeline. Mint buys, coral sells, amber conflicts; all states have text. Mobile keeps all three core numbers visible. OI/Funding/realized liquidation gaps stay unknown; early anomaly and price confirmation are separate mail phases.

V2.8: liquidation map uses mint short-liquidation risk above, coral long-liquidation risk below, with frozen native-quote event direction. Both sides share the full-map linear intensity scale. Display Chinese 万/亿 with a persistent “模型强度·非美元” label; actual USD liquidations and covered whale notional stay separate. Default ±5%, strongest3+nearest3 per side, price sort; details stay alongside on desktop and expand below the selected row on mobile. Stale snapshots are gray and excluded from priorities. BTC forward observation is 1h/4h, with counts before statistically eligible sample rates.

V2.9: BTC funds alerts add independent 5m pulse / 10m continuation cards, six non-overlapping 5m bars, explicit formal blockers and the latest formal event. Formal 15m/1h thresholds and mail remain unchanged. Mobile uses two short cards then vertical longer windows. Flow-only timestamps, delayed price/ATR unknowns, stale snapshots and missing coverage remain explicit; short research is separate and BTC only.

V2.10: 大额挂单 defaults to BTC/ETH price zones (±10%, BTC $250 / ETH $10, closed 4h footprint). Shared USD scale; strongest3+nearest3 each side; support/resistance candidates and observation interval only. Original split board stays under 大单明细. Source tracking age and local observations are separate. Frozen amounts never change with later FX. Advanced book inventory and tracked-order samples stay separate, preserve missing time/price cells, and never draw backward from source creation.

BTC 链上筹码：独立 #onchain-cost，放在巨鲸之后，保留离开前的BTC/ETH选择；不加载现货总览WS或覆盖状态。成本桶用BTC线性数量轴，STH橙/LTH蓝，薄荷绿与珊瑚红仍只表示买卖方向。快照同日价格、真实日期回放、部分桶上下限、固定美元区间与来源分母必须保留；缺日留空，历史不借用今天证据。事件边界冻结，过期数据暂停判断，邮件默认关闭。
