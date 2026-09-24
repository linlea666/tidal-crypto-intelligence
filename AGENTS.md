# Tidal development contract

- Product: Chinese BTC/ETH intelligence dashboard. Public data only; no order execution.
- Selected visual: `docs/design/selected.png`. Use a graphite background, mint bids, coral asks, a shared linear dollar scale and horizontal price-bucket bars. Heatmaps are an advanced historical view. Whale cost and liquidation charts stay separate from spot orders.
- Preserve facts: missing coverage is not zero, stale FX cannot be treated as 1 USD, trade flow is not deposits/withdrawals, observed whales are not a global ranking, null liquidation prices remain null.
- Keep collectors bounded and reconnect safely. Validate sequences/checksums before exposing a book. No generated sample market data in production.
- Source changes originate locally, enter GitHub, and deploy through a stable Release. Never commit credentials, runtime databases, logs or server-specific secret configuration.
- Run Go financial/recovery/storage tests, frontend typecheck/build and browser interaction checks for relevant changes. Record incomplete acceptance honestly in `docs/STATUS.md`.
