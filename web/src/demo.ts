import type { Frame, Zone } from "./types";
// Visual regression fixture. Only reachable in Vite's local development mode.
export function designFixture(): Frame {
  const rows: [number, number, number, string, "bid" | "ask"][] = [
    [85100, 96000000, 18, "尚未触及", "ask"],
    [84900, 48000000, 12, "持续存在", "ask"],
    [84700, 64000000, 25, "疑似撤走（估计）", "ask"],
    [84400, 32000000, 6, "新增挂单", "ask"],
    [84100, 32000000, 18, "少量成交", "bid"],
    [84000, 80000000, 31, "持续存在", "bid"],
    [83800, 128000000, 42, "出现承接", "bid"],
    [83600, 48000000, 9, "尚未触及", "bid"],
  ];
  return {
    asset: "BTC",
    at: new Date().toISOString(),
    price: 84250,
    step: 100,
    startedAt: "2026-09-24T04:00:00Z",
    coverage: ["binance", "okx", "coinbase", "bybit", "kraken"].map((v) => ({
      venue: v,
      symbol: "BTCUSD",
      quote: "USD",
      valid: true,
      observedAt: new Date().toISOString(),
      bidLow: 82000,
      askHigh: 88000,
      bid: 84249,
      ask: 84251,
      levels: 1000,
      resyncs: 0,
    })),
    rates: [],
    zones: rows.map(
      ([p, n, t, e, side]): Zone => ({
        price: p,
        step: 100,
        usdCents: n * 100,
        seconds: t * 60,
        evidence: e,
        side,
        sources: {
          binance: n * 40,
          okx: n * 30,
          coinbase: n * 15,
          bybit: n * 10,
          kraken: n * 5,
        },
        since: new Date(Date.now() - t * 60000).toISOString(),
        occupancy: 1,
        grade: n >= 96000000 ? "较大" : "普通",
        tradedCents: e === "出现承接" ? 10000000 : 0,
        samples: t * 60,
        changeCents: 0,
      }),
    ),
  };
}
