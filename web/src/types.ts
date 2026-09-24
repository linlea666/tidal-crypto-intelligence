export type Asset = "BTC" | "ETH";
export type Zone = {
  updatedAt?: string;
  sampled?: boolean;
  percentile?: number | null;
  strong?: boolean;
  reason?: string;
  covered?: string[];
  price: number;
  step: number;
  side: "bid" | "ask";
  usdCents: number;
  sources: Record<string, number>;
  since: string;
  seconds: number;
  occupancy: number;
  evidence: string;
  grade: string;
  tradedCents: number;
  samples: number;
  changeCents: number;
};
export type Coverage = {
  venue: string;
  symbol: string;
  quote: string;
  valid: boolean;
  reason?: string;
  observedAt: string;
  bidLow: number;
  askHigh: number;
  bid: number;
  ask: number;
  levels: number;
  resyncs: number;
};
export type Frame = {
  priceAt?: string | null;
  priceValid?: boolean;
  note?: string;
  asset: Asset;
  at: string;
  price: number;
  step: number;
  zones: Zone[];
  coverage: Coverage[];
  rates: { quote: string; usd: string; observedAt: string }[];
  startedAt: string;
};
export type Health = {
  data?: unknown;
  feeds: {
    component: string;
    at: string;
    ok: boolean;
    detail: string;
    errors: number;
  }[];
  storage: {
    usedBytes: number;
    freeBytes: number;
    whaleBytes: number;
    fineHistoryPaused: boolean;
    settings: Settings;
    lastCleanup: string;
    error?: string;
  };
  startedAt: string;
  version: string;
  whales: Monitor;
  now: string;
};
export type Settings = {
  retentionDays: number;
  budgetGB: number;
  minFreeGB: number;
};
export type Monitor = {
  candidates: number;
  limit: number;
  coreLimit: number;
  websocketUsers: number;
  pinned: string[];
  scope: string;
  refreshSeconds: number;
};
export type Whale = {
  address: string;
  asset: Asset;
  side: string;
  size: string;
  entry: string;
  usdCents: number;
  leverage: number;
  margin: string;
  liquidation: string | null;
  distance: number | null;
  unrealizedCents: number;
  mark: number;
  at: string;
  changeSize: string;
  firstSeen: string;
  valid: boolean;
  quote: string;
  rate: string;
};
export type WhaleBucket = {
  price: number;
  side: string;
  usdCents: number;
  addresses: number;
  largestShare: number;
  kind: string;
};
export type WhalesResponse = {
  items: Whale[];
  count: number;
  buckets: WhaleBucket[];
  monitor: Monitor;
  at: string;
};
export type Flow = {
  partial: boolean;
  venues: Record<string, [number, number]>;
  buyCents: number;
  sellCents: number;
  netCents: number;
  vwap: number;
  series: {
    time: number;
    buyCents: number;
    sellCents: number;
    cvdCents: number;
  }[];
  footprint: Record<string, [number, number]>;
  step: number;
  market: string;
  resolution: string;
  from: string;
  to: string;
  startedAt: string;
  definition: string;
  coverage: Health["feeds"];
};
export type Derivative = {
  venue: string;
  asset: Asset;
  oiBase: number;
  oiUsdCents: number;
  mark: number;
  index: number;
  funding: number;
  intervalHours: number;
  nextFunding: number;
  at: string;
  valid: boolean;
  quote: string;
  source: string;
};
export type Liquidation = {
  venue: string;
  asset: Asset;
  side: string;
  price: number;
  usdCents: number;
  at: string;
  priceType: string;
  coverage: string;
};
export type DerivativesResponse = {
  changes: Record<
    string,
    { oiBase: number; oiUsdCents: number; since: string }
  >;
  series: {
    time: number;
    venue: string;
    oiUsdCents: number;
    oiBase: number;
    funding: number;
    basis: number;
  }[];
  items: Derivative[];
  liquidations: Liquidation[];
  coverage: string;
};
export type History = {
  points: {
    time: number;
    price: number;
    usdCents?: number | null;
    zones?: Zone[];
    candle: {
      time: number;
      open: number;
      high: number;
      low: number;
      close: number;
    };
  }[];
  resolution: string;
  sampleSeconds: number;
  startedAt: string;
  from: string;
  to: string;
  note: string;
};
export type Annotation = {
  id: string;
  asset: Asset;
  entry: number;
  stop: number;
  target: number;
  side: "long" | "short";
};
