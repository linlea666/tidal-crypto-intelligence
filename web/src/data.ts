import { useEffect, useSyncExternalStore, useCallback } from "react";
export const price = (value: number, digits = 2) =>
  Number.isFinite(value)
    ? value.toLocaleString("en-US", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      })
    : "—";
export function amount(cents: number, sign = false) {
  const n = cents / 100;
  const abs = Math.abs(n);
  const prefix = n < 0 ? "−" : sign && n > 0 ? "+" : "";
  return (
    prefix +
    (abs >= 1e8
      ? (abs / 1e8).toFixed(2).replace(/\.00$/, "") + "亿"
      : abs >= 1e4
        ? (abs / 1e4).toFixed(abs >= 1e7 ? 0 : 1).replace(/\.0$/, "") + "万"
        : price(abs, 0))
  );
}
export const age = (seconds: number) =>
  seconds >= 86400
    ? (seconds / 86400).toFixed(1) + "天"
    : seconds >= 3600
      ? (seconds / 3600).toFixed(1) + "小时"
      : seconds >= 60
        ? Math.floor(seconds / 60) + "分钟"
        : Math.max(0, Math.floor(seconds)) + "秒";
export const clock = (t: string | number) =>
  new Date(typeof t === "number" ? t * 1000 : t).toLocaleTimeString("zh-CN", {
    timeZone: "Asia/Shanghai",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  });
export const venue = (v: string) =>
  ({
    okx: "OKX",
    binance: "Binance",
    coinbase: "Coinbase",
    kraken: "Kraken",
    bybit: "Bybit",
    bitfinex: "Bitfinex",
    hyperliquid: "Hyperliquid",
  })[v] ?? v;
type Snapshot = { data: unknown; error: string; loading: boolean };
type Entry = {
  state: Snapshot;
  listeners: Set<() => void>;
  timer?: ReturnType<typeof setInterval>;
  period: number;
  inFlight?: Promise<void>;
  etag?: string;
};
const cache = new Map<string, Entry>();
const pending = new Map<string, Promise<unknown>>();
function entry(url: string): Entry {
  let e = cache.get(url);
  if (!e) {
    if (cache.size >= 64) {
      for (const [key, value] of cache) {
        if (!value.listeners.size && !value.inFlight) {
          cache.delete(key);
          if (cache.size < 48) break;
        }
      }
    }
    e = {
      state: { data: null, error: "", loading: false },
      listeners: new Set(),
      period: Number.POSITIVE_INFINITY,
    };
    cache.set(url, e);
  }
  return e;
}
function publish(e: Entry, state: Snapshot) {
  e.state = state;
  e.listeners.forEach((fn) => fn());
}
export function clearDataCache() {
  cache.forEach((e) => {
    if (e.timer) clearInterval(e.timer);
    publish(e, { data: null, error: "", loading: false });
  });
  cache.clear();
  pending.clear();
}
export function api<T>(url: string, options: RequestInit = {}): Promise<T> {
  const version = /^(login|logout|session|settings|annotations)(\?|$)/.test(url)
    ? "v1"
    : "v2";
  const method = options.method || "GET";
  const key = version + "/" + url;
  if (method === "GET" && !options.signal && pending.has(key))
    return pending.get(key) as Promise<T>;
  const promise = (async () => {
    const r = await fetch("/api/" + version + "/" + url, {
      ...options,
      headers: { "Content-Type": "application/json", ...options.headers },
    });
    const data = await r.json().catch(() => {
      throw Error("服务暂时不可用");
    });
    if (!r.ok) {
      if (r.status === 401 && url !== "login") {
        clearDataCache();
        window.dispatchEvent(new Event("tidal-session-expired"));
      }
      throw Error(data.error || "请求失败");
    }
    if (url === "logout") clearDataCache();
    return data as T;
  })();
  if (method === "GET" && !options.signal) {
    pending.set(key, promise);
    promise
      .finally(() => {
        if (pending.get(key) === promise) pending.delete(key);
      })
      .catch(() => {});
  }
  return promise;
}
async function load(url: string, e: Entry) {
  if (e.inFlight) return e.inFlight;
  publish(e, { ...e.state, loading: true });
  e.inFlight = api(url)
    .then((data) => publish(e, { data, error: "", loading: false }))
    .catch((error) =>
      publish(e, { ...e.state, error: error.message, loading: false }),
    )
    .finally(() => {
      e.inFlight = undefined;
    });
  return e.inFlight;
}
const empty: Snapshot = { data: null, error: "", loading: false };
export function useAPI<T>(url: string | null, ms = 15000) {
  const subscribe = useCallback(
    (listener: () => void) => {
      if (!url) return () => {};
      const e = entry(url);
      e.listeners.add(listener);
      if (ms < e.period && e.timer) {
        clearInterval(e.timer);
        e.timer = undefined;
      }
      e.period = Math.min(e.period, ms);
      if (!e.timer)
        e.timer = setInterval(() => {
          if (e.listeners.size) void load(url, e);
        }, e.period);
      return () => {
        e.listeners.delete(listener);
        if (!e.listeners.size && e.timer) {
          clearInterval(e.timer);
          e.timer = undefined;
        }
      };
    },
    [url, ms],
  );
  const get = useCallback(() => (url ? entry(url).state : empty), [url]);
  const state = useSyncExternalStore(subscribe, get, get);
  useEffect(() => {
    if (url) void load(url, entry(url));
  }, [url]);
  return {
    ...state,
    data: state.data as T | null,
    refresh: () => {
      if (url) void load(url, entry(url));
    },
  };
}
