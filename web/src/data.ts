import { useEffect, useState } from "react";
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
    hyperliquid: "Hyperliquid",
  })[v] ?? v;
export async function api<T>(
  url: string,
  options: RequestInit = {},
): Promise<T> {
  const r = await fetch("/api/v1/" + url, {
    ...options,
    headers: { "Content-Type": "application/json", ...options.headers },
  });
  let data;
  try {
    data = await r.json();
  } catch {
    throw Error("服务暂时不可用");
  }
  if (!r.ok) {
    if (r.status === 401 && url !== "login")
      window.dispatchEvent(new Event("tidal-session-expired"));
    throw Error(data.error || "请求失败");
  }
  return data;
}
export function useAPI<T>(url: string | null, ms = 15000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    if (!url) return;
    let active = true;
    const abort = new AbortController();
    setData(null);
    const load = () => {
      setLoading(true);
      api<T>(url, { signal: abort.signal })
        .then((d) => {
          if (active) {
            setData(d);
            setError("");
          }
        })
        .catch((e) => {
          if (active && e.name !== "AbortError") setError(e.message);
        })
        .finally(() => {
          if (active) setLoading(false);
        });
    };
    load();
    const timer = setInterval(load, ms);
    return () => {
      active = false;
      abort.abort();
      clearInterval(timer);
    };
  }, [url, ms, revision]);
  return { data, error, loading, refresh: () => setRevision((x) => x + 1) };
}
