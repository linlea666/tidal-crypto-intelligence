#!/usr/bin/env python3
"""Read the restricted server report and sample authenticated API latency.

Credentials stay in ignored secrets/access.json and secrets/monitor_key.
No passwords, session cookies or individual addresses are printed.
"""
import datetime as dt
import http.cookiejar
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
PRIVATE = ROOT / "secrets"


def stamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def mib(value):
    match = re.match(r"([\d.]+)([KMGT]?i?B)", value or "")
    if not match:
        return None
    scales = {"B": 1, "kB": 1000, "KB": 1000, "KiB": 1024, "MB": 1000000,
              "MiB": 1048576, "GB": 1000000000, "GiB": 1073741824}
    return round(float(match[1]) * scales.get(match[2], 1) / 1048576, 2)


def main():
    access = json.loads((PRIVATE / "access.json").read_text())
    base = access["url"].rstrip("/")
    parsed = urllib.parse.urlparse(base)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("A verified HTTPS dashboard URL is required")
    result = subprocess.run([
        "ssh", "-i", str(PRIVATE / "monitor_key"), "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=yes", "-o", "LogLevel=ERROR",
        "-o", "UserKnownHostsFile=" + str(PRIVATE / "known_hosts"),
        "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
        "tidal-monitor@" + parsed.hostname, "report"
    ], capture_output=True, text=True, timeout=60, check=True)
    report = json.loads(result.stdout)
    generation = next((g for g in ["v2.2", "v2.1", "v2"] if (PRIVATE / f"soak-{g}-start.json").exists()), "v1")
    stem = f"soak-{generation}" if generation != "v1" else "soak"
    path = PRIVATE / f"{stem}-latest.json"
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as out:
        json.dump(report, out)
    start_path = PRIVATE / f"{stem}-start.json"
    baseline = json.loads(start_path.read_text()) if start_path.exists() else {}
    start = stamp(baseline["start"]) if baseline.get("start") else 0
    samples = [s for s in report.get("samples", []) if stamp(s["utc"]) >= start]
    latest = report.get("latest", {})
    rss = [mib(c.get("MemUsage")) for s in samples for c in s.get("containers", []) if c.get("Name") == "tidal-app-1"]
    rss = [x for x in rss if x is not None]
    errors = [s["utc"] for s in samples if s.get("error") or not s.get("ready", {}).get("ok")]
    summary = {"version": report.get("deployed-version"), "sampleCount": len(samples),
               "elapsedHours": round((time.time() - start) / 3600, 2) if start else None,
               "peakAppMiB": max(rss) if rss else None, "failedHealthSamples": errors,
               "certificate": report.get("certificate"), "latest": {k: latest.get(k) for k in ["utc", "projectBytes", "containers", "healthQueryMs"]},
               "containers": {n: report.get(n) for n in ["tidal-app-1", "tidal-gateway-1"]}}
    markets = latest.get("collector", {}).get("markets", {})
    summary["generation"] = generation
    summary["sampleGapMinutes"] = max([(stamp(b["utc"])-stamp(a["utc"]))/60 for a,b in zip(samples,samples[1:])],default=None)
    deployed = report.get("deployed-version", "").split()
    summary["versionMatches"] = not baseline.get("version") or deployed[:3] == [baseline.get("version"), baseline.get("revision"), baseline.get("digest")]
    summary["mixedVersionSamples"] = [s["utc"] for s in samples if baseline.get("version") and s.get("health",{}).get("version") != baseline["version"]]
    expected_identity = " ".join(baseline.get(k, "") for k in ["version", "revision", "digest"])
    summary["identityMismatchSamples"] = [s["utc"] for s in samples if generation == "v2.2" and s.get("deployedVersion") != expected_identity]
    summary["containerIssueSamples"] = [s["utc"] for s in samples if any(c.get("oomKilled") or c.get("restartCount", 0) > 0 or c.get("running") is False or c.get("health") == "unhealthy" for c in s.get("containerStates", {}).values())]
    if generation != "v1":
        collector=latest.get("collector", {})
        summary["quota"]=collector.get("scheduler")
        summary["storage"]=collector.get("storage")
        summary["fx"]=collector.get("fx",{}).get("status")
        summary["staleDatasets"]=[x["dataset"] for x in collector.get("datasets",[]) if x.get("status") in ("missing","stale")]
        summary["legacyCollectorsRunning"]=collector.get("legacyCollectorsRunning")
        summary["contracts"]=collector.get("contracts")
        summary["signals"]=collector.get("signals")
        summary["researchGap"]=collector.get("researchGap")
        summary["mail"]=collector.get("mail")
    summary["markets"] = {a: {k: m.get(k) for k in ["validBooks", "totalBooks", "freshWhales", "observedWhales"]} for a, m in markets.items()}
    jar = http.cookiejar.CookieJar()
    client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    body = json.dumps({"password": access["password"]}).encode()
    req = urllib.request.Request(base + "/api/v1/login", data=body, headers={"Content-Type": "application/json", "Origin": base})
    try:
        with client.open(req, timeout=15) as response:
            json.load(response)
        summary["apiMs"] = {}
        extra = ["signals?asset=BTC", "wallet-trends?asset=BTC", "etf?asset=BTC", "etf?asset=ETH", "studies?asset=BTC"] if generation == "v2.2" else []
        for endpoint in extra + (["activity?asset=BTC&hours=1"] if generation in ("v2.1","v2.2") else []) + ["levels?asset=BTC", "levels?asset=ETH", "flow?asset=BTC&hours=1", "candles?asset=BTC&hours=24", "whales?asset=BTC&limit=50", "derivatives?asset=ETH"]:
            before = time.monotonic()
            with client.open(base + "/api/v1/" + endpoint, timeout=20) as response:
                json.load(response)
            summary["apiMs"][endpoint] = round((time.monotonic() - before) * 1000, 1)
        logout = urllib.request.Request(base + "/api/v1/logout", data=b"{}", headers={"Origin": base})
        with client.open(logout, timeout=10) as response:
            response.read()
    except Exception as error:
        summary["apiError"] = type(error).__name__ + ": " + str(error)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 1 if summary.get("apiError") or errors or not summary["versionMatches"] or summary["mixedVersionSamples"] or summary["identityMismatchSamples"] or summary["containerIssueSamples"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
