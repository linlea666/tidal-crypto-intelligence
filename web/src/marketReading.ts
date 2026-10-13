import type { Activity } from "./Activity";
import type { BookFlowData, BookFlowItem, BookFlowZone } from "./BookFlow";
import type { Meta } from "./Pages";

export function recent(at: string | null | undefined, now: number, ttl: number) {
  const age = now - Date.parse(at || "");
  return Number.isFinite(age) && age >= 0 && age <= ttl;
}
export function liveMeta(meta: Meta | undefined, now: number) {
  return !!meta && ["fresh", "retrieval_only"].includes(meta.status) && Date.parse(meta.expiresAt) > now;
}
export function liveBook(d: BookFlowData | null, failed: boolean, now: number) {
  return !!d && !failed && d.mode !== "off" && recent(d.at, now, 45000) && recent(d.lastProcessedAt, now, 180000);
}
export function liveVenue(d: BookFlowData, venue: string, now: number) {
  const point = d.current[venue];
  return !!point?.fresh && recent(point.snapshot.sourceAt, now, 180000) && recent(point.snapshot.firstFetchedAt, now, 180000);
}
export function comparable(z: BookFlowZone) {
  return z.multiple != null && Number.isFinite(Number(z.multiple)) && z.baselineMedian != null && Number(z.baselineMedian) > 0 && z.baselineCoverage >= 29 / 30;
}
export type KeyZone = { venue: string; zone: BookFlowZone; usd: number | null; distance: number; sourceAt: string; reference: number };
export function keyZones(d: BookFlowData | null, failed: boolean, now: number): KeyZone[] {
  if (!d || !liveBook(d, failed, now)) return [];
  const all: KeyZone[] = [];
  for (const [venue, point] of Object.entries(d.current)) {
    if (!liveVenue(d, venue, now)) continue;
    const reference = Number(point.snapshot.referenceQuote);
    if (!Number.isFinite(reference) || reference <= 0) continue;
    for (const zone of point.snapshot.zones) {
      const low = Number(zone.low), high = Number(zone.high);
      if (!Number.isFinite(low) || !Number.isFinite(high) || low <= 0 || high <= low || zone.quantity == null || !["buy", "sell"].includes(zone.side)) continue;
      const fx = point.snapshot.fxUsd == null ? NaN : Number(point.snapshot.fxUsd);
      const notional = zone.notionalQuote == null ? NaN : Number(zone.notionalQuote);
      const usd = Number.isFinite(fx) && fx > 0 && Number.isFinite(notional) && notional >= 0 ? notional * fx : null;
      const distance = reference < low ? (low - reference) / reference * 100 : reference > high ? (reference - high) / reference * 100 : 0;
      all.push({ venue, zone, usd, distance, sourceAt: point.snapshot.sourceAt, reference });
    }
  }
  return all.sort((a, b) => Number(b.zone.enhanced && comparable(b.zone)) - Number(a.zone.enhanced && comparable(a.zone)) || a.distance - b.distance || a.venue.localeCompare(b.venue) || Number(a.zone.low) - Number(b.zone.low));
}
export function featuredZones(all: KeyZone[], side: string) {
  const chosen: KeyZone[] = [];
  for (const row of all.filter(r => r.zone.side === side)) {
    if (chosen.some(c => c.venue === row.venue && Number(c.zone.low) < Number(row.zone.high) && Number(row.zone.low) < Number(c.zone.high))) continue;
    chosen.push(row);
    if (chosen.length === 2) break;
  }
  return chosen;
}
export function eventStage(item: BookFlowItem, current: boolean, now: number) {
  const e = item.event;
  if (!Number.isFinite(Date.parse(e.publishedAt)) || Date.parse(e.publishedAt) > now || !Number.isFinite(Date.parse(e.expiresAt))) return { kind: "unknown", label: "事件时间待核对", note: "发布时间尚不可用，不用于当前判断。", at: e.publishedAt };
  const updates = e.updates.filter(u => Date.parse(u.publishedAt) <= now);
  const terminal = [...updates].reverse().find(u => ["broken", "faded", "data_gap", "foot_gap", "evidence_unknown", "expired"].includes(u.kind));
  const ended = e.ended || Date.parse(e.expiresAt) <= now;
  if (terminal || ended || !e.completePath) {
    const names: Record<string, string> = { broken: "区域已失守", faded: "挂单已减少", data_gap: "盘口中断，停止判断", foot_gap: "成交证据有缺口", evidence_unknown: "成交证据待核对", expired: "观察期已结束" };
    return { kind: "ended", label: terminal ? names[terminal.kind] : !e.completePath ? "证据路径不完整" : "观察期已结束", note: "保留已发生的观察；不再作为当前有效线索。", at: terminal?.publishedAt || e.publishedAt };
  }
  if (!current || !recent(e.lastSeenAt, now, 180000)) return { kind: "unknown", label: "当前状态待更新", note: "已有事件保留，等待有效盘口与研究时钟恢复。", at: e.publishedAt };
  const absorption = updates.find(u => u.kind === "absorption" && u.evidence?.before != null && u.evidence?.after != null && u.evidence.depthRetention != null);
  if (absorption) return { kind: "absorption", label: e.direction === "buy" ? "出现买方承接线索" : "出现卖方抛压线索", note: e.direction === "buy" ? "卖出成交进入区域后，买方深度仍保持。继续观察挂单是否消退或区域失守。" : "买入成交进入区域后，卖方深度仍保持。继续观察挂单是否消退或区域失守。", at: absorption.publishedAt };
  const persistent = updates.find(u => u.kind === "persistent");
  if (persistent) return { kind: "persistent", label: "后续快照仍保持增强", note: "独立快照已确认；下一步看成交进入区域后，深度能否保持。", at: persistent.publishedAt };
  return { kind: "enhanced", label: "首次发现挂单增加", note: "下一步等待独立快照确认。暂未证明成交承接。", at: e.publishedAt };
}
export function buildBrief(a: Activity | null, d: BookFlowData | null, flowFailed: boolean, bookFailed: boolean, now: number) {
  const flowOK = !!a && !flowFailed && recent(a.at, now, 45000) && liveMeta(a.flowMeta, now) && a.flow.rows > 0 && a.flow.coverage >= .9 && a.flow.boundaries && a.bias !== "证据不足";
  const zones = keyZones(d, bookFailed, now);
  const bookOK = liveBook(d, bookFailed, now);
  const active = d && bookOK && d.mode === "run" && d.runStatus.state === "running" ? d.items.filter(i => liveVenue(d, i.event.venue, now) && ["enhanced", "persistent", "absorption"].includes(eventStage(i, true, now).kind)) : [];
  const sides = new Set(active.map(i => i.event.direction));
  const direction = flowOK ? a?.bias === "买方较主动" ? "buy" : a?.bias === "卖方较主动" ? "sell" : "neutral" : "unknown";
  const inWindow = active.filter(i => a && Date.parse(i.event.publishedAt) >= Date.parse(a.from) && Date.parse(i.event.publishedAt) <= Date.parse(a.to));
  const conflict = flowOK && direction !== "neutral" && inWindow.some(i => i.event.direction !== direction);
  const headline = flowOK ? conflict ? "成交与挂单出现不同方向的线索" : a!.bias === "相对均衡" ? "这一窗口买卖成交相对均衡" : a!.bias === "买方较主动" ? "这一窗口买方成交更主动" : "这一窗口卖方成交更主动" : active.length ? sides.size > 1 ? "买卖两侧均有挂单线索" : sides.has("buy") ? "出现买方挂单线索，成交方向待确认" : "出现卖方挂单线索，成交方向待确认" : "当前证据不足，暂不判断方向";
  const bookLine = bookFailed || !bookOK ? "挂单读取或更新未就绪，暂不用于当前判断。" : d?.runStatus.state === "warming" ? "挂单研究正在积累30分钟基准，尚未开始新观察。" : d?.mode !== "run" ? "挂单研究尚未运行，当前快照不计为前向观察。" : active.length ? `最近已加载事件中，${active.length}条仍可跟踪；挂单变化与选定成交窗口分别判断。` : zones.some(z => !comparable(z.zone)) ? "部分价区缺少可比历史，暂不能判断挂单是否增强。" : zones.length ? "当前可读价区及已加载事件中，暂无持续跟踪的增强线索。" : "当前没有有效价区数据，不能解释为没有挂单。";
  const next = active.length ? eventStage(active[0], true, now).note : !flowOK ? "先等成交窗口与当前证据补齐；某一项缺失不覆盖其他有效观察。" : "继续看价格是否跟随成交，以及附近挂单能否得到后续快照与成交验证。";
  return { flowOK, bookOK, zones, active, headline, direction, conflict, bookLine, next };
}
