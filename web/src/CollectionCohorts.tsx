export type CollectionCohort = { collectionVersion: string; direction: string; count: number; complete1h: number; complete4h: number; mean1hPercent: string | null; mean4hPercent: string | null };
export const collectionLabel = (v: string) => v === "unrecorded" ? "旧样本 · 当时未记录采集配置" : v === "btc-spot-priority-v1" ? "BTC主链路优先 v1" : v === "legacy-spot-cadence-v1" ? "原采集频率 · 新登记" : v;
const pct = (v: string | null) => v == null ? "未知" : `${Number(v).toFixed(3)}%`;
export function CollectionCohorts({items, title, truncated=false}: {items?: CollectionCohort[];title:string;truncated?:boolean}) {
 return <details><summary>{title} · 采集配置分组</summary><p>规则未变也可能因采集提速而更早发布。分别统计配置已知与未记录样本；不回填旧配置，不据均值直接判断有效。方向收益从各自发布后的下一根完整5分钟现货开盘计算，不含成本。</p>{items == null ? <p>此份旧报告尚无配置分组，等待下一次后台计算。</p> : !items.length ? <p>当前范围没有可分组的已登记样本。</p> : items.map(g=><article key={`${g.collectionVersion}/${g.direction}`}><b>{collectionLabel(g.collectionVersion)} · {g.direction === "buy" ? "买方" : "卖方"}</b><p>已登记 {g.count} · 完整1小时 {g.complete1h}/{g.count}，均值 {pct(g.mean1hPercent)} · 完整4小时 {g.complete4h}/{g.count}，均值 {pct(g.mean4hPercent)}</p></article>)}{truncated && <p>读取达到2000条上限，此处为截断结果，不能当作全量成绩。</p>}</details>;
}
