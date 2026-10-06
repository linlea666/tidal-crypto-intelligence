import { clock } from "./data";

export type ShortRuntime = {
  pipelineVersion: string; origin: string; at: string; persistenceError?: string;
  stages: Record<string, { lastFailureAt?: string; lastFailure?: string; lastFailureClass?: string; lastFailureOperation?: string; state: string; at: string; lastSuccessAt: string | null; errors: number; error?: string; errorClass?: string; durationMs: number; operations: Record<string, { count: number; lastMs: number; maxMs: number }> }>;
};
const states: Record<string, string> = { ready: "最近执行成功", yielded: "已保存进度，继续准备", error: "最近执行异常" };
const classes: Record<string, string> = { timeout: "超时", database_busy: "数据库争用", write: "写入失败", capacity: "容量保护", read_or_compute: "读取或计算失败" };
export function ShortFlowHealth({ d }: { d?: ShortRuntime | null }) {
  if (!d) return <p>此快照尚未记录分阶段运行诊断。</p>;
  return <details className="flow-raw"><summary>运行诊断：成交、基线、研究</summary>
    <div className="flow-detail-grid">{[["observation", "成交观察"], ["baseline", "同周期基线"], ["study", "结果研究"]].map(([key, title]) => { const s = d.stages[key]; return <section key={key}><h4>{title}</h4>{s ? <><p>{Date.now() - new Date(s.at).getTime() > 120000 && "历史运行记录 · "}{states[s.state] ?? "状态未知"} · {clock(s.at)}</p><p>最近成功 {s.lastSuccessAt ? clock(s.lastSuccessAt) : "尚未记录"} · 累计异常 {s.errors} 次</p>{s.error && <p className="flow-error">{classes[s.errorClass ?? ""] ?? "异常"}：{s.error}</p>}{s.lastFailureAt && <p className="helper">最近失败 {clock(s.lastFailureAt)} · {s.lastFailureOperation || "操作未知"} · {classes[s.lastFailureClass ?? ""] ?? "异常"}：{s.lastFailure}</p>}<small>最近执行 {s.durationMs.toFixed(0)} ms</small></> : <p>等待首次执行</p>}</section>; })}</div>
    {d.persistenceError && <p className="flow-error">{d.persistenceError}</p>}
    <p>保存进度后继续计算属于正常续算。任务异常次数不等于缺失窗口数；研究暂停不代表成交方向中性。分阶段计数从本运行版本开始，原累计异常与研究起点保留。</p>
  </details>;
}
