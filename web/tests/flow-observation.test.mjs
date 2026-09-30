import test from "node:test";
import assert from "node:assert/strict";
import { build } from "esbuild";
import { createRequire } from "node:module";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

const result = await build({entryPoints:[new URL("../src/FlowObservation.tsx",import.meta.url).pathname],bundle:true,write:false,platform:"node",format:"cjs",jsx:"automatic",external:["react"],logLevel:"silent"});
const mod = {exports:{}};
new Function("require","module","exports",result.outputFiles[0].text)(createRequire(import.meta.url),mod,mod.exports);
const {WindowCard,Continuity,FlowObservation,checkValue} = mod.exports;
const render = (component, props) => renderToStaticMarkup(React.createElement(component,props));
const now = new Date();
const end = new Date(now.getTime()-60000).toISOString();
const window = {minutes:5,from:new Date(now.getTime()-360000).toISOString(),to:end,coverage:1,netCents:0,buyCents:0,sellCents:0,volumeCents:0,buyShare:null,sellShare:null,volumeRatio:null};

test("closed windows distinguish zero, missing and both sides without borrowed direction",()=>{
  const zero=render(WindowCard,{w:window,title:"最近5分钟"});
  assert.match(zero,/主动买卖相当/);assert.match(zero,/买入 未知/);assert.match(zero,/USD/);
  const missing=render(WindowCard,{w:{...window,netCents:null,coverage:.5},title:"最近10分钟"});
  assert.match(missing,/窗口不完整/);assert.match(missing,/时间覆盖 50%/);assert.match(missing,/>—</);
  const sell=render(WindowCard,{w:{...window,netCents:-100000000,buyShare:40,sellShare:60},title:"最近5分钟"});
  assert.match(sell,/主动净卖出/);assert.match(sell,/买入 40.0%/);assert.match(sell,/卖出 60.0%/);
});

test("six independent intervals keep gaps and zero separate",()=>{
  const rows=Array.from({length:6},(_,i)=>({...window,from:new Date(now.getTime()-(i+1)*300000).toISOString(),netCents:i===0?null:i===1?0:10000000}));
  const html=render(Continuity,{rows,title:"六段独立5分钟"});
  assert.equal((html.match(/class="flow-strip"/g)||[]).length,6);
  assert.match(html,/缺失/);assert.match(html,/不重复累加/);
});

test("expired observation disables hints while keeping historical amounts and models separate",()=>{
  const s={rulesVersion:"flow-observation-v1",at:now.toISOString(),dataThrough:new Date(now.getTime()-360000).toISOString(),fresh:true,availableAt:null,windows:{"5":{...window,netCents:300000000},"10":window,"15":window,"60":window,"240":window},segments:[],hints:[{minutes:5,direction:"buy",active:true,checks:[]}],baseline:{valid:true,coverage:1,validDates:29,from:end,to:end},prices:{},zones:[{id:"z",side:"short",low:85000,high:85250,quote:"USDT",strength:"312000000",distancePercent:1,relativeStrength:90,fetchedAt:end}],zoneNote:"独立模型",coverageNote:"交易所覆盖未知",coverageRequested:"五家",researchPaused:true,researchReason:"容量保护"};
  const html=render(FlowObservation,{s,failed:false});
  assert.match(html,/暂停新提示/);assert.doesNotMatch(html,/<strong>5分钟买入脉冲/);
  assert.match(html,/300万/);assert.match(html,/3.12亿/);assert.match(html,/模型强度·非美元/);assert.match(html,/独立验证暂停/);
  const gaps=render(FlowObservation,{s:{...s,zones:null,hints:null,segments:null},failed:false});
  assert.match(gaps,/300万/);assert.match(gaps,/交易所覆盖未知/);
});

test("blocker values retain cents, ratios, counts and unknown distinctions",()=>{
  assert.equal(checkValue({name:"连续三根5分钟同向",known:true,passed:false,actual:2,required:3}),"2 / 3");
  assert.match(checkValue({name:"该方向15分钟P95",known:true,passed:false,actual:500000000,required:1000000000}),/500万.*1000万 USD/);
  assert.equal(checkValue({name:"主导方占比≥55%",known:false,passed:false,actual:null,required:55}),"未知");
});
