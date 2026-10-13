import test from 'node:test';
import assert from 'node:assert/strict';
import { build } from 'esbuild';
import { createRequire } from 'node:module';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
async function bundle(path) {
 const r=await build({entryPoints:[new URL(path,import.meta.url).pathname],bundle:true,write:false,platform:'node',format:'cjs',jsx:'automatic',external:['react'],loader:{'.css':'empty'},logLevel:'silent'});
 const m={exports:{}};new Function('require','module','exports',r.outputFiles[0].text)(createRequire(import.meta.url),m,m.exports);return m.exports;
}
const {buildBrief,keyZones,featuredZones,eventStage}=await bundle('../src/marketReading.ts');
const {KeyZones,EventDigest}=await bundle('../src/MarketBrief.tsx');
const now=Date.now(),iso=(delta=0)=>new Date(now+delta).toISOString();
const zone=(side='buy',low=83200)=>({side,low:String(low),high:String(low+200),quantity:'20',notionalQuote:'1664000',baselineMedian:'8',multiple:'2.5',increaseUsd:'1000000',baselineCoverage:1,enhanced:true});
const point={sourceAt:iso(-30000),firstFetchedAt:iso(-20000),referenceQuote:'83500',fxUsd:'1',zones:[zone(),zone('buy',83300),zone('sell',84000)],bands:[]};
const event={id:'local-test',venue:'Binance',direction:'buy',publishedAt:iso(-60000),expiresAt:iso(1740000),lastSeenAt:iso(-20000),zone:zone(),updates:[],completePath:true,ended:false};
const item={event,observations:[],notifications:[]};
const book={at:iso(),mode:'run',lastProcessedAt:iso(-10000),runStatus:{state:'running'},current:{Binance:{fresh:true,snapshot:point}},items:[item]};
const flow={at:iso(),from:iso(-3600000),to:iso(-30000),bias:'买方较主动',flowMeta:{status:'fresh',expiresAt:iso(60000)},flow:{rows:60,coverage:1,boundaries:true}};

test('failed and expired evidence cannot supply a current direction; independent good evidence survives',()=>{
 assert.match(buildBrief(flow,book,false,false,now).headline,/买方成交更主动/);
 assert.equal(buildBrief(flow,book,true,true,now).direction,'unknown');
 assert.equal(keyZones(book,true,now).length,0);
 assert.equal(keyZones({...book,at:iso(-46000)},false,now).length,0);
 assert.equal(keyZones({...book,current:{Binance:{fresh:true,snapshot:{...point,sourceAt:iso(-181000)}}}},false,now).length,0);
 assert.match(buildBrief({...flow,flowMeta:{...flow.flowMeta,expiresAt:iso(-1)}},book,false,false,now).headline,/挂单线索/);
 assert.match(buildBrief(flow,book,false,true,now).headline,/成交更主动/);
 assert.equal(buildBrief({...flow,flow:{...flow.flow,boundaries:false}},null,false,false,now).flowOK,false);
 assert.equal(buildBrief({...flow,at:iso(1)},null,false,false,now).flowOK,false);
});
test('only observations inside the selected window describe a conflict',()=>{
 const opposite={...book,items:[{...item,event:{...event,direction:'sell'}}]};
 assert.equal(buildBrief(flow,opposite,false,false,now).conflict,true);
 assert.equal(buildBrief(flow,{...opposite,items:[{...item,event:{...event,direction:'sell',publishedAt:iso(-10000)}}]},false,false,now).conflict,false);
 assert.equal(buildBrief(flow,{...opposite,items:[{...item,event:{...event,direction:'sell',ended:true}}]},false,false,now).conflict,false);
});
test('missing baseline, FX, quantities and zero amounts are distinct',()=>{
 const unknown={...book,items:[],current:{Binance:{fresh:true,snapshot:{...point,fxUsd:null,zones:[{...zone(),multiple:null,baselineMedian:null,baselineCoverage:0}]}}}};
 const rows=keyZones(unknown,false,now);assert.equal(rows[0].usd,null);
 assert.match(buildBrief(flow,unknown,false,false,now).bookLine,/缺少可比历史/);
 const html=renderToStaticMarkup(React.createElement(KeyZones,{data:unknown}));
 assert.match(html,/美元金额未知/);assert.match(html,/比较所需历史不足/);assert.doesNotMatch(html,/brief-zone-fill/);
 const zero={...book,current:{Binance:{fresh:true,snapshot:{...point,zones:[{...zone(),notionalQuote:'0'}]}}}};
 assert.equal(keyZones(zero,false,now)[0].usd,0);
 const missing={...zero,current:{Binance:{fresh:true,snapshot:{...point,zones:[{...zone(),quantity:null}]}}}};
 assert.equal(keyZones(missing,false,now).length,0);
});
test('featured zones do not sum overlapping windows or merge venues',()=>{
 const both={...book,current:{...book.current,OKX:{fresh:true,snapshot:point}}};
 const all=keyZones(both,false,now),selected=featuredZones(all,'buy');
 assert.equal(selected.length,2);assert.notEqual(selected[0].venue,selected[1].venue);
 assert.equal(selected[0].usd,1664000);assert.equal(selected[1].usd,1664000);
});
test('confirmation is separate from absorption and incomplete or ended paths never remain active',()=>{
 assert.equal(eventStage(item,true,now).kind,'enhanced');
 const persistent={...item,event:{...event,updates:[{kind:'persistent',publishedAt:iso(-40000)}]}};
 assert.equal(eventStage(persistent,true,now).kind,'persistent');
 const noProof={...item,event:{...event,updates:[...persistent.event.updates,{kind:'absorption',publishedAt:iso(-10000)}]}};
 assert.equal(eventStage(noProof,true,now).kind,'persistent');
 const proved={...item,event:{...event,updates:[{kind:'absorption',publishedAt:iso(-10000),evidence:{before:{},after:{},depthRetention:'0.8'}}]}};
 assert.equal(eventStage(proved,true,now).kind,'absorption');
 for(const patch of [{ended:true},{completePath:false},{expiresAt:iso(-1)},{updates:[{kind:'foot_gap',publishedAt:iso(-1)}]}])assert.equal(eventStage({...proved,event:{...proved.event,...patch}},true,now).kind,'ended');
 assert.equal(eventStage(proved,false,now).kind,'unknown');
 assert.equal(eventStage({...proved,event:{...proved.event,lastSeenAt:iso(-181000)}},true,now).kind,'unknown');
 const html=renderToStaticMarkup(React.createElement(EventDigest,{item:persistent,current:true}));assert.match(html,/后续快照仍保持增强/);assert.match(html,/尚无完整成交验证/);
});
test('warming and unsupported collection mode do not publish directional book conclusions',()=>{
 assert.equal(buildBrief(null,{...book,runStatus:{state:'warming'}},false,false,now).active.length,0);
 assert.equal(buildBrief(null,{...book,mode:'collect'},false,false,now).active.length,0);
 assert.equal(buildBrief(null,null,false,false,now).headline,'当前证据不足，暂不判断方向');
});
