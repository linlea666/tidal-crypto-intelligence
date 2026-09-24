#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import glob,json,os,subprocess,time
root='/opt/tidal'
out={'observedAt':time.time()}
for f in ['deployed-version','previous-image']:
 try: out[f]=open(root+'/'+f).read().strip()
 except OSError: pass
files=sorted(glob.glob(root+'/metrics/*.jsonl'))
records=[]
latest={}
for name in files[-4:]:
 for line in open(name):
  try:
   full=json.loads(line);latest=full
   item={k:full[k] for k in ['utc','projectBytes','health','ready','healthQueryMs','error','collectorError'] if k in full}
   item['containers']=[{k:c.get(k) for k in ['Name','CPUPerc','MemUsage','MemPerc','PIDs']} for c in full.get('containers',[])]
   c=full.get('collector',{})
   item['collector']={k:c[k] for k in ['at','generation','uptimeSeconds','heapBytes','goroutines','scheduler','fx','storage','datasets','legacyCollectorsRunning'] if k in c}
   item['collector']['markets']={a:{'validBooks':m.get('validBooks'),'totalBooks':m.get('totalBooks'),'freshWhales':m.get('freshWhales'),'observedWhales':m.get('observedWhales'),'priceValid':m.get('priceValid'),'priceAt':m.get('priceAt'),'validDerivatives':sum(1 for d in m.get('derivatives',[]) if d.get('valid')),'rates':m.get('rates',[])} for a,m in c.get('markets',{}).items()}
   records.append(item)
  except ValueError: pass
out['samples']=records[-900:]
out['latest']=latest
for name in ['tidal-app-1','tidal-gateway-1']:
 try:
  d=json.loads(subprocess.check_output(['docker','inspect',name],universal_newlines=True))[0]
  out[name]={'restartCount':d['RestartCount'],'state':d['State']}
 except Exception as e: out[name]={'error':str(e)}
try:
 out['certificate']=subprocess.check_output(['openssl','x509','-noout','-dates','-in',root+'/letsencrypt/live/tidal-ip/fullchain.pem'],universal_newlines=True).strip()
except Exception: pass
print(json.dumps(out))
PY
