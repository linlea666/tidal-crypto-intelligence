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
for name in files[-4:]:
 for line in open(name):
  try: records.append(json.loads(line))
  except ValueError: pass
out['samples']=records[-900:]
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
