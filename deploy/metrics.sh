#!/usr/bin/env bash
set -euo pipefail
root=/opt/tidal
install -d -m 700 "$root/metrics"
python3 - "$root" <<'PY'
import subprocess,json,sys,time,os,datetime
root=sys.argv[1]
def run(args):
 return subprocess.check_output(args,universal_newlines=True,timeout=15).strip()
def size(path):
 try: return int(run(['du','-sb',path]).split()[0])
 except Exception: return 0
external=size(root+'/backups')+size(root+'/metrics')+size(root+'/certbot-logs')+size(root+'/releases')+size(root+'/letsencrypt')
images=set()
for name in ['tidal-app-1','tidal-gateway-1']:
 try:
  d=json.loads(run(['docker','inspect',name]))[0];images.add(d['Image']);log=d.get('LogPath','')
  if log:
   import glob
   for p in glob.glob(log+'*'): external+=os.path.getsize(p)
 except Exception: pass
for image in images:
 try: external+=int(run(['docker','image','inspect','--format','{{.Size}}',image]))
 except Exception: pass
try:
 previous=open(root+'/previous-image').read().strip()
 image=json.loads(run(['docker','image','inspect',previous]))[0]
 if image['Id'] not in images: external+=image['Size']
except Exception: pass
external+=size(root+'/secrets')
try:
 certbot=json.loads(run(['docker','image','inspect','certbot/certbot:v5.4.0']))[0]
 if certbot['Id'] not in images: external+=certbot['Size']
except Exception: pass
p=root+'/data/external-usage.json'
with open(p+'.tmp','w') as f: json.dump({'bytes':external,'at':time.time()},f)
os.chmod(p+'.tmp',0o644);os.replace(p+'.tmp',p)
try: stats=[json.loads(s) for s in run(['docker','stats','--no-stream','--format','{{json .}}','tidal-app-1','tidal-gateway-1']).splitlines()]
except Exception: stats=[]
record={'utc':datetime.datetime.utcnow().isoformat()+'Z','disk':run(['df','-Pk',root]),'projectBytes':size(root+'/data')+external,'containers':stats}
try:
 with open(root+'/data/collector-health.json') as f: record['collector']=json.load(f)
except Exception as e: record['collectorError']=str(e)
try:
 import urllib.request
 start=time.monotonic()
 record['health']=json.load(urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=5))
 record['ready']=json.load(urllib.request.urlopen('http://127.0.0.1:8080/readyz',timeout=5))
 record['healthQueryMs']=round((time.monotonic()-start)*1000,2)
except Exception as e: record['error']=str(e)
with open(root+'/metrics/'+datetime.datetime.utcnow().strftime('%Y-%m-%d')+'.jsonl','a') as f: f.write(json.dumps(record)+'\n')
for name in os.listdir(root+'/metrics'):
 p=root+'/metrics/'+name
 if name.endswith('.jsonl') and os.path.getmtime(p)<time.time()-7*86400: os.unlink(p)
PY

# Age and size bounds apply together; rotate the project's own current log after 7 days.
for name in tidal-app-1 tidal-gateway-1; do
  path=$(docker inspect --format '{{.LogPath}}' "$name" 2>/dev/null || true)
  [[ -n "$path" ]] || continue
  find "$(dirname "$path")" -maxdepth 1 -name '*.log.*' -mtime +7 -delete
  if [[ -f "$root/metrics/.log-epoch-$name" ]] && [[ $(find "$root/metrics/.log-epoch-$name" -mtime +7 -print) ]]; then
    truncate -s 0 "$path"; touch "$root/metrics/.log-epoch-$name"
  elif [[ ! -f "$root/metrics/.log-epoch-$name" ]]; then touch "$root/metrics/.log-epoch-$name"; fi
done
