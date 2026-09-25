#!/usr/bin/env bash
# Offline Linux acceptance; credentials, actual markets and production are unused.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p tmp/resource-v24
rm -f tmp/resource-v24/samples.jsonl tmp/resource-v24/runtime.jsonl
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -c ./internal/datahub -o tmp/resource-v24/replay.test
mode=${TIDAL_REPLAY_MODE:-replay}
run='^TestResourceReplay$'
if [[ "$mode" == reference ]]; then run='^TestBaselineResource$'; fi
cid=$(docker create --memory=768m --cpus=1.7 -e GOMEMLIMIT=512MiB -e TIDAL_RESOURCE_REPLAY=1 -e TIDAL_RESOURCE_OUTPUT=/out -e TIDAL_REPLAY_DURATION="${TIDAL_REPLAY_DURATION:-6m}" -e TIDAL_BASELINE_REPLAY=1 -e TIDAL_BASELINE_REFERENCE="${TIDAL_BASELINE_REFERENCE:-0}" -v "$PWD/tmp/resource-v24:/out" alpine:3.22 /out/replay.test -test.run="$run" -test.v -test.timeout=335m -test.cpuprofile=/out/cpu.pprof)
trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
docker start "$cid" >/dev/null
while [[ $(docker inspect -f '{{.State.Running}}' "$cid") == true ]]; do
  docker stats --no-stream --format '{{json .}}' "$cid" >> tmp/resource-v24/samples.jsonl
  sleep 3
done
docker logs "$cid" > tmp/resource-v24/replay.log 2>&1
docker inspect "$cid" > tmp/resource-v24/container.json
cat tmp/resource-v24/replay.log
python3 - <<'PY'
import json,pathlib,re,statistics,os,datetime
p=pathlib.Path('tmp/resource-v24')
samples=[json.loads(s) for s in (p/'samples.jsonl').read_text().splitlines() if s]
def mib(v):
 m=re.match(r'([\d.]+)([KMGT]?i?B)',v.strip());return float(m[1])*{'B':1,'KiB':1024,'MiB':2**20,'GiB':2**30}[m[2]]/2**20
memory=[mib(s['MemUsage'].split('/')[0]) for s in samples]
state=json.loads((p/'container.json').read_text())[0]['State']
result={'samples':len(samples),'peakMiB':max(memory,default=0),'medianMiB':statistics.median(memory) if memory else 0,'lastMiB':memory[-1] if memory else 0,'peakCPUPercent':max(float(s['CPUPerc'].rstrip('%')) for s in samples) if samples else 0,'exit':state['ExitCode'],'oom':state['OOMKilled'],'memoryLimitMiB':768,'cpuLimit':1.7,'memoryGoalMiB':600}
runtime=[json.loads(s) for s in (p/'runtime.jsonl').read_text().splitlines()] if (p/'runtime.jsonl').exists() else []
if runtime:
 result['runtimeSamples']=len(runtime)
 result['wallHours']=(datetime.datetime.fromisoformat(runtime[-1]['at'].replace('Z','+00:00'))-datetime.datetime.fromisoformat(runtime[0]['at'].replace('Z','+00:00'))).total_seconds()/3600
 result['peakHeapMiB']=max(r['heapBytes'] for r in runtime)/2**20
 result['peakHeapSysMiB']=max(r['heapSysBytes'] for r in runtime)/2**20
(p/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
assert len(samples)>=8,'insufficient Linux replay samples'
assert state['ExitCode']==0 and not state['OOMKilled'],'resource replay failed'
if os.getenv('TIDAL_REPLAY_MODE') != 'reference':
 assert max(memory)<600,'resource target exceeded'
 if os.getenv('TIDAL_REPLAY_LONG') == '1': assert result.get('wallHours',0)>=5.1,'long replay did not cover observed failure span'
PY
