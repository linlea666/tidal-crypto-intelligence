#!/usr/bin/env bash
# Offline Linux acceptance; credentials, actual markets and production are unused.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p tmp/resource-v23
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -c ./internal/datahub -o tmp/resource-v23/replay.test
cid=$(docker create --memory=768m --cpus=1.7 -e GOMEMLIMIT=512MiB -e TIDAL_RESOURCE_REPLAY=1 -e TIDAL_RESOURCE_OUTPUT=/out -v "$PWD/tmp/resource-v23:/out" alpine:3.22 /out/replay.test -test.run='^TestResourceReplay$' -test.v -test.timeout=22m -test.cpuprofile=/out/cpu.pprof)
trap 'docker rm -f "$cid" >/dev/null 2>&1 || true' EXIT
docker start "$cid" >/dev/null
while [[ $(docker inspect -f '{{.State.Running}}' "$cid") == true ]]; do
  docker stats --no-stream --format '{{json .}}' "$cid" >> tmp/resource-v23/samples.jsonl
  sleep 3
done
docker logs "$cid" > tmp/resource-v23/replay.log 2>&1
docker inspect "$cid" > tmp/resource-v23/container.json
cat tmp/resource-v23/replay.log
python3 - <<'PY'
import json,pathlib,re,statistics
p=pathlib.Path('tmp/resource-v23')
samples=[json.loads(s) for s in (p/'samples.jsonl').read_text().splitlines() if s]
def mib(v):
 m=re.match(r'([\d.]+)([KMGT]?i?B)',v.strip());return float(m[1])*{'B':1,'KiB':1024,'MiB':2**20,'GiB':2**30}[m[2]]/2**20
memory=[mib(s['MemUsage'].split('/')[0]) for s in samples]
state=json.loads((p/'container.json').read_text())[0]['State']
result={'samples':len(samples),'peakMiB':max(memory,default=0),'medianMiB':statistics.median(memory) if memory else 0,'lastMiB':memory[-1] if memory else 0,'peakCPUPercent':max(float(s['CPUPerc'].rstrip('%')) for s in samples) if samples else 0,'exit':state['ExitCode'],'oom':state['OOMKilled'],'memoryLimitMiB':768,'cpuLimit':1.7,'memoryGoalMiB':600}
(p/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
assert len(samples)>=20,'insufficient Linux replay samples'
assert state['ExitCode']==0 and not state['OOMKilled'],'resource replay failed'
assert max(memory)<600,'resource target exceeded'
PY
