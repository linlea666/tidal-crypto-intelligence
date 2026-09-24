#!/usr/bin/env bash
set -euo pipefail
umask 077
[[ $# == 3 ]] || exit 64
digest=$1
revision=$2
version=$3
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ && "$revision" =~ ^[0-9a-f]{40}$ && "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 64
root=/opt/tidal
repo=linlea666/tidal-crypto-intelligence
image="ghcr.io/$repo@$digest"
exec 9>"$root/deploy.lock"
flock -w 600 9
available=$(df -Pk "$root" | awk 'NR==2 {print $4}')
(( available >= 8388608 )) || { echo 'Less than 8 GiB free; deployment refused.'; exit 1; }
[[ -s "$root/letsencrypt/live/tidal-ip/fullchain.pem" ]] || { echo "TLS certificate missing"; exit 1; }
# Validate the release and immutable revision before accepting any deployment input.
python3 - "$repo" "$version" "$revision" <<'PY'
import json,sys,urllib.request
repo,tag,sha=sys.argv[1:]
def get(path):
 req=urllib.request.Request('https://api.github.com/repos/'+repo+path,headers={'User-Agent':'Tidal-Deploy','Accept':'application/vnd.github+json'})
 return json.load(urllib.request.urlopen(req,timeout=25))
r=get('/releases/tags/'+tag)
assert not r['draft'] and not r['prerelease'], 'Only stable published releases may deploy'
ref=get('/git/ref/tags/'+tag)['object']
if ref['type']=='tag': ref=get('/git/tags/'+ref['sha'])['object']
assert ref['sha']==sha and ref['type']=='commit', 'Release revision mismatch'
PY
# Anonymous pull deliberately verifies that the published package is public.
docker pull "$image"
actual=$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$image")
[[ "$actual" == "$revision" ]] || { echo 'Image revision label mismatch'; exit 1; }
release="$root/releases/$revision"
install -d -m 755 "$release" "$root/backups"
archive=$(mktemp)
trap 'rm -f "$archive"' EXIT
curl --fail --location --retry 3 --max-time 120 "https://codeload.github.com/$repo/tar.gz/$revision" -o "$archive"
tar -xzf "$archive" --strip-components=2 -C "$release" --wildcards '*/deploy/compose.yaml' '*/deploy/nginx.conf' '*/deploy/renew.sh' '*/deploy/metrics.sh' '*/deploy/deploy.sh' '*/deploy/report.sh'
chmod 644 "$release/compose.yaml" "$release/nginx.conf"
old_release=$(readlink "$root/current" || true)
old_image=''
if [[ -f "$root/state.env" ]]; then old_image=$(sed -n 's/^TIDAL_IMAGE=//p' "$root/state.env"); fi
# Online SQLite backup includes WAL transactions and avoids copying live WAL files.
if [[ -f "$root/data/state.sqlite" ]]; then
  sqlite3 "$root/data/state.sqlite" ".timeout 10000" ".backup '$root/backups/state-$version.sqlite'"
fi
[[ ! -f "$root/state.env" ]] || cp "$root/state.env" "$root/backups/state-$version.env"
ln -sfn "$release" "$root/current.next"
mv -Tf "$root/current.next" "$root/current"
printf 'TIDAL_IMAGE=%s\n' "$image" > "$root/state.env"
compose=(docker compose --env-file "$root/state.env" -f "$root/current/compose.yaml")
rollback() {
  echo 'Health check failed; restoring previous application and configuration.'
  if [[ -n "$old_image" && -n "$old_release" ]]; then
    printf 'TIDAL_IMAGE=%s\n' "$old_image" > "$root/state.env"
    ln -sfn "$old_release" "$root/current.next"; mv -Tf "$root/current.next" "$root/current"
    "${compose[@]}" up -d --force-recreate app gateway
    for attempt in $(seq 1 30); do
      if curl --fail --silent --max-time 5 http://127.0.0.1:8080/readyz >/dev/null; then echo 'Previous stable image restored.'; return 0; fi
      sleep 3
    done
    echo 'Rollback requires attention: previous image did not become ready.'
  fi
  return 1
}
if docker inspect tidal-acme-bootstrap >/dev/null 2>&1; then docker rm -f tidal-acme-bootstrap >/dev/null; fi
if ! "${compose[@]}" up -d --force-recreate app gateway; then rollback || true; exit 1; fi
healthy=false
for attempt in $(seq 1 60); do
  if curl --fail --silent --max-time 5 http://127.0.0.1:8080/readyz >/dev/null && curl --fail --silent --max-time 5 "https://$(cat "$root/public-ip"):8443/healthz" >/dev/null; then healthy=true; break; fi
  sleep 3
done
if [[ "$healthy" != true ]]; then rollback || true; exit 1; fi
if [[ -n "$old_image" && "$old_image" != "$image" ]]; then printf '%s\n' "$old_image" > "$root/previous-image"; fi
printf '%s %s %s\n' "$version" "$revision" "$digest" > "$root/deployed-version"
install -m 700 "$release/deploy.sh" "$root/bootstrap/deploy.sh"
install -m 700 "$release/renew.sh" "$root/bootstrap/renew.sh"
install -m 700 "$release/metrics.sh" "$root/bootstrap/metrics.sh"
install -m 700 "$release/report.sh" "$root/bootstrap/report.sh"
# Backups and application images are bounded; keep current plus previous stable.
find "$root/backups" -type f -mtime +7 -delete
previous=$(cat "$root/previous-image" 2>/dev/null || true)
current_id=$(docker image inspect --format '{{.Id}}' "$image")
previous_id=''
[[ -z "$previous" ]] || previous_id=$(docker image inspect --format '{{.Id}}' "$previous" 2>/dev/null || true)
while read -r id; do
  [[ -n "$id" && "$id" != "$current_id" && "$id" != "$previous_id" ]] || continue
  docker image rm "$id" >/dev/null 2>&1 || true
done < <(docker image ls "ghcr.io/$repo" --format '{{.ID}}' --no-trunc | sort -u)
systemctl enable --now tidal-metrics.timer >/dev/null
"$root/bootstrap/metrics.sh" || true
echo "Deployed $version at $revision with $digest"
