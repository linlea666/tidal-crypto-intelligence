#!/usr/bin/env bash
# Packaging rehearsal on an empty disposable CI runner, with an ephemeral login.
set -euo pipefail
[[ "${CI:-}" == true && ! -e /opt/tidal ]] || { echo 'Requires an empty disposable CI runner'; exit 64; }
[[ "${TIDAL_IMAGE:-}" == ghcr.io/*@sha256:* ]] || exit 64
root=/opt/tidal
sudo install -d -m 755 "$root/data" "$root/secrets" "$root/current" "$root/acme" "$root/letsencrypt/live/tidal-ip"
sudo chown 10001:10001 "$root/data"
sudo cp deploy/nginx.conf deploy/compose.yaml "$root/current/"
compose=(docker compose -f "$root/current/compose.yaml")
tmp=$(mktemp -d)
cleanup() {
  status=$?
  if (( status != 0 )); then "${compose[@]}" logs --tail 40 || true; fi
  "${compose[@]}" down --volumes || true
  rm -rf "$tmp"
  exit "$status"
}
trap cleanup EXIT
password=$(openssl rand -hex 24)
docker run --rm -e TIDAL_PASSWORD="$password" "$TIDAL_IMAGE" hash-password > "$tmp/hash"
sudo install -o 10001 -g 10001 -m 400 "$tmp/hash" "$root/secrets/password_hash"
sudo openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=127.0.0.1 -addext subjectAltName=IP:127.0.0.1 -keyout "$root/letsencrypt/live/tidal-ip/privkey.pem" -out "$root/letsencrypt/live/tidal-ip/fullchain.pem" 2>/dev/null
"${compose[@]}" up -d
ok=false
for attempt in $(seq 1 30); do
  if curl --fail --silent --max-time 3 --cacert "$root/letsencrypt/live/tidal-ip/fullchain.pem" https://127.0.0.1/healthz > "$tmp/health"; then ok=true; break; fi
  sleep 2
done
[[ "$ok" == true ]] || exit 1
curl --fail --silent --cacert "$root/letsencrypt/live/tidal-ip/fullchain.pem" https://127.0.0.1/ | grep -q 'root'
code=$(curl --silent --output /dev/null --write-out '%{http_code}' --cacert "$root/letsencrypt/live/tidal-ip/fullchain.pem" https://127.0.0.1/api/v1/levels)
[[ "$code" == 401 ]] || exit 1
printf '{"password":"%s"}' "$password" > "$tmp/login"
curl --fail --silent --cacert "$root/letsencrypt/live/tidal-ip/fullchain.pem" -H 'Content-Type: application/json' --data-binary "@$tmp/login" -c "$tmp/cookie" https://127.0.0.1/api/v1/login >/dev/null
curl --fail --silent --cacert "$root/letsencrypt/live/tidal-ip/fullchain.pem" -b "$tmp/cookie" https://127.0.0.1/api/v1/health >/dev/null
echo 'Production Compose startup, verified TLS, authentication and API smoke checks passed.'
