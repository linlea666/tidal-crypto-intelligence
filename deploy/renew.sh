#!/usr/bin/env bash
set -euo pipefail
root=/opt/tidal
exec 8>"$root/certbot.lock"
flock -n 8 || exit 0
# The systemd timer already spreads requests with RandomizedDelaySec. A second
# Certbot sleep could exceed the service's bounded runtime before renewal starts.
docker run --rm --memory 192m --cpus .5 -v "$root/letsencrypt:/etc/letsencrypt" -v "$root/acme:/var/www/acme" -v "$root/certbot-logs:/var/log/letsencrypt" certbot/certbot:v5.4.0 renew --quiet --no-random-sleep-on-renew
if docker inspect tidal-gateway-1 >/dev/null 2>&1; then docker exec tidal-gateway-1 nginx -t && docker exec tidal-gateway-1 nginx -s reload; fi
find "$root/certbot-logs" -type f -mtime +7 -delete
