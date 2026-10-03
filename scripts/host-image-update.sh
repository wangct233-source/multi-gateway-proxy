#!/bin/bash
# Host-side executor for image-mode updates (runs on the Docker host, NOT in the container).
#
# Flow: the app's updater (app/updater.py) writes data/image-update-request.json when
# an admin confirms an update. This script picks it up, pulls the prebuilt ghcr.io
# image for the candidate commit, rewrites MGP_IMAGE in .env, recreates the container,
# health-checks, and rolls back to the previous image on failure.
#
# Install (root crontab; every minute, flock prevents overlap):
#   * * * * * root flock -n /tmp/mgp-update.lock /path/to/host-image-update.sh >> /var/log/mgp-update.log 2>&1
#
# Configuration (environment or defaults):
#   MGP_DEPLOY_DIR     required: compose directory containing docker-compose.yml/.env/data/
#   MGP_COMPOSE_PROJECT compose project name (default: basename of MGP_DEPLOY_DIR)
#   MGP_IMAGE_REPO     image repository      (default: ghcr.io/wangct233-source/multi-gateway-proxy)
#   MGP_HEALTH_URL     health endpoint       (default: http://127.0.0.1:8000/healthz)
set -u

DIR="${MGP_DEPLOY_DIR:?MGP_DEPLOY_DIR must point to the compose directory}"
PROJECT="${MGP_COMPOSE_PROJECT:-$(basename "$DIR")}"
IMAGE_REPO="${MGP_IMAGE_REPO:-ghcr.io/wangct233-source/multi-gateway-proxy}"
HEALTH_URL="${MGP_HEALTH_URL:-http://127.0.0.1:8000/healthz}"
REQ="$DIR/data/image-update-request.json"
STATUS="$DIR/data/image-update-status.json"
LOG_TAG=mgp-update
[ -f "$REQ" ] || exit 0
parse() { sed -n "s|.*\"$1\" *: *\"\([^\"]*\)\".*|\1|p" "$REQ" | head -1; }
TARGET=$(parse target)
PREV_IMG=$(parse previous_image)
[ -n "$TARGET" ] || { echo "$LOG_TAG: bad request"; rm -f "$REQ"; exit 1; }
SHORT=$(echo "$TARGET" | cut -c1-7)
IMG="$IMAGE_REPO:sha-$TARGET"
echo "$LOG_TAG: pulling $IMG"
if ! docker pull "$IMG" >/dev/null 2>&1; then
  echo '{"state":"failed","last_error":"image_pull_failed"}' > "$STATUS"
  rm -f "$REQ"; exit 1
fi
cd "$DIR"
cp -p .env "${DIR}/.env.backup.$(date +%Y%m%d_%H%M%S)"
if grep -q '^MGP_IMAGE=' .env; then
  sed -i "s|^MGP_IMAGE=.*|MGP_IMAGE=$IMG|" .env
else
  echo "MGP_IMAGE=$IMG" >> .env
fi
docker compose -p "$PROJECT" up -d app 2>&1 | tail -1
OK=0
for i in $(seq 1 60); do
  sleep 3
  C=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$HEALTH_URL" || true)
  if [ "$C" = "200" ]; then OK=1; break; fi
done
if [ "$OK" = "1" ]; then
  echo "{\"state\":\"applied\",\"commit\":\"$TARGET\",\"image\":\"$IMG\"}" > "$STATUS"
  echo "$LOG_TAG: applied $SHORT"
  docker images --format '{{.Repository}}:{{.Tag}}' "$IMAGE_REPO" \
    | grep -v "$IMG" | head -n -1 | xargs -r docker image rm >/dev/null 2>&1 || true
else
  echo "{\"state\":\"rolled_back\",\"commit\":\"$TARGET\",\"last_error\":\"health_check_failed\"}" > "$STATUS"
  echo "$LOG_TAG: health failed; rolling back to $PREV_IMG"
  if [ -n "$PREV_IMG" ]; then
    sed -i "s|^MGP_IMAGE=.*|MGP_IMAGE=$PREV_IMG|" .env
    docker compose -p "$PROJECT" up -d app 2>&1 | tail -1
  fi
fi
rm -f "$REQ"
