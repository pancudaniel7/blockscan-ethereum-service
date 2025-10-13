#!/bin/sh
set -eu

FILE="${1:-/add_block.lua}"
HOST="${REDIS_HOST:-redis}"
PORT="${REDIS_PORT:-6379}"
LIB="${LIBRARY_NAME:-blockchain}"

[ -f "$FILE" ] || { echo "Missing file: $FILE"; exit 1; }

RC="redis-cli -h $HOST -p $PORT"

i=0
until $RC PING >/dev/null 2>&1; do
  i=$((i+1)); [ "$i" -gt 60 ] && { echo "Redis not ready at $HOST:$PORT"; exit 1; }
  sleep 1
done

CLUSTER_ENABLED="$($RC INFO cluster 2>/dev/null | grep "^cluster_enabled:" | cut -d: -f2 | tr -d '\r')"

load_on_node() {
  nhost="$1"; nport="$2"
  j=0
  until redis-cli -h "$nhost" -p "$nport" PING >/dev/null 2>&1; do
    j=$((j+1)); [ "$j" -gt 60 ] && { echo "Node $nhost:$nport not ready"; exit 1; }
    sleep 1
  done
  echo "Loading $LIB on $nhost:$nport"
  tr -d '\r' < "$FILE" | redis-cli -h "$nhost" -p "$nport" -x FUNCTION LOAD REPLACE
  if ! redis-cli -h "$nhost" -p "$nport" FUNCTION LIST LIBRARYNAME "$LIB" | grep -q "$LIB"; then
    echo "Failed to verify library $LIB on $nhost:$nport"; exit 1
  fi
}

if [ "$CLUSTER_ENABLED" = "1" ]; then
  $RC CLUSTER NODES \
  | while IFS=' ' read -r id addr flags _; do
      case "$flags" in
        *master*) case "$flags" in *fail* ) ;; * ) echo "$addr" ;; esac ;;
      esac
    done \
  | cut -d@ -f1 \
  | while IFS= read -r node; do
      nhost="${node%:*}"; nport="${node#*:}"
      load_on_node "$nhost" "$nport"
    done
else
  load_on_node "$HOST" "$PORT"
fi

echo "Loaded $LIB functions OK"
