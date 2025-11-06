#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$REPO_ROOT/bin"
TMP_DIR="$REPO_ROOT/tmp"
PID_DIR="$TMP_DIR/pids"
LOG_DIR="$TMP_DIR/logs"
PID_FILE="$PID_DIR/services.pids"

DEFAULT_SERVICES="scheduler plc-adapter-p1 plc-adapter-p2 engraver-adapter hmi-service flash-station release-policy device-registry validation-controller quality-service template-service genealogy-service"

# Allow Makefile SERVICES variable to override the default set.
IFS=' ' read -r -a SERVICES_TO_START <<< "${SERVICES:-$DEFAULT_SERVICES}"

mkdir -p "$PID_DIR" "$LOG_DIR"

ensure_kafka_topics() {
  local compose_file="$REPO_ROOT/docker/docker-compose.infra.yml"
  local topics=(
    "production.plan"
    "adapter.ack"
    "quality.disposition"
    "test.metrics"
    "device.registered"
    "customization.engraving.request"
    "customization.engraving.complete"
    "device.firmware.flash.request"
    "device.postflash.test"
    "genealogy.recall.query"
    "production.changeover.start"
    "production.operator.prompt"
    "validation.test.complete"
    "device.released"
  )

  if ! command -v docker-compose >/dev/null 2>&1; then
    echo "Warning: docker-compose not found; skipping Kafka topic bootstrap." >&2
    return
  fi

  if [[ ! -f "$compose_file" ]]; then
    return
  fi

  if ! docker-compose -f "$compose_file" ps kafka >/dev/null 2>&1; then
    echo "Warning: Kafka container not running; skipping Kafka topic bootstrap." >&2
    return
  fi

  for topic in "${topics[@]}"; do
    docker-compose -f "$compose_file" exec -T kafka kafka-topics \
      --bootstrap-server kafka:9092 \
      --create \
      --if-not-exists \
      --topic "$topic" \
      --replication-factor 1 \
      --partitions 1 >/dev/null 2>&1 || true
  done
}

if [[ -f "$PID_FILE" ]]; then
  echo "Services already appear to be running (found $PID_FILE). Run 'make services-down' first." >&2
  exit 1
fi

touch "$PID_FILE"

cleanup() {
  rm -f "$PID_FILE"
}
trap cleanup ERR INT

ensure_kafka_topics

for service in "${SERVICES_TO_START[@]}"; do
  binary="$BIN_DIR/$service"
  log_file="$LOG_DIR/$service.log"

  if [[ ! -x "$binary" ]]; then
    echo "Missing executable for $service at $binary. Build services with 'make services-build'." >&2
    exit 1
  fi

  env_cmd=(env)
  case "$service" in
    scheduler)
      # Avoid port clash with Kafka UI (Docker infra reserves 8080) and HMI default port (8088).
      env_cmd+=("HTTP_PORT=${HTTP_PORT_OVERRIDE:-8090}")
      ;;
    device-registry)
      # Avoid conflict with Kafka Schema Registry container on 8081.
      env_cmd+=("HTTP_PORT=${DEVICE_REGISTRY_PORT_OVERRIDE:-8181}")
      ;;
    genealogy-service)
      # Align database credentials with docker/postgres init script.
      env_cmd+=("PORT=${GENEALOGY_PORT_OVERRIDE:-8089}")
      env_cmd+=("DATABASE_URL=${GENEALOGY_DATABASE_URL_OVERRIDE:-postgres://i40admin:i40secret@localhost:5432/i40_production?sslmode=disable}")
      ;;
  esac

  echo "Starting $service..."
  "${env_cmd[@]}" "$binary" >"$log_file" 2>&1 &
  pid=$!
  echo "$service:$pid" >>"$PID_FILE"
done

trap - ERR INT
echo "All services started. Logs: $LOG_DIR"
