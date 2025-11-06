#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$REPO_ROOT/tmp/pids/services.pids"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No running services found."
  exit 0
fi

while IFS=: read -r service pid; do
  if [[ -z "$pid" ]]; then
    continue
  fi

  if kill -0 "$pid" 2>/dev/null; then
    echo "Stopping $service (pid $pid)..."
    kill "$pid" 2>/dev/null || true

    # Give the process up to 5 seconds to exit gracefully.
    for _ in {1..5}; do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 1
    done

    if kill -0 "$pid" 2>/dev/null; then
      echo "Force killing $service (pid $pid)..."
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi
done <"$PID_FILE"

rm -f "$PID_FILE"
echo "All services stopped."
