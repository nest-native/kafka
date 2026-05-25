#!/usr/bin/env bash
# Run milestones until done or blocked. Invoke from anywhere.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p runs
while true; do
  LOG="runs/$(date +%Y%m%d-%H%M%S).log"
  claude -p "$(cat .briefing/PROMPT.md)" --dangerously-skip-permissions \
    2>&1 | tee "$LOG"
  status=$(jq -r .status .briefing/LAST_RUN.json)
  echo "=== outcome: $status ==="
  [ "$status" = "MILESTONE_MERGED" ] || break
done
