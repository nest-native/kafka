#!/usr/bin/env bash
# One milestone. Invoke from anywhere; resolves to the project root.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p runs
LOG="runs/$(date +%Y%m%d-%H%M%S).log"
claude -p "$(cat .briefing/PROMPT.md)" --dangerously-skip-permissions \
  2>&1 | tee "$LOG"
echo
echo "=== outcome ==="
jq . .briefing/LAST_RUN.json
