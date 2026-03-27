#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/dev.sh [--workspace-root PATH] [--analysis-dir PATH] [--host HOST] [--backend-port PORT] [--frontend-port PORT]

Behavior:
  - Always performs a full development restart.
  - Stops the current backend and frontend first.
  - Restarts backend with --reload and frontend with Vite hot reload.

Examples:
  bash scripts/dev.sh --workspace-root analysis
  bash scripts/dev.sh --analysis-dir analysis/workspace
  bash scripts/dev.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

exec bash "$SCRIPT_DIR/service.sh" restart all --reload "$@"
