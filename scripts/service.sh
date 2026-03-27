#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RUN_DIR="$ROOT_DIR/.run"
LOG_DIR="$RUN_DIR/logs"
CONFIG_FILE="$RUN_DIR/service.env"

DEFAULT_HOST="0.0.0.0"
DEFAULT_BACKEND_PORT="8000"
DEFAULT_FRONTEND_PORT="5173"
DEFAULT_RELOAD="1"
START_TIMEOUT="45"
STOP_TIMEOUT="20"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/service.sh <start|stop|restart|status> [all|backend|frontend] [options]

Examples:
  bash scripts/service.sh start --workspace-root analysis
  bash scripts/service.sh start --analysis-dir analysis/workspace
  bash scripts/service.sh restart
  bash scripts/service.sh stop frontend
  bash scripts/service.sh status

Options:
  --workspace-root PATH Workspace root containing data/, workspace/, and .tune/config.yaml
  --analysis-dir PATH   Legacy config path; also accepts the workspace root for compatibility.
  --host HOST           Host for backend and frontend. Default: 0.0.0.0
  --backend-port PORT   Backend port. Default: 8000
  --frontend-port PORT  Frontend port. Default: 5173
  --reload              Enable backend reload mode.
  --no-reload           Disable backend reload mode.
  -h, --help            Show this help message.

Notes:
  - Runtime state is stored in .run/
  - Frontend runs with Vite dev server and --strictPort so port drift is treated as failure.
  - Process control uses pid files, listening ports, and command matching to recover from stale state.
EOF
}

mkdir -p "$RUN_DIR" "$LOG_DIR"

trim() {
  local value="${1:-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

is_running() {
  local pid="${1:-}"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

pid_file_for() {
  local component="$1"
  echo "$RUN_DIR/${component}.pid"
}

log_file_for() {
  local component="$1"
  echo "$LOG_DIR/${component}.log"
}

screen_session_for() {
  local component="$1"
  echo "tune-${component}-service"
}

component_label() {
  local component="$1"
  case "$component" in
    backend) echo "Backend" ;;
    frontend) echo "Frontend" ;;
    *) echo "$component" ;;
  esac
}

url_for() {
  local component="$1"
  case "$component" in
    backend) echo "http://${HOST}:${BACKEND_PORT}" ;;
    frontend) echo "http://${HOST}:${FRONTEND_PORT}" ;;
    *) return 1 ;;
  esac
}

probe_url_for() {
  local component="$1"
  case "$component" in
    backend) echo "http://127.0.0.1:${BACKEND_PORT}/api/jobs/" ;;
    frontend) echo "http://127.0.0.1:${FRONTEND_PORT}/" ;;
    *) return 1 ;;
  esac
}

port_for_component() {
  local component="$1"
  case "$component" in
    backend) echo "$BACKEND_PORT" ;;
    frontend) echo "$FRONTEND_PORT" ;;
    *) return 1 ;;
  esac
}

load_config() {
  if [[ -f "$CONFIG_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$CONFIG_FILE"
  fi

  HOST="${HOST:-$DEFAULT_HOST}"
  BACKEND_PORT="${BACKEND_PORT:-$DEFAULT_BACKEND_PORT}"
  FRONTEND_PORT="${FRONTEND_PORT:-$DEFAULT_FRONTEND_PORT}"
  RELOAD="${RELOAD:-$DEFAULT_RELOAD}"
  ANALYSIS_DIR="${ANALYSIS_DIR:-}"
}

require_screen() {
  if ! command -v screen >/dev/null 2>&1; then
    echo "screen is required but not installed." >&2
    exit 1
  fi
}

save_config() {
  cat > "$CONFIG_FILE" <<EOF
ANALYSIS_DIR=$(printf '%q' "$ANALYSIS_DIR")
HOST=$(printf '%q' "$HOST")
BACKEND_PORT=$(printf '%q' "$BACKEND_PORT")
FRONTEND_PORT=$(printf '%q' "$FRONTEND_PORT")
RELOAD=$(printf '%q' "$RELOAD")
EOF
}

resolve_analysis_dir() {
  local raw="$1"
  if [[ -z "$raw" ]]; then
    return 0
  fi
  if [[ ! -d "$raw" ]]; then
    echo "Analysis directory does not exist: $raw" >&2
    exit 1
  fi
  ANALYSIS_DIR="$(cd "$raw" && pwd)"
}

component_selected() {
  local component="$1"
  case "$TARGET" in
    all) [[ "$component" == "backend" || "$component" == "frontend" ]] ;;
    *) [[ "$component" == "$TARGET" ]] ;;
  esac
}

require_backend_analysis_dir() {
  if [[ -z "$ANALYSIS_DIR" ]]; then
    echo "Backend start requires --workspace-root/--analysis-dir, or a previously saved value in .run/service.env." >&2
    exit 1
  fi
}

backend_command_prefix() {
  if command -v tune >/dev/null 2>&1; then
    echo "tune"
    return 0
  fi
  if [[ -x "$ROOT_DIR/.venv/bin/tune" ]]; then
    echo "$ROOT_DIR/.venv/bin/tune"
    return 0
  fi
  echo "Unable to find the tune CLI. Activate the environment or install the project CLI." >&2
  exit 1
}

command_for_pid() {
  local pid="${1:-}"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  ps -p "$pid" -o command= 2>/dev/null || true
}

process_matches_component() {
  local component="$1"
  local pid="$2"
  local cmd
  cmd="$(command_for_pid "$pid")"
  case "$component" in
    backend)
      [[ "$cmd" == *"tune start"* || "$cmd" == *"uvicorn"* || "$cmd" == *"tune.api.app:app"* || "$cmd" == *"watchfiles"* ]]
      ;;
    frontend)
      [[ "$cmd" == *"$ROOT_DIR/frontend/node_modules/.bin/vite"* || "$cmd" == *"vite"* || "$cmd" == "npm run dev"* || "$cmd" == *"npm run dev "* ]]
      ;;
    *)
      return 1
      ;;
  esac
}

append_unique_pid() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  local existing
  for existing in "${PID_ACCUM[@]:-}"; do
    if [[ "$existing" == "$pid" ]]; then
      return 0
    fi
  done
  PID_ACCUM+=("$pid")
}

listener_pids_for_port() {
  local port="$1"
  lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true
}

screen_session_ids() {
  local session="$1"
  screen -ls 2>/dev/null | awk -v target=".$session" '$1 ~ target {print $1}'
}

screen_session_exists() {
  local session="$1"
  [[ -n "$(screen_session_ids "$session" | head -n 1 || true)" ]]
}

stop_screen_session() {
  local session="$1"
  local entry
  while IFS= read -r entry; do
    entry="$(trim "$entry")"
    [[ -n "$entry" ]] || continue
    screen -S "$entry" -X quit >/dev/null 2>&1 || true
  done < <(screen_session_ids "$session")
}

direct_child_pids() {
  local pid="$1"
  ps -axo pid=,ppid= | awk -v target="$pid" '$2 == target {print $1}'
}

parent_pid_for() {
  local pid="$1"
  ps -p "$pid" -o ppid= 2>/dev/null | tr -d ' ' || true
}

collect_descendants() {
  local pid="$1"
  local child
  while IFS= read -r child; do
    child="$(trim "$child")"
    [[ -n "$child" ]] || continue
    append_unique_pid "$child"
    collect_descendants "$child"
  done < <(direct_child_pids "$pid")
}

collect_matching_ancestors() {
  local component="$1"
  local pid="$2"
  local current parent
  current="$pid"
  while true; do
    parent="$(parent_pid_for "$current")"
    [[ -n "$parent" ]] || break
    [[ "$parent" =~ ^[0-9]+$ ]] || break
    [[ "$parent" -gt 1 ]] || break
    if process_matches_component "$component" "$parent"; then
      append_unique_pid "$parent"
      current="$parent"
      continue
    fi
    break
  done
}

seed_pids_for_component() {
  local component="$1"
  local port pid_file pid
  port="$(port_for_component "$component")"
  pid_file="$(pid_file_for "$component")"

  if [[ -f "$pid_file" ]]; then
    pid="$(trim "$(cat "$pid_file" 2>/dev/null || true)")"
    if [[ -n "$pid" ]]; then
      append_unique_pid "$pid"
    fi
  fi

  while IFS= read -r pid; do
    pid="$(trim "$pid")"
    [[ -n "$pid" ]] || continue
    append_unique_pid "$pid"
  done < <(listener_pids_for_port "$port")

  if [[ "$component" == "frontend" ]]; then
    while IFS= read -r pid; do
      pid="$(trim "$pid")"
      [[ -n "$pid" ]] || continue
      append_unique_pid "$pid"
    done < <(pgrep -f "$ROOT_DIR/frontend/node_modules/.bin/vite" 2>/dev/null || true)
    while IFS= read -r pid; do
      pid="$(trim "$pid")"
      [[ -n "$pid" ]] || continue
      append_unique_pid "$pid"
    done < <(pgrep -f "npm run dev" 2>/dev/null || true)
  fi

  if [[ "$component" == "backend" ]]; then
    while IFS= read -r pid; do
      pid="$(trim "$pid")"
      [[ -n "$pid" ]] || continue
      append_unique_pid "$pid"
    done < <(pgrep -f "tune start" 2>/dev/null || true)
    while IFS= read -r pid; do
      pid="$(trim "$pid")"
      [[ -n "$pid" ]] || continue
      append_unique_pid "$pid"
    done < <(pgrep -f "uvicorn" 2>/dev/null || true)
  fi
}

component_pids() {
  local component="$1"
  PID_ACCUM=()
  seed_pids_for_component "$component"

  local seed
  local seeds=("${PID_ACCUM[@]:-}")
  for seed in "${seeds[@]}"; do
    append_unique_pid "$seed"
    collect_descendants "$seed"
    collect_matching_ancestors "$component" "$seed"
  done

  local pid
  for pid in "${PID_ACCUM[@]:-}"; do
    if is_running "$pid" && process_matches_component "$component" "$pid"; then
      echo "$pid"
    fi
  done | awk '!seen[$0]++'
}

refresh_pid_file() {
  local component="$1"
  local pid_file pid
  pid_file="$(pid_file_for "$component")"
  pid="$(component_pids "$component" | head -n 1 || true)"
  if [[ -n "$pid" ]]; then
    echo "$pid" > "$pid_file"
  else
    rm -f "$pid_file"
  fi
}

component_running() {
  local component="$1"
  [[ -n "$(component_pids "$component" | head -n 1 || true)" ]]
}

port_conflict_pid() {
  local component="$1"
  local port pid
  port="$(port_for_component "$component")"
  while IFS= read -r pid; do
    pid="$(trim "$pid")"
    [[ -n "$pid" ]] || continue
    if ! process_matches_component "$component" "$pid"; then
      echo "$pid"
      return 0
    fi
  done < <(listener_pids_for_port "$port")
  return 1
}

wait_for_http() {
  local url="$1"
  local timeout="${2:-$START_TIMEOUT}"
  local waited=0
  while [[ "$waited" -lt "$timeout" ]]; do
    if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

wait_for_port_clear() {
  local port="$1"
  local timeout="${2:-$STOP_TIMEOUT}"
  local waited=0
  while [[ "$waited" -lt "$timeout" ]]; do
    if [[ -z "$(listener_pids_for_port "$port")" ]]; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

start_backend() {
  local pid_file log_file cmd session
  pid_file="$(pid_file_for backend)"
  log_file="$(log_file_for backend)"
  session="$(screen_session_for backend)"

  if component_running backend; then
    refresh_pid_file backend
    echo "Backend already running: pid=$(cat "$pid_file") url=$(url_for backend)"
    return 0
  fi

  if port_conflict_pid backend >/dev/null 2>&1; then
    echo "Backend port ${BACKEND_PORT} is occupied by an unrelated process: pid=$(port_conflict_pid backend)" >&2
    exit 1
  fi

  require_backend_analysis_dir
  require_screen
  cmd="$(backend_command_prefix)"
  : > "$log_file"
  stop_screen_session "$session"

  local launch_cmd
  launch_cmd="$cmd start --analysis-dir $(printf '%q' "$ANALYSIS_DIR") --host $(printf '%q' "$HOST") --port $(printf '%q' "$BACKEND_PORT")"
  if [[ "$RELOAD" == "1" ]]; then
    launch_cmd+=" --reload"
  fi

  local dev_env=""
  if [[ "$RELOAD" == "1" ]]; then
    dev_env="export TUNE_INLINE_TASKS=1; "
  fi

  screen -dmS "$session" bash -lc "cd $(printf '%q' "$ROOT_DIR") && ${dev_env}exec >>$(printf '%q' "$log_file") 2>&1 && exec $launch_cmd"

  if wait_for_http "$(probe_url_for backend)" "$START_TIMEOUT"; then
    refresh_pid_file backend
    echo "Backend started: pid=$(cat "$pid_file") session=$session url=$(url_for backend) log=$log_file"
    return 0
  fi

  refresh_pid_file backend
  stop_screen_session "$session"
  echo "Backend failed to start. Check log: $log_file" >&2
  tail -n 80 "$log_file" >&2 || true
  exit 1
}

start_frontend() {
  local pid_file log_file session
  pid_file="$(pid_file_for frontend)"
  log_file="$(log_file_for frontend)"
  session="$(screen_session_for frontend)"

  if component_running frontend; then
    refresh_pid_file frontend
    echo "Frontend already running: pid=$(cat "$pid_file") url=$(url_for frontend)"
    return 0
  fi

  if port_conflict_pid frontend >/dev/null 2>&1; then
    echo "Frontend port ${FRONTEND_PORT} is occupied by an unrelated process: pid=$(port_conflict_pid frontend)" >&2
    exit 1
  fi

  if [[ ! -d "$ROOT_DIR/frontend" ]]; then
    echo "Frontend directory not found: $ROOT_DIR/frontend" >&2
    exit 1
  fi

  require_screen
  : > "$log_file"
  stop_screen_session "$session"
  screen -dmS "$session" bash -lc "cd $(printf '%q' "$ROOT_DIR/frontend") && exec >>$(printf '%q' "$log_file") 2>&1 && exec npm run dev -- --host $(printf '%q' "$HOST") --port $(printf '%q' "$FRONTEND_PORT") --strictPort"

  if wait_for_http "$(probe_url_for frontend)" "$START_TIMEOUT"; then
    refresh_pid_file frontend
    echo "Frontend started: pid=$(cat "$pid_file") session=$session url=$(url_for frontend) log=$log_file"
    return 0
  fi

  refresh_pid_file frontend
  stop_screen_session "$session"
  echo "Frontend failed to start. Check log: $log_file" >&2
  tail -n 80 "$log_file" >&2 || true
  exit 1
}

stop_component() {
  local component="$1"
  local pid_file port pid
  pid_file="$(pid_file_for "$component")"
  port="$(port_for_component "$component")"
  local session
  local had_session="0"
  session="$(screen_session_for "$component")"

  if screen_session_exists "$session"; then
    had_session="1"
  fi
  stop_screen_session "$session"

  PIDS=()
  while IFS= read -r pid; do
    pid="$(trim "$pid")"
    [[ -n "$pid" ]] || continue
    PIDS+=("$pid")
  done < <(component_pids "$component")
  if [[ "${#PIDS[@]}" -eq 0 ]]; then
    rm -f "$pid_file"
    if [[ "$had_session" == "1" ]]; then
      wait_for_port_clear "$port" 5 || true
      echo "$(component_label "$component") stopped: session=$session"
      return 0
    fi
    echo "$(component_label "$component") already stopped."
    return 0
  fi

  local unique_desc=""
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
    unique_desc+="${pid} "
  done

  local waited=0
  while [[ "$waited" -lt "$STOP_TIMEOUT" ]]; do
    local still_running=0
    for pid in "${PIDS[@]}"; do
      if is_running "$pid"; then
        still_running=1
        break
      fi
    done
    if [[ "$still_running" -eq 0 ]] && wait_for_port_clear "$port" 1; then
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done

  for pid in "${PIDS[@]}"; do
    if is_running "$pid"; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done

  rm -f "$pid_file"
  wait_for_port_clear "$port" 5 || true
  echo "$(component_label "$component") stopped: ${unique_desc% }"
}

status_component() {
  local component="$1"
  local pid_file log_file pid session screen_state
  pid_file="$(pid_file_for "$component")"
  log_file="$(log_file_for "$component")"
  session="$(screen_session_for "$component")"
  refresh_pid_file "$component"
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  screen_state="absent"
  if screen_session_exists "$session"; then
    screen_state="$session"
  fi

  if [[ -n "$pid" ]] && component_running "$component"; then
    echo "$(component_label "$component"): running pid=$pid session=$screen_state url=$(url_for "$component") log=$log_file"
    return 0
  fi

  echo "$(component_label "$component"): stopped session=$screen_state log=$log_file"
}

COMMAND="${1:-}"
if [[ -z "$COMMAND" ]]; then
  usage
  exit 1
fi
shift

case "$COMMAND" in
  start|stop|restart|status) ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown command: $COMMAND" >&2
    usage
    exit 1
    ;;
esac

TARGET="all"
if [[ $# -gt 0 ]]; then
  case "$1" in
    all|backend|frontend)
      TARGET="$1"
      shift
      ;;
  esac
fi

ANALYSIS_DIR=""
HOST=""
BACKEND_PORT=""
FRONTEND_PORT=""
RELOAD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace-root)
      [[ $# -ge 2 ]] || { echo "Missing value for --workspace-root" >&2; exit 1; }
      ANALYSIS_DIR="$2"
      shift 2
      ;;
    --analysis-dir)
      [[ $# -ge 2 ]] || { echo "Missing value for --analysis-dir" >&2; exit 1; }
      ANALYSIS_DIR="$2"
      shift 2
      ;;
    --host)
      [[ $# -ge 2 ]] || { echo "Missing value for --host" >&2; exit 1; }
      HOST="$2"
      shift 2
      ;;
    --backend-port)
      [[ $# -ge 2 ]] || { echo "Missing value for --backend-port" >&2; exit 1; }
      BACKEND_PORT="$2"
      shift 2
      ;;
    --frontend-port)
      [[ $# -ge 2 ]] || { echo "Missing value for --frontend-port" >&2; exit 1; }
      FRONTEND_PORT="$2"
      shift 2
      ;;
    --reload)
      RELOAD="1"
      shift
      ;;
    --no-reload)
      RELOAD="0"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

load_config
resolve_analysis_dir "$ANALYSIS_DIR"

case "$COMMAND" in
  start)
    if component_selected backend; then
      require_backend_analysis_dir
    fi
    save_config
    if component_selected backend; then
      start_backend
    fi
    if component_selected frontend; then
      start_frontend
    fi
    ;;
  stop)
    if component_selected frontend; then
      stop_component frontend
    fi
    if component_selected backend; then
      stop_component backend
    fi
    ;;
  restart)
    if component_selected frontend; then
      stop_component frontend
    fi
    if component_selected backend; then
      stop_component backend
      require_backend_analysis_dir
    fi
    save_config
    if component_selected backend; then
      start_backend
    fi
    if component_selected frontend; then
      start_frontend
    fi
    ;;
  status)
    if component_selected backend; then
      status_component backend
    fi
    if component_selected frontend; then
      status_component frontend
    fi
    ;;
esac
