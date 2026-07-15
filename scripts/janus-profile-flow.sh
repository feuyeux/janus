#!/usr/bin/env sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RUN_DIR="$PROJECT_ROOT/.run"
LOAD_LOG="$RUN_DIR/janus-load.log"

CPU_DURATION=30
MEM_DURATION=30
MEM_EVENT="alloc"
PARALLELISM=4
PAUSE_MILLIS=0
WS_URL="ws://127.0.0.1:8080/json"
LOAD_EXTRA_SECONDS=10
START_ARGS=""
FLAME_ARGS=""

usage() {
    cat <<'EOF'
Usage:
  scripts/janus-profile-flow.sh [options]

Flow:
  1. Start local Janus service
  2. Generate WebSocket JSON traffic
  3. Capture CPU and memory flame graphs with Arthas

Options:
  --cpu-duration <sec>      Default: 30
  --mem-duration <sec>      Default: 30
  --mem-event <event>       Default: alloc
  --parallelism <n>         Default: 4
  --pause-millis <ms>       Default: 0
  --ws-url <url>            Default: ws://127.0.0.1:8080/json
  --live                    Pass --live to arthas-flame.sh
  --force-restart           Restart local Janus if already running
  -h, --help                Show this help.
EOF
}

fail() {
    printf '%s\n' "ERROR: $*" >&2
    exit 1
}

need_value() {
    [ $# -ge 2 ] || fail "Missing value for $1"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --cpu-duration)
            need_value "$@"
            CPU_DURATION="$2"
            shift 2
            ;;
        --mem-duration)
            need_value "$@"
            MEM_DURATION="$2"
            shift 2
            ;;
        --mem-event)
            need_value "$@"
            MEM_EVENT="$2"
            shift 2
            ;;
        --parallelism)
            need_value "$@"
            PARALLELISM="$2"
            shift 2
            ;;
        --pause-millis)
            need_value "$@"
            PAUSE_MILLIS="$2"
            shift 2
            ;;
        --ws-url)
            need_value "$@"
            WS_URL="$2"
            shift 2
            ;;
        --live)
            FLAME_ARGS="$FLAME_ARGS --live"
            shift
            ;;
        --force-restart)
            START_ARGS="$START_ARGS --force-restart"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "Unknown argument: $1"
            ;;
    esac
done

mkdir -p "$RUN_DIR"

printf '%s\n' "[1/3] Starting Janus"
# shellcheck disable=SC2086
"$SCRIPT_DIR/janus-local-start.sh" $START_ARGS

LOAD_DURATION=$(( CPU_DURATION + MEM_DURATION + LOAD_EXTRA_SECONDS ))
printf '%s\n' "[2/3] Generating Janus load for ${LOAD_DURATION}s"
"$SCRIPT_DIR/janus-request.sh" \
    --url "$WS_URL" \
    --duration-seconds "$LOAD_DURATION" \
    --parallelism "$PARALLELISM" \
    --pause-millis "$PAUSE_MILLIS" >"$LOAD_LOG" 2>&1 &
LOAD_PID=$!

cleanup() {
    if kill -0 "$LOAD_PID" 2>/dev/null; then
        kill "$LOAD_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT HUP TERM

sleep 2

printf '%s\n' "[3/3] Capturing flame graphs"
# shellcheck disable=SC2086
"$SCRIPT_DIR/arthas-flame.sh" \
    --cpu-duration "$CPU_DURATION" \
    --mem-duration "$MEM_DURATION" \
    --mem-event "$MEM_EVENT" $FLAME_ARGS

wait "$LOAD_PID"
trap - EXIT INT HUP TERM
printf '%s\n' "Load log: $LOAD_LOG"
