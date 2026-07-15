#!/usr/bin/env sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
RUN_DIR="$PROJECT_ROOT/.run"
PID_FILE="$RUN_DIR/janus-local.pid"
LOG_FILE="$RUN_DIR/janus-local.log"
JAR_FILE="$PROJECT_ROOT/target/janus.jar"

WS_PORT=8080
GRPC_PORT=9090
METRICS_PORT=9100
BUILD_IF_MISSING=1
FORCE_RESTART=0
WAIT_SECONDS=30

usage() {
    cat <<'EOF'
Usage:
  scripts/janus-local-start.sh [options]

Options:
  --ws-port <port>         WebSocket port. Default: 8080
  --grpc-port <port>       gRPC port. Default: 9090
  --metrics-port <port>    Metrics port. Default: 9100
  --no-build               Do not run Maven when target/janus.jar is missing.
  --force-restart          Stop the pid in .run/janus-local.pid before restart.
  --wait-seconds <sec>     Readiness wait timeout. Default: 30
  -h, --help               Show this help.
EOF
}

fail() {
    printf '%s\n' "ERROR: $*" >&2
    exit 1
}

need_value() {
    [ $# -ge 2 ] || fail "Missing value for $1"
}

is_alive() {
    kill -0 "$1" 2>/dev/null
}

while [ $# -gt 0 ]; do
    case "$1" in
        --ws-port)
            need_value "$@"
            WS_PORT="$2"
            shift 2
            ;;
        --grpc-port)
            need_value "$@"
            GRPC_PORT="$2"
            shift 2
            ;;
        --metrics-port)
            need_value "$@"
            METRICS_PORT="$2"
            shift 2
            ;;
        --no-build)
            BUILD_IF_MISSING=0
            shift
            ;;
        --force-restart)
            FORCE_RESTART=1
            shift
            ;;
        --wait-seconds)
            need_value "$@"
            WAIT_SECONDS="$2"
            shift 2
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

if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if is_alive "$OLD_PID"; then
        if [ "$FORCE_RESTART" -eq 1 ]; then
            printf '%s\n' "Stopping existing Janus process: $OLD_PID"
            kill "$OLD_PID"
            rm -f "$PID_FILE"
        else
            printf '%s\n' "Janus already running with pid $OLD_PID"
            printf '%s\n' "Log: $LOG_FILE"
            exit 0
        fi
    else
        rm -f "$PID_FILE"
    fi
fi

if [ ! -f "$JAR_FILE" ]; then
    [ "$BUILD_IF_MISSING" -eq 1 ] || fail "$JAR_FILE not found. Run mvn package first or omit --no-build."
    printf '%s\n' "Building target/janus.jar with Maven"
    (cd "$PROJECT_ROOT" && mvn -DskipTests package)
fi

if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
else
    JAVA_CMD=$(command -v java 2>/dev/null || true)
fi
[ -n "${JAVA_CMD:-}" ] || fail "java not found. Please install JDK/JRE or set JAVA_HOME."

printf '%s\n' "Starting Janus locally on ws=$WS_PORT grpc=$GRPC_PORT metrics=$METRICS_PORT"
cd "$PROJECT_ROOT"
nohup env \
    JANUS_SERVER_ID=local-profiler \
    JANUS_ADVERTISED_HOST=localhost \
    JANUS_HOST=0.0.0.0 \
    JANUS_WS_PORT="$WS_PORT" \
    JANUS_GRPC_PORT="$GRPC_PORT" \
    JANUS_METRICS_PORT="$METRICS_PORT" \
    JANUS_DOWNSTREAM_PROTOCOL=none \
    JANUS_DOWNSTREAM_DISCOVERY=none \
    JANUS_REGISTER=none \
    JANUS_OTEL_ENABLED=N \
    JANUS_METRICS_ENABLED=Y \
    "$JAVA_CMD" -jar "$JAR_FILE" >"$LOG_FILE" 2>&1 </dev/null &
echo $! >"$PID_FILE"

PID=$(cat "$PID_FILE")
DEADLINE=$(( $(date +%s) + WAIT_SECONDS ))
READY_URL="http://127.0.0.1:$METRICS_PORT/metrics"

while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    if ! is_alive "$PID"; then
        tail -n 50 "$LOG_FILE" >&2 || true
        fail "Janus exited during startup"
    fi
    if command -v curl >/dev/null 2>&1 && (
        curl -fsS "$READY_URL" >/dev/null 2>&1 ||
        curl -fsS "http://127.0.0.1:$METRICS_PORT/" >/dev/null 2>&1
    ); then
        printf '%s\n' "Janus ready. pid=$PID"
        printf '%s\n' "Log: $LOG_FILE"
        exit 0
    fi
    if grep -q "Janus Server started successfully" "$LOG_FILE" 2>/dev/null; then
        printf '%s\n' "Janus ready. pid=$PID"
        printf '%s\n' "Log: $LOG_FILE"
        exit 0
    fi
    sleep 1
done

printf '%s\n' "Timed out waiting for $READY_URL" >&2
tail -n 50 "$LOG_FILE" >&2 || true
exit 1
