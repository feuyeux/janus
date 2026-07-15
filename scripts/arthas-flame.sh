#!/usr/bin/env sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

DEFAULT_ARTHAS_JAR="$PROJECT_ROOT/.tools/arthas/arthas-boot.jar"
DEFAULT_OUTPUT_DIR="$PROJECT_ROOT/arthas-output"
DEFAULT_MATCHER='org\.janus\.JanusServer|janus-server-java|janus\.jar'

ARTHAS_JAR="$DEFAULT_ARTHAS_JAR"
OUTPUT_DIR="$DEFAULT_OUTPUT_DIR"
CPU_DURATION=30
MEM_DURATION=30
MEM_EVENT="alloc"
LIVE_FLAG=0
PID=""
MATCHER="$DEFAULT_MATCHER"
SKIP_CPU=0
SKIP_MEM=0

usage() {
    cat <<'EOF'
Usage:
  scripts/arthas-flame.sh [options]
  scripts/arthas-flame.sh <pid>

Options:
  --pid <pid>                Attach to a specific JVM pid.
  --matcher <regex>          Match JVM process name from jcmd/jps output.
  --cpu-duration <seconds>   CPU flame graph duration. Default: 30
  --mem-duration <seconds>   Memory flame graph duration. Default: 30
  --mem-event <event>        Memory event. Default: alloc
  --live                     Add --live when collecting memory flame graph.
  --output-dir <path>        Output directory. Default: ./arthas-output
  --arthas-jar <path>        Reuse an existing arthas-boot.jar.
  --skip-cpu                 Only generate memory flame graph.
  --skip-mem                 Only generate CPU flame graph.
  -h, --help                 Show this help.

Examples:
  scripts/arthas-flame.sh
  scripts/arthas-flame.sh --cpu-duration 45 --mem-duration 20
  scripts/arthas-flame.sh --pid 12345 --live
EOF
}

fail() {
    printf '%s\n' "ERROR: $*" >&2
    exit 1
}

is_integer() {
    case "${1:-}" in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

need_value() {
    [ $# -ge 2 ] || fail "Missing value for $1"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --pid)
            need_value "$@"
            PID="$2"
            shift 2
            ;;
        --matcher)
            need_value "$@"
            MATCHER="$2"
            shift 2
            ;;
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
        --live)
            LIVE_FLAG=1
            shift
            ;;
        --output-dir)
            need_value "$@"
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --arthas-jar)
            need_value "$@"
            ARTHAS_JAR="$2"
            shift 2
            ;;
        --skip-cpu)
            SKIP_CPU=1
            shift
            ;;
        --skip-mem)
            SKIP_MEM=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            if [ -z "$PID" ] && is_integer "$1"; then
                PID="$1"
                shift
            else
                fail "Unknown argument: $1"
            fi
            ;;
    esac
done

[ "$SKIP_CPU" -eq 0 ] || [ "$SKIP_MEM" -eq 0 ] || fail "Cannot skip both cpu and memory profiling"
[ "$CPU_DURATION" -gt 0 ] 2>/dev/null || fail "--cpu-duration must be a positive integer"
[ "$MEM_DURATION" -gt 0 ] 2>/dev/null || fail "--mem-duration must be a positive integer"
[ -n "$MEM_EVENT" ] || fail "--mem-event cannot be empty"

if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
else
    JAVA_CMD=$(command -v java 2>/dev/null || true)
fi
[ -n "${JAVA_CMD:-}" ] || fail "java not found. Please install JDK/JRE or set JAVA_HOME."

ensure_arthas() {
    if [ -f "$ARTHAS_JAR" ]; then
        return
    fi

    mkdir -p "$(dirname "$ARTHAS_JAR")"
    printf '%s\n' "Downloading Arthas bootstrap to $ARTHAS_JAR"

    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -o "$ARTHAS_JAR" "https://arthas.aliyun.com/arthas-boot.jar"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$ARTHAS_JAR" "https://arthas.aliyun.com/arthas-boot.jar"
    else
        fail "Neither curl nor wget is available to download arthas-boot.jar"
    fi
}

list_java_processes() {
    if command -v jcmd >/dev/null 2>&1; then
        jcmd -l 2>/dev/null || true
    elif command -v jps >/dev/null 2>&1; then
        jps -l 2>/dev/null || true
    else
        fail "Neither jcmd nor jps is available. Please use a full JDK or pass --pid."
    fi
}

resolve_pid() {
    if [ -n "$PID" ]; then
        is_integer "$PID" || fail "--pid must be numeric"
        return
    fi

    MATCHED=$(list_java_processes | grep -E "$MATCHER" || true)
    MATCH_COUNT=$(printf '%s\n' "$MATCHED" | sed '/^$/d' | wc -l | tr -d ' ')

    if [ "$MATCH_COUNT" = "0" ]; then
        printf '%s\n' "Visible JVM processes:" >&2
        list_java_processes >&2
        fail "No Janus JVM matched regex: $MATCHER"
    fi

    if [ "$MATCH_COUNT" != "1" ]; then
        printf '%s\n' "Multiple JVM processes matched. Please rerun with --pid." >&2
        printf '%s\n' "$MATCHED" >&2
        exit 1
    fi

    PID=$(printf '%s\n' "$MATCHED" | awk 'NR==1 { print $1 }')
    [ -n "$PID" ] || fail "Unable to parse pid from process list"
}

run_profiler() {
    EVENT="$1"
    DURATION="$2"
    OUTPUT_FILE="$3"
    LIVE="$4"
    START_CMD="profiler start --event $EVENT"
    if [ "$LIVE" = "1" ]; then
        START_CMD="$START_CMD --live"
    fi
    STOP_CMD="profiler stop --file \"$OUTPUT_FILE\""

    printf '%s\n' "Generating ${EVENT} flame graph -> $OUTPUT_FILE"
    "$JAVA_CMD" -jar "$ARTHAS_JAR" -c "$START_CMD" "$PID"
    sleep "$DURATION"
    "$JAVA_CMD" -jar "$ARTHAS_JAR" -c "$STOP_CMD" "$PID"
}

ensure_arthas
resolve_pid

TIMESTAMP=$(date '+%Y%m%d-%H%M%S')
RUN_DIR="$OUTPUT_DIR/$TIMESTAMP-pid-$PID"
mkdir -p "$RUN_DIR"

if [ "$SKIP_CPU" -eq 0 ]; then
    run_profiler "cpu" "$CPU_DURATION" "$RUN_DIR/cpu-${CPU_DURATION}s.html" "0"
fi

if [ "$SKIP_MEM" -eq 0 ]; then
    run_profiler "$MEM_EVENT" "$MEM_DURATION" "$RUN_DIR/memory-${MEM_EVENT}-${MEM_DURATION}s.html" "$LIVE_FLAG"
fi

printf '%s\n' "Done. Flame graphs saved under: $RUN_DIR"
