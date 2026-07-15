#!/usr/bin/env sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

URL="ws://127.0.0.1:8080/json"
DURATION_SECONDS=70
PARALLELISM=4
PAUSE_MILLIS=0

usage() {
    cat <<'EOF'
Usage:
  scripts/janus-request.sh [options]

Options:
  --url <wsUrl>             Default: ws://127.0.0.1:8080/json
  --duration-seconds <sec>  Default: 70
  --parallelism <n>         Default: 4
  --pause-millis <ms>       Default: 0
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
        --url)
            need_value "$@"
            URL="$2"
            shift 2
            ;;
        --duration-seconds)
            need_value "$@"
            DURATION_SECONDS="$2"
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
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "Unknown argument: $1"
            ;;
    esac
done

if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
else
    JAVA_CMD=$(command -v java 2>/dev/null || true)
fi
[ -n "${JAVA_CMD:-}" ] || fail "java not found. Please install JDK/JRE or set JAVA_HOME."

cd "$PROJECT_ROOT"
"$JAVA_CMD" scripts/JanusWsLoad.java \
    --url "$URL" \
    --duration-seconds "$DURATION_SECONDS" \
    --parallelism "$PARALLELISM" \
    --pause-millis "$PAUSE_MILLIS"
