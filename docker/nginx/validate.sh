#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# Janus load-balancing / limiting validation helper  (ONE experiment)
#
# Topology (A = 2 fronts, B = 3 backends):
#   client ─(ws json)─▶ nginx :8080 (EDGE RR) ─▶ A1, A2 (front)
#                       A1, A2 ─(ws binary)─▶ nginx :8081 (B-HOP) ─▶ B1, B2, B3
#
# Bring the stack up first, then wait ~30s (a2 boots delayed):
#   docker compose -f docker/docker-compose.exp.yml --project-directory . up --build
#
# Each check prints:  expected: … / actual: … / result: PASS|FAIL|SKIP
# and a final SUMMARY. Exit code: 0 = all non-skipped checks PASS, 1 = any FAIL.
#
# Experiment items (numbering matches doc/load-balance.md):
#   LB-1    B-HOP least_conn          → backends ~equal
#   CONN-1  B-HOP limit_conn=100      → excess concurrent conn 503 (per-IP, fair)
#   RATE-1  B-HOP limit_req 50r/s     → excess new-conn rate 503 (per-IP, fair)
#   CONN-2  B-HOP max_conns (SHARED)  → later front (a2) starved
#           ⚠ only meaningful after ENABLING the max_conns lines in nginx.conf
#             and restarting the stack (off by default).
#   LB-2    multi-instance nginx (3×)  → is aggregate A→B balance preserved?
#           ⚠ requires `--profile lb2` (extra nginx + a dedicated front).
#   SCALE-1 backend scale up/down      → least_conn only balances at connect time,
#           so a bare scale-up feeds ~0 to the new backend until connections churn.
#           ⚠ requires `--profile scale` + the janus-backend-4 line enabled in
#             nginx.conf + `nginx -s reload`; also needs the `docker` CLI.
#   RESTART-1 backend single restart   → failure window + front auto-reconnect.
#           ⚠ needs the `docker` CLI (the check restarts a backend container).
#
# External tools (each check independent; missing tool → that check is SKIP):
#   websocat  → WebSocket calls & connection floods
#   curl      → metrics scraping & nginx status
#   docker    → SCALE-1 / RESTART-1 only (restart fronts / a backend mid-test)
#
# Usage:  bash docker/nginx/validate.sh <lb|conn|rate|starve|lb2|scale|restart|all>
#         (all = lb + conn + rate; run the opt-in items separately, see notes above)
# ════════════════════════════════════════════════════════════════════════════
set -uo pipefail

EDGE_WS="ws://localhost:18080/json"      # client → EDGE → fronts (LB-1 / CONN-2)
LB2_WS="ws://localhost:18082/json"       # client → LB-2 front (multi-nginx, --profile lb2)
HOP_WS="ws://localhost:18081/binary"     # B-HOP via nginx (CONN-1 / RATE-1 direct probe)
STATUS_URL="http://localhost:18090/status"
FRONT_METRICS=("http://localhost:19101" "http://localhost:19102")                       # A1, A2
BACKEND_METRICS=("http://localhost:19201" "http://localhost:19202" "http://localhost:19203")  # B1..B3
BACKEND4_METRIC="http://localhost:19204"     # B4 (SCALE-1, --profile scale)
# Compose invocation for SCALE-1 / RESTART-1 (restart containers mid-test).
DC="docker compose -f docker/docker-compose.exp.yml --project-directory ."

declare -A RESULT   # item → PASS|FAIL|SKIP
declare -A DETAIL   # item → one-line actual summary

hr() { printf '─%.0s' {1..76}; echo; }
have() { command -v "$1" >/dev/null 2>&1; }
counter_sum() { curl -s "$1/metrics" 2>/dev/null | grep -E "^$2" | awk '{s+=$NF} END{print s+0}'; }
record() { RESULT["$1"]="$2"; DETAIL["$1"]="${3:-}"; printf "  result:   %s\n" "$2"; }

# Drive N WS-JSON requests through the EDGE in batches (used by SCALE-1/RESTART-1).
drive_edge() {  # $1=count  $2=meta-tag  [$3=per-request timeout secs, default 3]
  local total="$1" meta="$2" to="${3:-3}" batch=30 done=0
  while [ "$done" -lt "$total" ]; do
    for _ in $(seq 1 "$batch"); do
      ( printf '{"data":"0","meta":"%s"}\n' "$meta" | timeout "$to" websocat -n "$EDGE_WS" >/dev/null 2>&1 ) &
    done
    wait; done=$((done + batch)); echo "    ...sent $done/$total"
  done
  sleep 1
}

# ── LB-1 · front→backend load balancing (nginx least_conn) ───────────────────
check_lb() {
  hr; echo "▶ [LB-1] front→backend load balancing (B-HOP least_conn)"
  echo "  expected: 3 backends each ≈ 1/3 of the 180 requests; none at 0; min/max ≥ 50%"
  if ! have websocat || ! have curl; then
    echo "  actual:   (need websocat + curl — not both present)"; record LB-1 SKIP; return
  fi
  local before=() after i v
  for m in "${BACKEND_METRICS[@]}"; do before+=("$(counter_sum "$m" ws_messages_total)"); done

  local total=180 batch=30 done=0
  echo "  driving $total WS-JSON requests through the edge (client → nginx → front → nginx → backend)..."
  while [ "$done" -lt "$total" ]; do
    for _ in $(seq 1 "$batch"); do
      ( printf '{"data":"0","meta":"lb1"}\n' | timeout 2 websocat -n "$EDGE_WS" >/dev/null 2>&1 ) &
    done
    wait; done=$((done + batch)); echo "    ...sent $done/$total"
  done
  sleep 1

  local min=2000000000 max=0 sum=0 line="" i=0
  for m in "${BACKEND_METRICS[@]}"; do
    after=$(counter_sum "$m" ws_messages_total)
    v=$(( after - ${before[$i]} ))
    line+=" b$((i+1))=$v"; sum=$((sum + v))
    (( v < min )) && min=$v
    (( v > max )) && max=$v
    i=$((i+1))
  done
  local ratio=0; [ "$max" -gt 0 ] && ratio=$(( min * 100 / max ))
  echo "  actual:  $line  (Δtotal=$sum, min/max=${ratio}%)"
  if [ "$max" -gt 0 ] && [ "$min" -gt 0 ] && [ "$ratio" -ge 50 ]; then
    record LB-1 PASS "$line min/max=${ratio}%"
  else
    echo "  hint:     a backend at ~0 → JANUS_WS_POOL_SIZE < backend count, or uneven least_conn mapping"
    record LB-1 FAIL "$line min/max=${ratio}%"
  fi
}

# ── CONN-1 · per-IP connection cap on the B-HOP (limit_conn=100) ─────────────
check_conn() {
  hr; echo "▶ [CONN-1] nginx WS connection cap on the B-HOP (limit_conn=100, per-IP)"
  echo "  expected: hold 150 concurrent conns → ~100 accepted, ~50 rejected(503); rejected ≥ 20"
  if ! have websocat; then echo "  actual:   (websocat missing)"; record CONN-1 SKIP; return; fi
  local tmp; tmp="/tmp/janus_conn.$$"
  echo "  opening 150 concurrent WS connections to the B-HOP (held ~3s each)..."
  { for _ in $(seq 1 150); do
      ( timeout 3 websocat -n "$HOP_WS" >/dev/null 2>&1 && echo ok || echo rej ) &
    done; wait; } > "$tmp" 2>&1
  local ok rej; ok=$(grep -c '^ok' "$tmp" 2>/dev/null || echo 0); rej=$(grep -c '^rej' "$tmp" 2>/dev/null || echo 0)
  rm -f "$tmp"
  echo "  actual:   accepted≈$ok  rejected≈$rej"
  echo "  nginx status:"; curl -s "$STATUS_URL" 2>/dev/null | sed 's/^/    /'
  if [ "$rej" -ge 20 ]; then record CONN-1 PASS "accepted≈$ok rejected≈$rej"
  else echo "  hint:     rejected too low — concurrency may not have exceeded the cap"; record CONN-1 FAIL "accepted≈$ok rejected≈$rej"; fi
}

# ── RATE-1 · per-IP new-connection rate cap on the B-HOP (limit_req 50r/s) ───
check_rate() {
  hr; echo "▶ [RATE-1] nginx WS new-connection rate cap on the B-HOP (limit_req=50r/s, burst=100)"
  echo "  expected: fire 300 rapid new conns → over rate+burst rejected(503); rejected ≥ 20"
  if ! have websocat; then echo "  actual:   (websocat missing)"; record RATE-1 SKIP; return; fi
  local tmp; tmp="/tmp/janus_rate.$$"
  echo "  firing 300 short-lived connections as fast as possible (open→close)..."
  { for _ in $(seq 1 300); do
      ( timeout 1 websocat "$HOP_WS" </dev/null >/dev/null 2>&1 && echo ok || echo rej ) &
    done; wait; } > "$tmp" 2>&1
  local ok rej; ok=$(grep -c '^ok' "$tmp" 2>/dev/null || echo 0); rej=$(grep -c '^rej' "$tmp" 2>/dev/null || echo 0)
  rm -f "$tmp"
  echo "  actual:   accepted≈$ok  rejected≈$rej"
  if [ "$rej" -ge 20 ]; then record RATE-1 PASS "accepted≈$ok rejected≈$rej"
  else echo "  hint:     rejected too low — burst may have absorbed the flood; raise the count"; record RATE-1 FAIL "accepted≈$ok rejected≈$rej"; fi
}

# ── CONN-2 · shared max_conns starves the later front ────────────────────────
# PASS = starvation reproduced: a clear share (≥25% of N) of client requests 503
# because a2 could not open a single downstream connection, while a1 still served
# a clear share. Requires the max_conns lines in nginx.conf to be ENABLED.
check_starve() {
  local N="${1:-120}"
  hr; echo "▶ [CONN-2] shared B connection cap → later front (a2) starved"
  echo "  setup:    enable max_conns=2 on the 3 backends in nginx.conf + restart;"
  echo "            B capacity = 6 slots, front pool = 6 → a1 (immediate) fills all,"
  echo "            a2 (delayed) gets none."
  echo "  expected: edge RR ≈ 50/50 → success(a1) ≈ starved-503(a2); backends fed"
  echo "            ONLY by a1 → b1≈b2≈b3 but Σ ≈ half of N (imbalance hidden)."
  if ! have websocat; then echo "  actual:   (websocat missing)"; record CONN-2 SKIP; return; fi
  local out; out="/tmp/janus_starve.$$"; : > "$out"
  local batch=20 done=0
  echo "  driving $N WS-JSON requests through the edge (RR across a1/a2)..."
  while [ "$done" -lt "$N" ]; do
    for _ in $(seq 1 "$batch"); do
      ( printf '{"data":"0","meta":"starve"}\n' | timeout 3 websocat -n "$EDGE_WS" 2>/dev/null ) >> "$out" &
    done
    wait; done=$((done + batch)); echo "    ...sent $done/$N"
  done
  # Success reply carries "mode":"RESPONSE"; a starved front replies with an
  # error envelope: "mode":"ERROR" / "status":503 / "downstream WS unavailable".
  local ok err empty
  ok=$(grep -c '"mode":"RESPONSE"' "$out" 2>/dev/null || echo 0)
  err=$(grep -c 'downstream WS unavailable' "$out" 2>/dev/null || echo 0)
  empty=$(( N - ok - err )); rm -f "$out"
  echo "  actual:   success(a1)=$ok  starved-503(a2)=$err  no-reply=$empty"

  if have curl; then
    local b_line="" i=0 v
    for m in "${BACKEND_METRICS[@]}"; do
      v=$(counter_sum "$m" ws_messages_total); b_line+=" b$((i+1))=$v"; i=$((i+1))
    done
    echo "  backends: ws_messages_total$b_line — b*≈ each other but underfed (only a1 feeds)"
  fi
  local thr=$(( N / 4 )); [ "$thr" -lt 1 ] && thr=1
  if [ "$err" -ge "$thr" ] && [ "$ok" -ge "$thr" ]; then
    record CONN-2 PASS "success=$ok starved-503=$err (thr≥$thr)"
  else
    echo "  hint:     no starvation → is max_conns enabled + stack restarted? is a2 delayed?"
    record CONN-2 FAIL "success=$ok starved-503=$err (thr≥$thr)"
  fi
}

# ── LB-2 · multi-instance nginx: does A→B stay balanced? (--profile lb2) ─────
# The LB-2 front fans its pool across 3 nginx, each running its OWN least_conn.
# With pool=12 (default) each nginx gets 4 conns → 2/1/1 → aggregate 6/3/3 SKEW.
# PASS here = the predicted SKEW was reproduced (min/max ≤ 65%), i.e. multiple
# uncoordinated nginx do NOT auto-balance. (pool=9 → 1/1/1 each → 3/3/3 balanced.)
check_lb2() {
  hr; echo "▶ [LB-2] multi-instance nginx front→backend balance (3 nginx, each own least_conn)"
  echo "  setup:    start with --profile lb2; LB-2 front pool=12 fanned across 3 nginx (4 each)"
  echo "  expected: 4 is not a multiple of 3 backends → each nginx 2/1/1 → aggregate ≈ 6:3:3"
  echo "            (min/max ≈ 50%). Uncoordinated nginx compound the same tie-break → SKEW."
  if ! have websocat || ! have curl; then
    echo "  actual:   (need websocat + curl — not both present)"; record LB-2 SKIP; return
  fi
  local before=() after i v
  for m in "${BACKEND_METRICS[@]}"; do before+=("$(counter_sum "$m" ws_messages_total)"); done
  local total=180 batch=30 done=0
  echo "  driving $total WS-JSON requests through the LB-2 front (client → front → 3×nginx → backend)..."
  while [ "$done" -lt "$total" ]; do
    for _ in $(seq 1 "$batch"); do
      ( printf '{"data":"0","meta":"lb2"}\n' | timeout 2 websocat -n "$LB2_WS" >/dev/null 2>&1 ) &
    done
    wait; done=$((done + batch)); echo "    ...sent $done/$total"
  done
  sleep 1
  local min=2000000000 max=0 sum=0 line="" i=0
  for m in "${BACKEND_METRICS[@]}"; do
    after=$(counter_sum "$m" ws_messages_total)
    v=$(( after - ${before[$i]} ))
    line+=" b$((i+1))=$v"; sum=$((sum + v))
    (( v < min )) && min=$v
    (( v > max )) && max=$v
    i=$((i+1))
  done
  local ratio=0; [ "$max" -gt 0 ] && ratio=$(( min * 100 / max ))
  echo "  actual:  $line  (Δtotal=$sum, min/max=${ratio}%)"
  if [ "$sum" -eq 0 ]; then
    echo "  hint:     no backend traffic — is --profile lb2 up and port 18082 reachable?"; record LB-2 FAIL "no traffic"
  elif [ "$ratio" -le 65 ]; then
    record LB-2 PASS "skew reproduced $line min/max=${ratio}% (≤65%)"
  else
    echo "  note:     distribution looks balanced — pool may be a multiple of backends (e.g. 9), or nginx not multi-worker-isolated"
    record LB-2 FAIL "balanced $line min/max=${ratio}% (expected skew ≤65%)"
  fi
}

# ── RESTART-1 · single backend restart → failure window + auto-reconnect ─────
# The fronts talk only to the STATIC B-HOP (nginx), never to backends directly.
# Restarting a backend drops the front↔nginx↔backend connections pinned to it;
# the front pool maintainer (~3s) re-opens them through nginx onto healthy
# backends, and nginx's max_fails/fail_timeout ejects the down node meanwhile.
# PASS = a clean post-restart batch recovers to ≈100% success (fronts re-pooled).
check_restart() {
  local N="${1:-120}"
  hr; echo "▶ [RESTART-1] single backend restart → failure window + front auto-reconnect"
  echo "  setup:    default 3-backend stack; this check restarts janus-backend-2 mid-traffic."
  echo "  expected: brief failures while conns to b2 drop; fronts re-pool via nginx"
  echo "            (maintainer ~3s) → a clean post-restart batch ≈ 100% success."
  if ! have websocat; then echo "  actual:   (websocat missing)"; record RESTART-1 SKIP; return; fi
  if ! have docker;   then echo "  actual:   (docker CLI missing — cannot restart a backend)"; record RESTART-1 SKIP; return; fi

  local out; out="/tmp/janus_restart.$$"; : > "$out"
  echo "  restarting janus-backend-2 while driving $N WS-JSON requests through the edge..."
  ( sleep 1; $DC restart janus-backend-2 >/dev/null 2>&1; echo "    ...janus-backend-2 restarted" ) &
  local rpid=$!
  local batch=20 done=0
  while [ "$done" -lt "$N" ]; do
    for _ in $(seq 1 "$batch"); do
      ( printf '{"data":"0","meta":"restart"}\n' | timeout 5 websocat -n "$EDGE_WS" 2>/dev/null ) >> "$out" &
    done
    wait; done=$((done + batch)); echo "    ...sent $done/$N"
  done
  wait "$rpid" 2>/dev/null
  local ok err; ok=$(grep -c '"mode":"RESPONSE"' "$out" 2>/dev/null); ok=${ok:-0}
  err=$(( N - ok )); rm -f "$out"
  echo "  during-restart: success=$ok  non-success≈$err (some failures expected in the window)"

  echo "  waiting ~12s for fronts to re-pool through nginx, then a clean recovery batch..."
  sleep 12
  local rout; rout="/tmp/janus_restart_rec.$$"; : > "$rout"
  local M=60 rdone=0
  while [ "$rdone" -lt "$M" ]; do
    for _ in $(seq 1 20); do
      ( printf '{"data":"0","meta":"recover"}\n' | timeout 5 websocat -n "$EDGE_WS" 2>/dev/null ) >> "$rout" &
    done
    wait; rdone=$((rdone + 20))
  done
  local rok; rok=$(grep -c '"mode":"RESPONSE"' "$rout" 2>/dev/null); rok=${rok:-0}; rm -f "$rout"
  local rratio=0; [ "$M" -gt 0 ] && rratio=$(( rok * 100 / M ))
  echo "  actual:   recovery batch success $rok/$M (${rratio}%)"
  if [ "$rratio" -ge 95 ]; then
    record RESTART-1 PASS "recovery ${rok}/${M}=${rratio}% (during: ok=$ok non-ok≈$err)"
  else
    echo "  hint:     low recovery → increase settle time, or check backend-2 is back (docker ps)"
    record RESTART-1 FAIL "recovery ${rok}/${M}=${rratio}%"
  fi
}

# ── SCALE-1 · backend scale-up → sticky connections; churn to rebalance ──────
# least_conn only balances at CONNECT time. After scaling B from 3→4 (start
# backend-4 + enable its upstream line + reload), the fronts' long-lived
# multiplexed connections stay on b1..b3, so b4 gets ~0 traffic (phase A). Only
# after the connections churn (restart the fronts) do the pools re-open evenly
# across all 4 backends (phase B). PASS = both halves reproduced.
check_scale() {
  hr; echo "▶ [SCALE-1] backend scale-up → sticky connections, churn to rebalance"
  echo "  setup:    up with --profile scale (adds janus-backend-4); uncomment the"
  echo "            janus-backend-4 line in nginx.conf; then: $DC exec nginx nginx -s reload"
  echo "  expected: phase A (just reloaded) → b4 ≈ 0 (conns still pinned to b1..b3);"
  echo "            phase B (after churning the fronts) → all 4 backends ≈ 1/4, b4 > 0."
  if ! have websocat || ! have curl; then echo "  actual:   (need websocat + curl)"; record SCALE-1 SKIP; return; fi
  if ! have docker; then echo "  actual:   (docker CLI missing — cannot churn fronts)"; record SCALE-1 SKIP; return; fi
  if ! curl -s "$BACKEND4_METRIC/metrics" >/dev/null 2>&1; then
    echo "  actual:   backend-4 metrics unreachable — did you start with --profile scale?"; record SCALE-1 SKIP; return
  fi

  local ALL=("${BACKEND_METRICS[@]}" "$BACKEND4_METRIC") m v i
  # phase A — bare scale-up: connections still pinned to b1..b3
  local a_before=() i=0
  for m in "${ALL[@]}"; do a_before+=("$(counter_sum "$m" ws_messages_total)"); done
  echo "  phase A: driving 120 requests right after the reload (no churn yet)..."
  drive_edge 120 "scaleA"
  local a_line="" a_b4=0 i=0
  for m in "${ALL[@]}"; do
    v=$(( $(counter_sum "$m" ws_messages_total) - ${a_before[$i]} ))
    a_line+=" b$((i+1))=$v"; [ "$i" -eq 3 ] && a_b4=$v; i=$((i+1))
  done
  echo "  phase A: $a_line  → b4 should be ≈ 0 (least_conn balances only at connect time)"

  # phase B — churn the fronts so their pools re-open across all 4 backends
  echo "  churning fronts (restart a1/a2) so their pools re-open across all 4 backends..."
  $DC restart janus-front-a1 janus-front-a2 >/dev/null 2>&1
  echo "  waiting ~32s for fronts to boot (a2 delayed) + re-pool..."
  sleep 32
  local b_before=() i=0
  for m in "${ALL[@]}"; do b_before+=("$(counter_sum "$m" ws_messages_total)"); done
  echo "  phase B: driving 240 requests after the churn..."
  drive_edge 240 "scaleB"
  local b_line="" min=2000000000 max=0 sum=0 i=0
  for m in "${ALL[@]}"; do
    v=$(( $(counter_sum "$m" ws_messages_total) - ${b_before[$i]} ))
    b_line+=" b$((i+1))=$v"; sum=$((sum + v))
    (( v < min )) && min=$v
    (( v > max )) && max=$v
    i=$((i+1))
  done
  local ratio=0; [ "$max" -gt 0 ] && ratio=$(( min * 100 / max ))
  echo "  phase B: $b_line  (Δtotal=$sum, min/max=${ratio}%)"
  echo "  actual:  phaseA b4=$a_b4 (want ≈0) ; phaseB 4-way min/max=${ratio}% (want ≥50%)"
  if [ "$a_b4" -le 10 ] && [ "$min" -gt 0 ] && [ "$ratio" -ge 50 ]; then
    record SCALE-1 PASS "phaseA b4=$a_b4≈0; phaseB$b_line min/max=${ratio}%"
  else
    echo "  hint:     b4>0 in phase A → conns already churned; phase B skew → wait longer or check reload"
    record SCALE-1 FAIL "phaseA b4=$a_b4; phaseB min/max=${ratio}%"
  fi
}

summary() {
  hr; echo "SUMMARY"
  local order=("$@") p fails=0 ran=0
  for p in "${order[@]}"; do
    local r="${RESULT[$p]:-SKIP}"
    printf "  %-8s %-4s  %s\n" "$p" "$r" "${DETAIL[$p]:-}"
    [ "$r" = "FAIL" ] && fails=$((fails+1))
    [ "$r" != "SKIP" ] && ran=$((ran+1))
  done
  hr
  if [ "$ran" -eq 0 ]; then echo "结果：全部 SKIP（缺少 websocat/curl，无法验证）"; return 2; fi
  if [ "$fails" -eq 0 ]; then echo "结果：通过 ✅（$ran 项已验证，0 失败）"; return 0
  else echo "结果：未通过 ❌（$fails 项失败）"; return 1; fi
}

case "${1:-all}" in
  lb)     check_lb;   summary LB-1 ;;
  conn)   check_conn; summary CONN-1 ;;
  rate)   check_rate; summary RATE-1 ;;
  starve) check_starve "${2:-120}"; summary CONN-2 ;;
  lb2)    check_lb2;  summary LB-2 ;;
  scale)  check_scale;   summary SCALE-1 ;;
  restart) check_restart "${2:-120}"; summary RESTART-1 ;;
  all)    check_lb; check_conn; check_rate; summary LB-1 CONN-1 RATE-1 ;;
  *)      echo "usage: $0 [lb|conn|rate|starve|lb2|scale|restart|all]"; exit 1 ;;
esac
