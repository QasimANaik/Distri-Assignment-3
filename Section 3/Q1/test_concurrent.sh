#!/bin/bash
# ============================================================
# Concurrency test for the Document Server
#
#   ./test_concurrent.sh [edits_per_client] [server_address]
#
# With no server_address, a server is started on localhost for the test.
# On RCE, pass the address of a server already running on another node,
# e.g. ./test_concurrent.sh 200 node01:50051
# Set PYTHON to use a different interpreter (default: python3).
#
# Part 1 (Step 5 of the question): two clients insert "Distributed " and
#   "Systems " at position 6 of "Hello World" at the same moment.
# Part 2 (stress): the same two clients each send N edits as fast as they
#   can, while a third client is subscribed. Checks that
#   - every edit is applied exactly once and no text is corrupted,
#   - the subscriber received all 2N updates, in order.
# ============================================================
N=${1:-200}
ADDRESS=$2
PYTHON=${PYTHON:-python3}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP=$(mktemp -d)

cleanup() {
    touch "$TMP/done"
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null
    rm -rf "$TMP"
}
trap cleanup EXIT

if [ -z "$ADDRESS" ]; then
    ADDRESS="localhost:$((50100 + RANDOM % 500))"
    "$PYTHON" "$SCRIPT_DIR/server.py" "$ADDRESS" > "$TMP/server.log" 2>&1 &
    SERVER_PID=$!
    until grep -q "listening" "$TMP/server.log" 2>/dev/null; do
        kill -0 "$SERVER_PID" 2>/dev/null || { cat "$TMP/server.log"; exit 1; }
        sleep 0.1
    done
fi

# send COMMANDS... to a new client named $1
client() {
    local name=$1; shift
    printf '%s\n' "$@" | "$PYTHON" "$SCRIPT_DIR/client.py" "$ADDRESS" "$name"
}

# content of document $1 as seen by a fresh client
content_of() {
    client reader "open $1" | sed -n 's/^\[Client\] //p' | tail -n +2 | head -1
}

FAILED=0
check() {   # check DESCRIPTION CONDITION...
    local what=$1; shift
    if "$@"; then echo "  PASS  $what"; else echo "  FAIL  $what"; FAILED=1; fi
}

echo "Server: $ADDRESS"
echo ""
echo "=== Part 1: two clients edit at position 6 at the same time ==="
DOC1="step5_$$.txt"
client setup "create $DOC1 \"Hello World\"" > /dev/null
client client1 "edit $DOC1 6 \"Distributed \"" | grep 'Edit applied' &
P1=$!
client client2 "edit $DOC1 6 \"Systems \"" | grep 'Edit applied' &
P2=$!
wait $P1 $P2
FINAL=$(content_of "$DOC1")
echo "  Final document: $FINAL"
check "both edits applied, nothing corrupted" \
    test "$FINAL" = "Hello Distributed Systems World" -o "$FINAL" = "Hello Systems Distributed World"

echo ""
echo "=== Part 2: $N edits from each of 2 clients, 1 subscriber ==="
DOC2="stress_$$.txt"
client setup "create $DOC2 \"Hello World\"" > /dev/null

# Subscriber stays connected until the test is done
(echo "subscribe $DOC2"; while [ ! -f "$TMP/done" ]; do sleep 0.1; done) |
    "$PYTHON" "$SCRIPT_DIR/client.py" "$ADDRESS" watcher > "$TMP/watcher.log" 2>&1 &
until grep -q "Subscribed to updates" "$TMP/watcher.log" 2>/dev/null; do sleep 0.1; done

A=(); B=()
for ((i = 0; i < N; i++)); do
    A+=("edit $DOC2 6 \"Distributed \""); B+=("edit $DOC2 6 \"Systems \"")
done
START=$(date +%s%N)
client client1 "${A[@]}" > "$TMP/c1.log" &
P1=$!
client client2 "${B[@]}" > "$TMP/c2.log" &
P2=$!
wait $P1 $P2
ELAPSED_MS=$(( ($(date +%s%N) - START) / 1000000 ))
echo "  $((2 * N)) edits in ${ELAPSED_MS} ms (including client start-up)"

# let the last updates reach the subscriber
EXPECTED=$((2 * N))
for _ in $(seq 50); do
    [ "$(grep -c '^\[Update\]' "$TMP/watcher.log")" -ge "$EXPECTED" ] && break
    sleep 0.1
done

FINAL=$(content_of "$DOC2")
LEN_EXPECTED=$((11 + N * 12 + N * 8))
# every edit makes the document longer, so updates delivered in order have
# strictly increasing lengths; prints "<in order?> <count> <last length>"
UPDATES=$(awk '/^\[Update\]/ { getline; print length($0) }' "$TMP/watcher.log" |
          awk 'NR > 1 && $1 <= prev { bad = 1 } { prev = $1; n++ }
               END { print (bad ? "unordered" : "ordered"), n + 0, prev + 0 }')

check "all $EXPECTED edits acknowledged" \
    test "$(cat "$TMP/c1.log" "$TMP/c2.log" | grep -c 'Edit applied')" -eq "$EXPECTED"
check "length is 11 + ${N}x12 + ${N}x8 = $LEN_EXPECTED" test "${#FINAL}" -eq "$LEN_EXPECTED"
check "$N x \"Distributed \" and $N x \"Systems \"" \
    test "$(grep -o 'Distributed ' <<< "$FINAL" | wc -l)" -eq "$N" \
      -a "$(grep -o 'Systems ' <<< "$FINAL" | wc -l)" -eq "$N"
check "removing the inserted words gives \"Hello World\"" \
    test "$(sed 's/Distributed //g; s/Systems //g' <<< "$FINAL")" = "Hello World"
check "subscriber got all $EXPECTED updates, in order, ending with the final document" \
    test "$UPDATES" = "ordered $EXPECTED $LEN_EXPECTED"

echo ""
if [ "$FAILED" -eq 0 ]; then echo "All checks passed."; else echo "Some checks FAILED."; fi
exit $FAILED
