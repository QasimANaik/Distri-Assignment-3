#!/bin/bash
# ============================================================
# Q3 correctness verification (run locally, no SLURM needed)
#
#   ./verify.sh
#
# For the graphs in test_data/ plus edge cases with known answers, checks:
#   1. the sequential counter gives the known answer (validates the reference),
#   2. Q3ForLocalTesting.sh matches the sequential counter,
#   3. Q3_distributed.sh matches it with P = 1, 2, 4 and 8 tasks
#      (srun is emulated with local processes when SLURM is not available).
# ============================================================
export LC_ALL=C
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

g++ -O2 -std=c++17 -o sequential sequential.cpp || exit 1
g++ -O2 -std=c++17 -o q3 Q3.cpp || exit 1

# ---- Edge cases (name, contents, known triangle count) ----
EDGE_DIR="$TMP/edge_cases"
mkdir -p "$EDGE_DIR"
declare -A KNOWN=(
    [q3_sample.txt]=2 [q3_small.txt]=9312 [q3_medium.txt]=82782 [q3_large.txt]=784965
)

# a single triangle: fewer edges than tasks, so some mappers get no input
printf '3 3\n0 1\n1 2\n2 0\n' > "$EDGE_DIR/one_triangle.txt"
KNOWN[one_triangle.txt]=1

# a path: no triangles at all
{ echo "4 3"; echo "0 1"; echo "1 2"; echo "2 3"; } > "$EDGE_DIR/path.txt"
KNOWN[path.txt]=0

# star with 999 leaves (one hub of degree 999): no triangles
{ echo "1000 999"; for ((i = 1; i < 1000; i++)); do echo "0 $i"; done; } > "$EDGE_DIR/star.txt"
KNOWN[star.txt]=0

# complete bipartite K(30,30): 900 edges, no triangles
{ echo "60 900"; for ((i = 0; i < 30; i++)); do for ((j = 30; j < 60; j++)); do echo "$i $j"; done; done; } > "$EDGE_DIR/bipartite.txt"
KNOWN[bipartite.txt]=0

# complete graph K60: C(60,3) = 34220 triangles, every vertex has the same degree
{ echo "60 1770"; for ((i = 0; i < 60; i++)); do for ((j = i + 1; j < 60; j++)); do echo "$i $j"; done; done; } > "$EDGE_DIR/complete.txt"
KNOWN[complete.txt]=34220

# the sample with duplicate edges (both directions) and self-loops: still 2
printf '4 10\n0 1\n1 2\n2 0\n2 3\n3 0\n1 0\n0 2\n2 2\n3 3\n3 0\n' > "$EDGE_DIR/duplicates.txt"
KNOWN[duplicates.txt]=2

GRAPHS=("$SCRIPT_DIR"/test_data/q3_*.txt "$EDGE_DIR"/*.txt)

# ---- Run everything ----
declare -A SEQ LOCAL
for g in "${GRAPHS[@]}"; do
    name=$(basename "$g")
    SEQ[$name]=$(./sequential < "$g")
    ./Q3ForLocalTesting.sh "$g" "$TMP/local_out" > /dev/null
    LOCAL[$name]=$(cat "$TMP/local_out")
done

declare -A DIST
for P in 1 2 4 8; do
    echo "Running Q3_distributed.sh with P = $P ..."
    SLURM_NTASKS=$P REPEATS=1 RESULTS_DIR="$TMP/results" \
        ./Q3_distributed.sh "${GRAPHS[@]}" > "$TMP/dist_P$P.log" 2>&1
    while IFS=, read -r name triangles; do
        DIST[$name,$P]=$triangles
    done < <(awk -F, 'NR > 1 { print $1 "," $21 }' "$TMP/results/q3_summary_P$P.csv")
done

# ---- Report ----
FAILED=0
echo ""
printf '%-16s %8s %11s %8s %8s %8s %8s %8s  %s\n' \
    graph known sequential local P=1 P=2 P=4 P=8 result
for g in "${GRAPHS[@]}"; do
    name=$(basename "$g")
    expected=${KNOWN[$name]}
    ok=yes
    [ "${SEQ[$name]}" = "$expected" ] || ok=no
    [ "${LOCAL[$name]}" = "${SEQ[$name]}" ] || ok=no
    for P in 1 2 4 8; do
        [ "${DIST[$name,$P]}" = "${SEQ[$name]}" ] || ok=no
    done
    [ "$ok" = yes ] || FAILED=1
    printf '%-16s %8s %11s %8s %8s %8s %8s %8s  %s\n' "$name" "$expected" "${SEQ[$name]}" \
        "${LOCAL[$name]}" "${DIST[$name,1]}" "${DIST[$name,2]}" "${DIST[$name,4]}" \
        "${DIST[$name,8]}" "$([ "$ok" = yes ] && echo PASS || echo FAIL)"
done
echo ""
if [ "$FAILED" -eq 0 ]; then echo "All graphs: PASS"; else echo "Some graphs FAILED"; fi
exit $FAILED
