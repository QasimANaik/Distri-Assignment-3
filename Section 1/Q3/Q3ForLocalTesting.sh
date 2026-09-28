#!/bin/bash

INPUT_FILE=${1:-test_data/q3_sample.txt}
OUTPUT_FILE=${2:-output.txt}

export LC_ALL=C   # byte-wise sort, same key order as Hadoop

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
Q3="$SCRIPT_DIR/q3"

# Build the mapper/combiner/reducer executable if needed
if [ ! -x "$Q3" ] || [ "$SCRIPT_DIR/Q3.cpp" -nt "$Q3" ]; then
    g++ -O2 -std=c++17 -o "$Q3" "$SCRIPT_DIR/Q3.cpp" || exit 1
fi

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

# Each job: Mapper -> Sort (shuffle) -> Combiner -> Sort again -> Reducer
# The first line of the input ("V E") is not an edge, so it is dropped.

# Job 1: Degree        edge (u,v) -> (u,1), (v,1)  => (v, deg(v))
tail -n +2 "$INPUT_FILE" | \
    "$Q3" degree_map | \
    sort | \
    "$Q3" sum | \
    sort | \
    "$Q3" sum > "$TMP_DIR/degrees"

# Job 2: Orient+Wedge  edge -> (lo, hi) by rank (deg, id)  => ("a,b", 1) wedges
tail -n +2 "$INPUT_FILE" | \
    "$Q3" orient_map "$TMP_DIR/degrees" | \
    sort | \
    "$Q3" wedge_reduce > "$TMP_DIR/wedges"

# Job 3: Close         edges + wedges -> triangles closed by an edge
(tail -n +2 "$INPUT_FILE"; cat "$TMP_DIR/wedges") | \
    "$Q3" join_map | \
    sort | \
    "$Q3" join_combine | \
    sort | \
    "$Q3" join_reduce > "$TMP_DIR/partial"

# Job 4: Total         partial counts -> single integer
"$Q3" identity < "$TMP_DIR/partial" | \
    sort | \
    "$Q3" sum | \
    sort | \
    "$Q3" final_reduce > "$OUTPUT_FILE"

echo "MapReduce pipeline completed. Output saved to $OUTPUT_FILE"
