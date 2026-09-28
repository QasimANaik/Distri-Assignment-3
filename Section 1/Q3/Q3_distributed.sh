#!/bin/bash
#SBATCH --job-name=q3_triangles_dist
#SBATCH --output=q3_dist_results_%j.out
#SBATCH --error=q3_dist_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:15:00

# ============================================================
# Q3 Distributed MapReduce Triangle Counting — SLURM Script
# Runs 4 chained MapReduce jobs (Degree, Wedge, Close, Total) and
# profiles the mapper, shuffle/sort, combiner and reducer stages
# of each job for the graphs in test_data/
# ============================================================

export LC_ALL=C   # byte-wise sort, same key order as Hadoop

# Use SLURM_SUBMIT_DIR if running under SLURM, otherwise fallback to script directory
if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi
cd "$SCRIPT_DIR"

RESULTS_DIR="$SCRIPT_DIR/perf_results"
mkdir -p "$RESULTS_DIR"

TEST_DATA_DIR="$SCRIPT_DIR/test_data"
SUMMARY_FILE="$RESULTS_DIR/q3_dist_benchmark_summary.csv"

# CSV header (one timing column per MapReduce job)
echo "input_file,input_size_bytes,num_edges,num_tasks,degree_time_s,wedge_time_s,close_time_s,total_job_time_s,total_time_s,triangles" > "$SUMMARY_FILE"

TEST_FILES=(
    "q3_sample.txt"
    "q3_small.txt"
    "q3_medium.txt"
    "q3_large.txt"
)

if [ -z "$SLURM_NTASKS" ]; then
    SLURM_NTASKS=4 # Fallback for local testing
fi

# Build the mapper/combiner/reducer executable once, on the shared
# filesystem, so every node runs the same binary
export Q3="$SCRIPT_DIR/q3"
if [ ! -x "$Q3" ] || [ "$SCRIPT_DIR/Q3.cpp" -nt "$Q3" ]; then
    g++ -O2 -std=c++17 -o "$Q3" "$SCRIPT_DIR/Q3.cpp" || exit 1
fi

# ------------------------------------------------------------
# run_dist_job NAME MAPPER COMBINER REDUCER INPUTS
#   One MapReduce job with SLURM_NTASKS mappers and SLURM_NTASKS
#   reducers. Mapper TID reads INPUTS with "TID" replaced by its
#   task id (00, 01, ...). An empty COMBINER skips Shuffle 1 and
#   the combiner. Reducer TID writes NAME/red_TID.out.
#   Sets JOB_TIME.
# ------------------------------------------------------------
run_dist_job() {
    export JOB=$1 MAPPER=$2 COMBINER=$3 REDUCER=$4 INPUTS=$5
    mkdir -p "$JOB"
    echo "  [$JOB]"
    JOB_START=$(date +%s%N)

    # Stage 1: Mapper (Distributed)
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        cat ${INPUTS//TID/$TID} | "$Q3" $MAPPER > "$JOB/map_${TID}.out"
    '
    STAGE_END=$(date +%s%N)
    MAPPER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "    Mapper (Dist):    ${MAPPER_TIME}s"

    if [ -n "$COMBINER" ]; then
        # Stage 2: Shuffle/Sort 1 (Distributed Local Sort)
        STAGE_START=$(date +%s%N)
        srun --ntasks=$SLURM_NTASKS bash -c '
            TID=$(printf "%02d" $SLURM_PROCID)
            sort "$JOB/map_${TID}.out" > "$JOB/shuf1_${TID}.out"
        '
        STAGE_END=$(date +%s%N)
        SHUFFLE1_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
        echo "    Shuffle 1:        ${SHUFFLE1_TIME}s"

        # Stage 3: Combiner (Distributed)
        STAGE_START=$(date +%s%N)
        srun --ntasks=$SLURM_NTASKS bash -c '
            TID=$(printf "%02d" $SLURM_PROCID)
            "$Q3" $COMBINER < "$JOB/shuf1_${TID}.out" > "$JOB/comb_${TID}.out"
        '
        STAGE_END=$(date +%s%N)
        COMBINER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
        echo "    Combiner (Dist):  ${COMBINER_TIME}s"
        export MAP_OUT=comb
    else
        echo "    Shuffle 1:        - (no combiner)"
        echo "    Combiner (Dist):  -"
        export MAP_OUT=map
    fi

    # Stage 4: Shuffle/Sort 2 (Distributed Hash Partition + Gather + Sort)
    # Every mapper sends key k to reducer hash(k) % SLURM_NTASKS; then
    # each reducer gathers its bucket from all mappers and sorts it.
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        "$Q3" partition $SLURM_NTASKS "$JOB/part_${TID}_" < "$JOB/${MAP_OUT}_${TID}.out"
    '
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        sort "$JOB"/part_*_${TID} > "$JOB/shuf2_${TID}.out"
    '
    STAGE_END=$(date +%s%N)
    SHUFFLE2_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "    Shuffle 2 (Part): ${SHUFFLE2_TIME}s"

    # Stage 5: Reducer (Distributed)
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        "$Q3" $REDUCER < "$JOB/shuf2_${TID}.out" > "$JOB/red_${TID}.out"
    '
    STAGE_END=$(date +%s%N)
    REDUCER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "    Reducer (Dist):   ${REDUCER_TIME}s"

    JOB_END=$(date +%s%N)
    JOB_TIME=$(echo "scale=6; ($JOB_END - $JOB_START) / 1000000000" | bc)
    echo "    Job time:         ${JOB_TIME}s"
}

echo "============================================"
echo "Distributed MapReduce Triangle Counting Benchmark"
echo "Date: $(date)"
echo "Nodes Allocated: $SLURM_JOB_NODELIST"
echo "Number of Tasks: $SLURM_NTASKS"
echo "============================================"
echo ""

for TEST_FILE in "${TEST_FILES[@]}"; do
    INPUT_PATH="$TEST_DATA_DIR/$TEST_FILE"

    if [ ! -f "$INPUT_PATH" ]; then
        echo "WARNING: $INPUT_PATH not found, skipping."
        continue
    fi

    INPUT_SIZE=$(stat --format=%s "$INPUT_PATH" 2>/dev/null || stat -f%z "$INPUT_PATH" 2>/dev/null)
    NUM_EDGES=$(( $(wc -l < "$INPUT_PATH") - 1 ))
    INPUT_SIZE_MB=$(echo "scale=2; $INPUT_SIZE / 1048576" | bc)

    echo "--------------------------------------------"
    echo "Input: $TEST_FILE ($INPUT_SIZE_MB MB, $NUM_EDGES edges)"
    echo "--------------------------------------------"

    OUTPUT_FILE="$RESULTS_DIR/dist_output_${TEST_FILE}"

    # All intermediate files live in a work directory on the shared
    # filesystem, so every node can read what the others wrote
    WORK_DIR="$SCRIPT_DIR/q3_work_${SLURM_JOB_ID:-local}"
    rm -rf "$WORK_DIR"
    mkdir -p "$WORK_DIR"
    cd "$WORK_DIR"

    # ---- Total pipeline timing ----
    TOTAL_START=$(date +%s%N)

    # Setup: drop the "V E" header and split the edge list across tasks
    tail -n +2 "$INPUT_PATH" > edges
    split -d -a 2 -n l/$SLURM_NTASKS edges chunk_

    # Job 1: Degree        edge (u,v) -> (u,1), (v,1)  => (v, deg(v))
    run_dist_job degree "degree_map" "sum" "sum" "chunk_TID"
    DEGREE_TIME=$JOB_TIME

    # Distributed cache: every mapper of job 2 gets the full degree table
    cat degree/red_*.out > degrees

    # Job 2: Orient+Wedge  edge -> (lo, hi) by rank (deg, id)  => ("a,b", 1) wedges
    run_dist_job wedge "orient_map degrees" "" "wedge_reduce" "chunk_TID"
    WEDGE_TIME=$JOB_TIME

    # Job 3: Close         edges + wedges -> triangles closed by an edge (one partial count per reducer)
    run_dist_job close "join_map" "join_combine" "join_reduce" "chunk_TID wedge/red_TID.out"
    CLOSE_TIME=$JOB_TIME

    # Job 4: Total (Single Node: only SLURM_NTASKS partial counts to add up)
    echo "  [total]"
    STAGE_START=$(date +%s%N)
    cat close/red_*.out | \
        "$Q3" identity | \
        sort | \
        "$Q3" sum | \
        sort | \
        "$Q3" final_reduce > "$OUTPUT_FILE"
    STAGE_END=$(date +%s%N)
    TOTAL_JOB_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "    Job time:         ${TOTAL_JOB_TIME}s"

    TOTAL_END=$(date +%s%N)
    TOTAL_TIME=$(echo "scale=6; ($TOTAL_END - $TOTAL_START) / 1000000000" | bc)
    TRIANGLES=$(cat "$OUTPUT_FILE")
    echo "  ─────────────────────"
    echo "  TOTAL:            ${TOTAL_TIME}s"
    echo "  Triangles:        ${TRIANGLES}"
    echo ""

    # Record to CSV
    echo "${TEST_FILE},${INPUT_SIZE},${NUM_EDGES},${SLURM_NTASKS},${DEGREE_TIME},${WEDGE_TIME},${CLOSE_TIME},${TOTAL_JOB_TIME},${TOTAL_TIME},${TRIANGLES}" >> "$SUMMARY_FILE"

    # Cleanup temp files for this iteration
    cd "$SCRIPT_DIR"
    rm -rf "$WORK_DIR"
done

echo ""
echo "============================================"
echo "Distributed Benchmark complete!"
echo "Summary CSV: $SUMMARY_FILE"
echo "============================================"
echo ""
echo "--- CSV Summary ---"
column -t -s',' "$SUMMARY_FILE"
