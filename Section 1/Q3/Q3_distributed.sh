#!/bin/bash
#SBATCH --job-name=q3_triangles_dist
#SBATCH --output=q3_dist_results_%j.out
#SBATCH --error=q3_dist_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00

# ============================================================
# Q3 Distributed MapReduce Triangle Counting — SLURM Script
# Runs 4 chained MapReduce jobs (Degree, Wedge, Close, Total) and
# profiles the mapper, shuffle/sort, combiner and reducer stages
# of each job. P = number of SLURM tasks = mappers = reducers.
#
#   sbatch Q3_distributed.sh                          # P = 4, graphs in test_data/
#   sbatch --nodes=2 --ntasks=2 Q3_distributed.sh     # any other P
#   ./submit_all.sh                                   # P = 1, 2, 4, 8
#   SLURM_NTASKS=8 ./Q3_distributed.sh [graphs...]    # locally, without SLURM
#
# Environment: REPEATS (runs per graph, default 3),
#              RESULTS_DIR (default perf_results/)
#
# Every run is checked against the sequential counter (sequential.cpp).
# Results: $RESULTS_DIR/q3_summary_P<P>.csv, one row per graph per run.
# ============================================================

export LC_ALL=C   # byte-wise sort, same key order as Hadoop

# Use SLURM_SUBMIT_DIR if running under SLURM, otherwise fallback to script directory
if [ -n "$SLURM_SUBMIT_DIR" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi

# Graphs to run: the arguments, or the default test set. Absolute paths,
# because the pipeline runs inside a work directory.
if [ $# -gt 0 ]; then
    TEST_PATHS=("$@")
else
    TEST_PATHS=(
        "$SCRIPT_DIR/test_data/q3_sample.txt"
        "$SCRIPT_DIR/test_data/q3_small.txt"
        "$SCRIPT_DIR/test_data/q3_medium.txt"
        "$SCRIPT_DIR/test_data/q3_large.txt"
    )
fi
for i in "${!TEST_PATHS[@]}"; do
    TEST_PATHS[$i]=$(realpath -m "${TEST_PATHS[$i]}")
done
cd "$SCRIPT_DIR"

if [ -z "$SLURM_NTASKS" ]; then
    SLURM_NTASKS=4 # Fallback for local testing
fi
REPEATS=${REPEATS:-3}

RESULTS_DIR=${RESULTS_DIR:-$SCRIPT_DIR/perf_results}
mkdir -p "$RESULTS_DIR"
SUMMARY_FILE="$RESULTS_DIR/q3_summary_P${SLURM_NTASKS}.csv"

# CSV header: one row per graph per run.
#   *_time_s    : wall time of each MapReduce job, and of the whole pipeline
#   map_s .. reduce_s : stage times summed over jobs 1-3 (computation:
#                 map, combine, reduce; local sort; shuffle = partition +
#                 transfer through the shared filesystem + merge sort)
#   num_sruns, srun_overhead_s : srun launches per run, and the cost of one
#                 empty srun (start-up overhead included in every stage)
#   shuffle_bytes : intermediate data sent from mappers to reducers
echo "input_file,input_size_bytes,num_edges,num_tasks,num_nodes,run,total_time_s,degree_time_s,wedge_time_s,close_time_s,total_job_time_s,map_s,sort_s,combine_s,shuffle_s,reduce_s,num_sruns,srun_overhead_s,shuffle_bytes,seq_time_s,triangles,seq_triangles,correct" > "$SUMMARY_FILE"

# Without SLURM (local testing), emulate "srun --ntasks=N cmd...": run N
# copies of cmd in parallel, each with its own SLURM_PROCID
if ! command -v srun >/dev/null 2>&1; then
    srun() {
        local n=${1#--ntasks=} i rc=0 pids=()
        shift
        for ((i = 0; i < n; i++)); do
            SLURM_PROCID=$i SLURM_NTASKS=$n "$@" &
            pids+=($!)
        done
        for i in "${pids[@]}"; do wait "$i" || rc=1; done
        return $rc
    }
fi

# Build the mapper/combiner/reducer executable and the sequential reference
# once, on the shared filesystem, so every node runs the same binary
export Q3="$SCRIPT_DIR/q3"
if [ ! -x "$Q3" ] || [ "$SCRIPT_DIR/Q3.cpp" -nt "$Q3" ]; then
    g++ -O2 -std=c++17 -o "$Q3" "$SCRIPT_DIR/Q3.cpp" || exit 1
fi
SEQUENTIAL="$SCRIPT_DIR/sequential"
if [ ! -x "$SEQUENTIAL" ] || [ "$SCRIPT_DIR/sequential.cpp" -nt "$SEQUENTIAL" ]; then
    g++ -O2 -std=c++17 -o "$SEQUENTIAL" "$SCRIPT_DIR/sequential.cpp" || exit 1
fi

# Start-up cost of one srun step (median of 3 empty steps)
SRUN_TIMES=()
for _ in 1 2 3; do
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS true
    STAGE_END=$(date +%s%N)
    SRUN_TIMES+=("$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)")
done
SRUN_OVERHEAD=$(printf '%s\n' "${SRUN_TIMES[@]}" | sort -n | sed -n 2p)

# ------------------------------------------------------------
# run_dist_job NAME MAPPER COMBINER REDUCER INPUTS
#   One MapReduce job with SLURM_NTASKS mappers and SLURM_NTASKS
#   reducers. Mapper TID reads INPUTS with "TID" replaced by its
#   task id (00, 01, ...). An empty COMBINER skips Shuffle 1 and
#   the combiner. Reducer TID writes NAME/red_TID.out.
#   Sets JOB_TIME and adds to the stage totals (MAP_S, SORT_S,
#   COMBINE_S, SHUFFLE_S, REDUCE_S, NUM_SRUNS, SHUFFLE_BYTES).
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
    MAP_S=$(echo "$MAP_S + $MAPPER_TIME" | bc)
    NUM_SRUNS=$((NUM_SRUNS + 1))

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
        SORT_S=$(echo "$SORT_S + $SHUFFLE1_TIME" | bc)

        # Stage 3: Combiner (Distributed)
        STAGE_START=$(date +%s%N)
        srun --ntasks=$SLURM_NTASKS bash -c '
            TID=$(printf "%02d" $SLURM_PROCID)
            "$Q3" $COMBINER < "$JOB/shuf1_${TID}.out" > "$JOB/comb_${TID}.out"
        '
        STAGE_END=$(date +%s%N)
        COMBINER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
        echo "    Combiner (Dist):  ${COMBINER_TIME}s"
        COMBINE_S=$(echo "$COMBINE_S + $COMBINER_TIME" | bc)
        NUM_SRUNS=$((NUM_SRUNS + 2))
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
    SHUFFLE_S=$(echo "$SHUFFLE_S + $SHUFFLE2_TIME" | bc)
    NUM_SRUNS=$((NUM_SRUNS + 2))
    SHUFFLE_BYTES=$((SHUFFLE_BYTES + $(stat -c %s "$JOB"/part_* | awk '{ s += $1 } END { print s + 0 }')))

    # Stage 5: Reducer (Distributed)
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        "$Q3" $REDUCER < "$JOB/shuf2_${TID}.out" > "$JOB/red_${TID}.out"
    '
    STAGE_END=$(date +%s%N)
    REDUCER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "    Reducer (Dist):   ${REDUCER_TIME}s"
    REDUCE_S=$(echo "$REDUCE_S + $REDUCER_TIME" | bc)
    NUM_SRUNS=$((NUM_SRUNS + 1))

    JOB_END=$(date +%s%N)
    JOB_TIME=$(echo "scale=6; ($JOB_END - $JOB_START) / 1000000000" | bc)
    echo "    Job time:         ${JOB_TIME}s"
}

echo "============================================"
echo "Distributed MapReduce Triangle Counting Benchmark"
echo "Date: $(date)"
echo "Nodes Allocated: $SLURM_JOB_NODELIST"
echo "Number of Tasks: $SLURM_NTASKS"
echo "Runs per graph:  $REPEATS"
echo "srun overhead:   ${SRUN_OVERHEAD}s per step"
echo "============================================"
echo ""

MISMATCHES=0

for INPUT_PATH in "${TEST_PATHS[@]}"; do
    TEST_FILE=$(basename "$INPUT_PATH")

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

    OUTPUT_FILE="$RESULTS_DIR/dist_output_P${SLURM_NTASKS}_${TEST_FILE}"

    for ((RUN = 1; RUN <= REPEATS; RUN++)); do
        echo " Run $RUN/$REPEATS"

        # Reference answer and baseline time: sequential counter on one node
        STAGE_START=$(date +%s%N)
        SEQ_TRIANGLES=$("$SEQUENTIAL" < "$INPUT_PATH")
        STAGE_END=$(date +%s%N)
        SEQ_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)

        # All intermediate files live in a work directory on the shared
        # filesystem, so every node can read what the others wrote
        WORK_DIR="$SCRIPT_DIR/q3_work_${SLURM_JOB_ID:-local}"
        rm -rf "$WORK_DIR"
        mkdir -p "$WORK_DIR"
        cd "$WORK_DIR"

        MAP_S=0 SORT_S=0 COMBINE_S=0 SHUFFLE_S=0 REDUCE_S=0 NUM_SRUNS=0 SHUFFLE_BYTES=0

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
        if [ "$TRIANGLES" = "$SEQ_TRIANGLES" ]; then
            CORRECT=yes
        else
            CORRECT=no
            MISMATCHES=$((MISMATCHES + 1))
        fi
        echo "  ─────────────────────"
        echo "  TOTAL:            ${TOTAL_TIME}s  (sequential: ${SEQ_TIME}s)"
        echo "  Triangles:        ${TRIANGLES}  (sequential: ${SEQ_TRIANGLES}, correct: ${CORRECT})"
        echo ""

        # Record to CSV
        echo "${TEST_FILE},${INPUT_SIZE},${NUM_EDGES},${SLURM_NTASKS},${SLURM_JOB_NUM_NODES:-1},${RUN},${TOTAL_TIME},${DEGREE_TIME},${WEDGE_TIME},${CLOSE_TIME},${TOTAL_JOB_TIME},${MAP_S},${SORT_S},${COMBINE_S},${SHUFFLE_S},${REDUCE_S},${NUM_SRUNS},${SRUN_OVERHEAD},${SHUFFLE_BYTES},${SEQ_TIME},${TRIANGLES},${SEQ_TRIANGLES},${CORRECT}" >> "$SUMMARY_FILE"

        # Cleanup temp files for this iteration
        cd "$SCRIPT_DIR"
        rm -rf "$WORK_DIR"
    done
done

echo ""
echo "============================================"
echo "Distributed Benchmark complete!"
echo "Summary CSV: $SUMMARY_FILE"
if [ "$MISMATCHES" -eq 0 ]; then
    echo "Correctness: every run matches the sequential count"
else
    echo "Correctness: $MISMATCHES run(s) DO NOT match the sequential count"
fi
echo "============================================"
echo ""
echo "--- CSV Summary ---"
cut -d',' -f1,4,6,7,12-16,19-23 "$SUMMARY_FILE" | column -t -s','
[ "$MISMATCHES" -eq 0 ]
