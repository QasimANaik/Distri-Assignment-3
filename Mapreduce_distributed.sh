#!/bin/bash
#SBATCH --job-name=mapreduce_dist_benchmark
#SBATCH --output=benchmark_dist_results_%j.out
#SBATCH --error=benchmark_dist_results_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:15:00

# ============================================================
# Distributed MapReduce IDF Performance Benchmark — SLURM Script
# Profiles multi-node mapper, shuffle/sort, combiner, reducer stages
# for input sizes: 1MB, 10MB, 50MB, 100MB
# ============================================================

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
SUMMARY_FILE="$RESULTS_DIR/dist_benchmark_summary.csv"

# CSV header (added nodes/tasks information)
echo "input_file,input_size_bytes,num_docs,num_tasks,mapper_time_s,shuffle1_time_s,combiner_time_s,shuffle2_time_s,reducer_time_s,total_time_s" > "$SUMMARY_FILE"

TEST_FILES=(
    "p1_small.txt"
    "p1_medium.txt"
    "p1_large.txt"
)

echo "============================================"
echo "Distributed MapReduce IDF Performance Benchmark"
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

    # Using the same file size check as the original benchmark script
    INPUT_SIZE=$(stat --format=%s "$INPUT_PATH" 2>/dev/null || stat -f%z "$INPUT_PATH" 2>/dev/null)
    NUM_DOCS=$(wc -l < "$INPUT_PATH")
    INPUT_SIZE_MB=$(echo "scale=2; $INPUT_SIZE / 1048576" | bc)

    echo "--------------------------------------------"
    echo "Input: $TEST_FILE ($INPUT_SIZE_MB MB, $NUM_DOCS docs)"
    echo "--------------------------------------------"

    OUTPUT_FILE="$RESULTS_DIR/dist_output_${TEST_FILE}"
    
    # ---- Total pipeline timing ----
    TOTAL_START=$(date +%s%N)

    # Setup: Split the input file across tasks smoothly
    # Wait, the split command isn't measured in the time. It is setup overhead.
    if [ -z "$SLURM_NTASKS" ]; then
        SLURM_NTASKS=4 # Fallback for local testing
    fi
    split -d -a 2 -n l/$SLURM_NTASKS "$INPUT_PATH" chunk_

    # Stage 1: Mapper (Distributed)
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        python3 mapper.py < "chunk_${TID}" > "map_${TID}.out"
    '
    STAGE_END=$(date +%s%N)
    MAPPER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Mapper (Dist):   ${MAPPER_TIME}s"

    # Stage 2: Shuffle/Sort 1 (Distributed Local Sort)
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        sort "map_${TID}.out" > "shuf1_${TID}.out"
    '
    STAGE_END=$(date +%s%N)
    SHUFFLE1_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Shuffle 1:       ${SHUFFLE1_TIME}s"

    # Stage 3: Combiner (Distributed)
    STAGE_START=$(date +%s%N)
    srun --ntasks=$SLURM_NTASKS bash -c '
        TID=$(printf "%02d" $SLURM_PROCID)
        python3 combiner.py < "shuf1_${TID}.out" > "comb_${TID}.out"
    '
    STAGE_END=$(date +%s%N)
    COMBINER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Combiner (Dist): ${COMBINER_TIME}s"

    # Stage 4: Shuffle/Sort 2 (Global Gather & Sort on Master Node)
    STAGE_START=$(date +%s%N)
    sort comb_*.out > global_shuf2.out
    STAGE_END=$(date +%s%N)
    SHUFFLE2_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Shuffle 2 (Glob):${SHUFFLE2_TIME}s"

    # Stage 5: Reducer (Single Node on aggregated data)
    STAGE_START=$(date +%s%N)
    python3 reducer.py < global_shuf2.out > "$OUTPUT_FILE"
    STAGE_END=$(date +%s%N)
    REDUCER_TIME=$(echo "scale=6; ($STAGE_END - $STAGE_START) / 1000000000" | bc)
    echo "  Reducer:         ${REDUCER_TIME}s"

    TOTAL_END=$(date +%s%N)
    TOTAL_TIME=$(echo "scale=6; ($TOTAL_END - $TOTAL_START) / 1000000000" | bc)
    echo "  ─────────────────────"
    echo "  TOTAL:           ${TOTAL_TIME}s"
    echo ""

    # Record to CSV (added num_tasks to schema)
    echo "${TEST_FILE},${INPUT_SIZE},${NUM_DOCS},${SLURM_NTASKS},${MAPPER_TIME},${SHUFFLE1_TIME},${COMBINER_TIME},${SHUFFLE2_TIME},${REDUCER_TIME},${TOTAL_TIME}" >> "$SUMMARY_FILE"

    # Cleanup temp files for this iteration
    rm -f chunk_* map_*.out shuf1_*.out comb_*.out global_shuf2.out
done

echo ""
echo "============================================"
echo "Distributed Benchmark complete!"
echo "Summary CSV: $SUMMARY_FILE"
echo "============================================"
echo ""
echo "--- CSV Summary ---"
column -t -s',' "$SUMMARY_FILE"
