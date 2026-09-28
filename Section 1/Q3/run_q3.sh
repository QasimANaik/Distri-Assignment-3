#!/bin/bash
#SBATCH --job-name=q3_triangles
#SBATCH --output=q3_triangles_%j.out
#SBATCH --error=q3_triangles_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:15:00

# ============================================================
# Q3 - Triangle Counting with MapReduce (driver)
#
#   sbatch run_q3.sh [input_file]      # on the RCE SLURM cluster
#   ./run_q3.sh [input_file] [tasks]   # locally (tasks run as processes)
#
# Builds Q3.cpp into ./q3 and runs 4 chained MapReduce jobs (see Q3.cpp). For each job:
#   map    : M parallel tasks, each on its own input split
#            (mapper | sort | combiner | partition into R buckets)
#   shuffle: bucket i of every mapper goes to reducer i
#   reduce : R parallel tasks (sort | reducer)
# The final answer (a single integer) is printed on stdout; timings on stderr.
# ============================================================
set -euo pipefail
export LC_ALL=C          # byte-wise sort, same as Hadoop's key ordering

if [ -n "${SLURM_SUBMIT_DIR:-}" ]; then
    SCRIPT_DIR="$SLURM_SUBMIT_DIR"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi
INPUT="$(realpath "${1:-$SCRIPT_DIR/sample_input.txt}")"
export M="${SLURM_NTASKS:-${2:-4}}"            # number of mappers / reducers
export Q3="$SCRIPT_DIR/q3"
export WORK="$(mktemp -d "$SCRIPT_DIR/q3_work.XXXXXX")"   # on the shared FS
trap 'rm -rf "$WORK"' EXIT

# Build the mapper/reducer executable once (shared FS: every node uses it)
if [ ! -x "$Q3" ] || [ "$SCRIPT_DIR/Q3.cpp" -nt "$Q3" ]; then
    g++ -O2 -std=c++17 -o "$Q3" "$SCRIPT_DIR/Q3.cpp"
fi

# launch N 'script': run the script as N parallel tasks with SLURM_PROCID set
launch() {
    local n=$1 script=$2
    if [ -n "${SLURM_JOB_ID:-}" ] && command -v srun >/dev/null; then
        srun --ntasks="$n" bash -c "$script"
    else
        local pids=() i
        for ((i = 0; i < n; i++)); do
            SLURM_PROCID=$i bash -c "$script" & pids+=($!)
        done
        for i in "${pids[@]}"; do wait "$i"; done
    fi
}

# run_job NAME "MAP" "COMBINE" "REDUCE" R INPUT...  (inputs may contain {T})
run_job() {
    export JOB=$1 MAP=$2 COMB=$3 RED=$4 R=$5; shift 5
    export INPUTS="$*"
    local start=$(date +%s%N)
    mkdir -p "$WORK/$JOB"

    launch "$M" '
        set -euo pipefail
        T=$(printf "%02d" "$SLURM_PROCID"); cd "$WORK"
        cat ${INPUTS//\{T\}/$T} | "$Q3" $MAP |
            if [ -n "$COMB" ]; then sort | "$Q3" $COMB; else cat; fi |
            "$Q3" partition "$R" "$JOB/map_${T}_"
    '
    launch "$R" '
        set -euo pipefail
        T=$(printf "%02d" "$SLURM_PROCID"); cd "$WORK"
        cat "$JOB"/map_*_"$T" | sort | "$Q3" $RED > "$JOB/out_$T"
    '
    echo "  $JOB: $(echo "scale=3; ($(date +%s%N) - $start) / 1000000000" | bc)s" >&2
}

echo "Input: $INPUT | mappers/reducers: $M" >&2

# Data distribution: drop the "V E" header, split the edge list into M chunks
tail -n +2 "$INPUT" > "$WORK/edges"
(cd "$WORK" && split -d -a 2 -n "l/$M" edges edges_)

run_job degree  "degree_map" "sum" "sum" "$M" "edges_{T}"
cat "$WORK"/degree/out_* > "$WORK/degrees"      # distributed cache for job 2
run_job wedge   "orient_map degrees" "" "wedge_reduce" "$M" "edges_{T}"
run_job close   "join_map" "join_combine" "join_reduce" "$M" "edges_{T}" "wedge/out_{T}"
run_job total   "identity" "sum" "final_reduce" 1 "close/out_{T}"

cat "$WORK/total/out_00"
