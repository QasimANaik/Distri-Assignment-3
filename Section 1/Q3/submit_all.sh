#!/bin/bash
# ============================================================
# Submit the Q3 benchmark for P = 1, 2, 4, 8 tasks on the RCE cluster.
#
#   ./submit_all.sh            (run from this folder, on the login node)
#
# The jobs share one job name and use --dependency=singleton, so SLURM runs
# them one after another: runs never compete for nodes or for the shared
# filesystem, which would distort the timings. Tasks are spread over at most
# MAX_NODES nodes (default 4), so P = 8 runs 2 tasks per node.
#
# Results: perf_results/q3_summary_P<P>.csv, logs: q3_P<P>_<jobid>.out
# ============================================================
cd "$(dirname "$0")"
MAX_NODES=${MAX_NODES:-4}

for P in 1 2 4 8; do
    NODES=$((P < MAX_NODES ? P : MAX_NODES))
    sbatch --job-name=q3_scaling --dependency=singleton \
           --nodes=$NODES --ntasks=$P \
           --output="q3_P${P}_%j.out" --error="q3_P${P}_%j.err" \
           Q3_distributed.sh
done

echo ""
echo "Check progress with:  squeue -u \$USER"
echo "When all 4 jobs are done, download perf_results/ and run: python3 plot_results.py"
