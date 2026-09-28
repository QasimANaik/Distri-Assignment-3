# Q3 – Triangle Counting (MapReduce)

## Files
- `Q3.cpp`: every mapper, combiner and reducer, chosen by the first argument (for example `./q3 degree_map`). It reads key/value lines from stdin and writes them to stdout, the same way Hadoop Streaming works. Both scripts build it automatically with `g++ -O2 -std=c++17 -o q3 Q3.cpp`.
- `Q3ForLocalTesting.sh`: runs the 4 jobs one after another on a single machine, each as a plain `mapper | sort | combiner | sort | reducer` pipeline.
- `Q3_distributed.sh`: the SLURM benchmark script. For each graph in `test_data/`, it splits the edge list across the SLURM tasks and runs every job as `srun` stages: Mapper, Shuffle 1 (local sort), Combiner, Shuffle 2 (hash partition to reducers + sort), and Reducer. It prints the time for each stage and writes `perf_results/q3_dist_benchmark_summary.csv`. Only Job 4, which adds up one partial count per reducer, runs on a single node.
- `test_data/`: `q3_sample.txt` (the sample from the question), `q3_small.txt` (V=10³, E=10⁴), `q3_medium.txt` (V=10⁴, E=10⁵) and `q3_large.txt` (V=10⁵, E=10⁶, the maximum constraints).

## Run
```bash
./Q3ForLocalTesting.sh test_data/q3_sample.txt output.txt   # local: answer in output.txt
sbatch Q3_distributed.sh                                    # RCE cluster: run from this folder
```
On RCE, the stage timings and triangle counts appear in `q3_dist_results_<jobid>.out`, and each graph's answer is in `perf_results/dist_output_<file>`.

## Design (degree-ordered NodeIterator++)
| Job | Map | Combine | Reduce |
|---|---|---|---|
| 1 Degree | `u v` → `(u,1)`, `(v,1)` | sum | sum → `deg(v)` |
| 2 Wedge | orient each edge from the endpoint with the lower rank `(deg, id)` to the higher one: `(lo, hi)`. The degree table goes to every mapper as a distributed-cache file. | – | for each `lo`, emit `("a,b", 1)` for every pair of its higher-ranked neighbours |
| 3 Close | edge → `("min,max", $)`; wedges pass through unchanged | merge the wedge counts, keep one `$` | if `$` is present for a key, add its wedge count to the partial total |
| 4 Total | identity | partial sum | global sum → one integer |

**No double counting.** `(deg, id)` is a total order, so each triangle x, y, z with rank(x) < rank(y) < rank(z) produces exactly one wedge. That wedge is centred at x and closed by the edge (y, z), so the triangle is counted once.

**Efficiency.** Pointing edges toward higher-degree vertices keeps the number of wedges within O(E^1.5). A high-degree hub never lists all pairs of its neighbours, which would cost O(d²).

**Robustness.** The `V E` header is removed during data distribution. Self-loops are ignored. Duplicate edges, whether written `u v` twice or as `u v` and `v u`, are removed in the wedge reducer (sort + unique) and by keeping a single `$` marker.

## Verification
Both scripts match a brute-force counter on every test graph. `Q3_distributed.sh` was checked with 1, 3 and 4 tasks.

| Graph | Triangles |
|---|---|
| `q3_sample.txt` | 2 |
| `q3_small.txt` | 9312 |
| `q3_medium.txt` | 82782 |
| `q3_large.txt` | 784965 |
