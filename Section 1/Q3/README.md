# Q3 – Triangle Counting (MapReduce)

## Files
- `Q3.cpp`: every mapper, combiner and reducer, chosen by the first argument (for example `./q3 degree_map`). It reads key/value lines from stdin and writes them to stdout, the same way Hadoop Streaming works. Build it with `g++ -O2 -std=c++17 -o q3 Q3.cpp`; `run_q3.sh` does this automatically.
- `run_q3.sh`: the driver. It splits the edge list across M mappers, runs the map stage, hash-partitions the output to R reducers (the shuffle), sorts and reduces, and chains 4 jobs. It uses `srun` under SLURM and runs the tasks as parallel local processes otherwise.
- `sample_input.txt`: the sample from the question (expected output: `2`).

## Run
```bash
sbatch run_q3.sh input.txt        # RCE cluster: one map/reduce task per SLURM task
./run_q3.sh input.txt 4           # local run with 4 mappers / 4 reducers
```
The script prints only the triangle count on stdout. Timing for each job goes to stderr.

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
- The sample input gives `2`.
- Random graphs with hub vertices, self-loops and duplicate edges were checked against a brute-force counter, using 1, 3, 4 and 7 mappers.
- A graph with V = 10⁵ and E = 10⁶ edges, including hub vertices, took about 1.7 s locally with 4 tasks and matched the brute-force count.
