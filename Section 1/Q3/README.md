# Q3 – Triangle Counting (MapReduce)

## Files
| File | Purpose |
|---|---|
| `Q3.cpp` | Every mapper, combiner and reducer, chosen by the first argument (for example `./q3 degree_map`). It reads key/value lines from stdin and writes them to stdout, the same way Hadoop Streaming works |
| `Q3ForLocalTesting.sh` | Runs the 4 jobs one after another on one machine, each as a plain `mapper \| sort \| combiner \| sort \| reducer` pipeline |
| `Q3_distributed.sh` | The SLURM benchmark. It runs the pipeline with P mappers and P reducers (P = SLURM tasks), times every stage, and checks each answer against `sequential.cpp`. Without SLURM, it emulates `srun` with local processes |
| `submit_all.sh` | Submits `Q3_distributed.sh` for P = 1, 2, 4 and 8 on RCE, one job after another |
| `sequential.cpp` | Single-process triangle counter: the reference for correctness and the baseline for timing |
| `verify.sh` | Local correctness check: sequential vs known answers, then the local pipeline and P = 1, 2, 4, 8 vs sequential |
| `gen_graph.py` | Reproducible test-graph generator |
| `plot_results.py` | Speedup, efficiency and time-breakdown plots, plus `results.md` tables, from the benchmark CSVs |
| `test_data/` | `q3_sample.txt` (the sample from the question), `q3_small.txt`, `q3_medium.txt`, `q3_large.txt` |

## Build and run
The scripts compile `q3` and `sequential` automatically with `g++ -O2 -std=c++17`.
```bash
./Q3ForLocalTesting.sh test_data/q3_sample.txt output.txt   # one graph, locally: answer in output.txt
./verify.sh                                                 # full correctness check, locally
```
**On RCE**, run these from this folder on the login node:
```bash
./submit_all.sh              # P = 1, 2, 4, 8, run one after another; check with: squeue -u $USER
sbatch Q3_distributed.sh     # or a single run with P = 4 (use --nodes=N --ntasks=P for other values)
```
Each run writes `perf_results/q3_summary_P<P>.csv`, with one row per graph per repeat and every column explained in the script. The per-stage log goes to `q3_P<P>_<jobid>.out`. The environment variable `REPEATS` sets the number of runs per graph (default 3). Once all four jobs have finished, make the tables and plots (this needs `pip install matplotlib`):
```bash
python3 plot_results.py      # -> perf_results/results.md, perf_results/plots/*.png
```

## Design (degree-ordered NodeIterator++)
| Job | Map | Combine | Reduce |
|---|---|---|---|
| 1 Degree | `u v` → `(u,1)`, `(v,1)` | sum | sum → `deg(v)` |
| 2 Wedge | Orient each edge from the endpoint with the lower rank `(deg, id)` to the higher one: `(lo, hi)`. The degree table goes to every mapper as a distributed-cache file | – | For each `lo`, emit `("a,b", 1)` for every pair of its higher-ranked neighbours |
| 3 Close | Edge → `("min,max", $)`; wedges pass through unchanged | Merge the wedge counts, keep one `$` | If `$` is present for a key, add its wedge count to the partial total |
| 4 Total | identity | partial sum | global sum → one integer |

**Data distribution.** The `V E` header is dropped, and the edge list is split into P chunks by line (`split -n l/P`), one per mapper. Each map task runs `mapper | sort | combiner`, then hash-partitions its output into P buckets (`hash(key) % P`). Reducer i collects bucket i from every mapper through the shared filesystem, sorts it and reduces it. This is the shuffle. Only Job 4 runs on a single node, because it adds up just P partial counts.

**No double counting.** `(deg, id)` is a total order, so each triangle x, y, z with rank(x) < rank(y) < rank(z) produces exactly one wedge. That wedge is centred at x and closed by the edge (y, z), so the triangle is counted once.

**Efficiency.** Pointing edges toward higher-degree vertices keeps the number of wedges within O(E^1.5). A high-degree hub never lists all pairs of its neighbours, which would cost O(d²).

**Robustness.** Self-loops are ignored. Duplicate edges, whether written `u v` twice or as `u v` and `v u`, are removed in the wedge reducer (sort + unique) and by keeping a single `$` marker.

## Correctness verification
`sequential.cpp` counts triangles in a single process with the same degree-ordered method. It uses a sorted, de-duplicated edge list and marks common neighbours. `verify.sh` first checks it against graphs whose answers are known by construction. It then checks that the local pipeline and `Q3_distributed.sh` with P = 1, 2, 4 and 8 all give the same count:
```
graph               known  sequential    local      P=1      P=2      P=4      P=8  result
q3_large.txt       784965      784965   784965   784965   784965   784965   784965  PASS
q3_medium.txt       82782       82782    82782    82782    82782    82782    82782  PASS
q3_sample.txt           2           2        2        2        2        2        2  PASS
q3_small.txt         9312        9312     9312     9312     9312     9312     9312  PASS
bipartite.txt           0           0        0        0        0        0        0  PASS
complete.txt        34220       34220    34220    34220    34220    34220    34220  PASS
duplicates.txt          2           2        2        2        2        2        2  PASS
one_triangle.txt        1           1        1        1        1        1        1  PASS
path.txt                0           0        0        0        0        0        0  PASS
star.txt                0           0        0        0        0        0        0  PASS
```
The edge cases cover:
- `one_triangle`: 3 edges, so with P = 8 some mappers get no input.
- `path` and `star`: triangle-free; the star has one hub of degree 999.
- `bipartite`: K(30,30), 900 edges and no triangles.
- `complete`: K60, with C(60,3) = 34220 triangles and every vertex of equal degree, which tests the tie-break on id.
- `duplicates`: the sample with repeated and reversed edges and self-loops.

The known answers for the four test graphs were also checked with an independent brute-force counter. On RCE, `Q3_distributed.sh` compares every run with `sequential` too, in the CSV's `correct` column.

## Datasets
The graphs follow the question's constraints (V ≤ 10⁵, E ≤ 10⁶). They are simple graphs whose edges are 20 % from 5 hub vertices, 30 % inside dense groups of 30 consecutive vertices (which creates many triangles) and 50 % uniformly random. `gen_graph.py` reproduces the files byte for byte:

| File | Command | V | E | Size | Triangles |
|---|---|---:|---:|---:|---:|
| `q3_sample.txt` | (sample from the question) | 4 | 5 | 24 B | 2 |
| `q3_small.txt` | `python3 gen_graph.py 1000 10000 1` | 10³ | 10⁴ | 76 KB | 9 312 |
| `q3_medium.txt` | `python3 gen_graph.py 10000 100000 2` | 10⁴ | 10⁵ | 0.93 MB | 82 782 |
| `q3_large.txt` | `python3 gen_graph.py 100000 1000000 3` | 10⁵ | 10⁶ | 11.3 MB | 784 965 |

## Experiments
- **Configurations.** P = 1, 2, 4, 8 tasks (mappers = reducers), each on the 4 graphs, repeated 3 times; tables and plots use the median. Tasks are spread over min(P, 4) nodes, so P = 8 runs 2 tasks per node. `submit_all.sh` runs the 4 jobs one after another (`--dependency=singleton`), so they never compete for nodes or for the shared filesystem.
- **Speedup and efficiency.** S(P) = T(1) / T(P) and E(P) = S(P) / P, where T is the wall time of the whole pipeline: split, 4 jobs and the final sum. The sequential time is reported alongside for reference.
- **Computation vs communication.** For jobs 1–3, the time is split into:
  - computation: map, combine and reduce;
  - local sort;
  - shuffle: partitioning, transfer of the buckets through the shared filesystem, and the merge sort at the reducer;
  - `srun` start-up: the measured cost of one empty `srun` step × the number of steps (16 per run);
  - other: splitting, bookkeeping and the final sum.

  The CSV also records `shuffle_bytes`, the amount of intermediate data sent from mappers to reducers.

## Results
Results from RCE go in `perf_results/results.md` and `perf_results/plots/`, which `plot_results.py` creates from the four CSVs written by `submit_all.sh`.
