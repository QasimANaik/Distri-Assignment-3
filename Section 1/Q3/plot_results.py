"""
Q3 - Speedup / efficiency plots and result tables from the benchmark CSVs.

    usage: python3 plot_results.py [results_dir]        (default perf_results/)

Reads q3_summary_P<P>.csv (written by Q3_distributed.sh, one file per P) and
writes into results_dir:
    plots/time.png        execution time vs P, one line per input graph
    plots/speedup.png     speedup S(P) = T(1) / T(P), with the ideal S = P
    plots/efficiency.png  efficiency E(P) = S(P) / P
    plots/breakdown.png   where the time goes (computation, local sort,
                          shuffle, srun start-up, other), per graph and P
    results.md            the same numbers as tables (median over runs)
Needs matplotlib (pip install matplotlib).
"""
import csv
import glob
import os
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Reference palette (light): categorical slots in fixed order, and chart ink
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"

# srun steps per pipeline run, by stage category (see run_dist_job): map 3,
# combine 2, reduce 3 | local sort 2 | shuffle 2 per job x 3 jobs
SRUNS = {"compute": 8, "sort": 2, "shuffle": 6}
PARTS = [("compute", "Computation (map + combine + reduce)"),
         ("sort", "Local sort"),
         ("shuffle", "Shuffle (partition + transfer + merge)"),
         ("launch", "srun start-up"),
         ("other", "Other (split, bookkeeping, final sum)")]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "sans-serif", "font.size": 10,
    "text.color": INK, "axes.labelcolor": INK_2, "axes.titlecolor": INK,
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.edgecolor": AXIS, "axes.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK_2, "ytick.labelcolor": INK_2,
    "legend.frameon": False, "legend.labelcolor": INK_2,
})


def load(results_dir):
    """rows[(graph, P)] = list of CSV rows (one per run)."""
    rows = {}
    for path in glob.glob(os.path.join(results_dir, "q3_summary_P*.csv")):
        with open(path) as f:
            for row in csv.DictReader(f):
                rows.setdefault((row["input_file"], int(row["num_tasks"])), []).append(row)
    if not rows:
        sys.exit("no q3_summary_P*.csv files in " + results_dir)
    return rows


def median(runs, column):
    return statistics.median(float(r[column]) for r in runs)


def breakdown(runs):
    """Median seconds per category; stage times include their srun start-up,
    which is moved into its own 'launch' category."""
    overhead = median(runs, "srun_overhead_s")
    parts = {
        "compute": median(runs, "map_s") + median(runs, "combine_s") + median(runs, "reduce_s")
                   - SRUNS["compute"] * overhead,
        "sort": median(runs, "sort_s") - SRUNS["sort"] * overhead,
        "shuffle": median(runs, "shuffle_s") - SRUNS["shuffle"] * overhead,
        "launch": median(runs, "num_sruns") * overhead,
    }
    parts = {k: max(v, 0.0) for k, v in parts.items()}
    parts["other"] = max(median(runs, "total_time_s") - sum(parts.values()), 0.0)
    return parts


def label(graph, rows):
    edges = int(next(r for (g, _), rs in rows.items() if g == graph for r in rs)["num_edges"])
    return "%s (%s edges)" % (graph.replace("q3_", "").replace(".txt", ""), format(edges, ","))


def line_chart(ax, graphs, procs, values, rows, title, ylabel, ideal=None):
    if ideal:
        ax.plot(procs, [ideal(p) for p in procs], color=MUTED, linewidth=1.2, linestyle="--",
                zorder=1)
        ax.annotate("ideal", (procs[-1], ideal(procs[-1])), xytext=(6, 0),
                    textcoords="offset points", va="center", color=MUTED, fontsize=9)
    for i, graph in enumerate(graphs):
        ps = [p for p in procs if (graph, p) in values]
        ys = [values[(graph, p)] for p in ps]
        ax.plot(ps, ys, color=SERIES[i], linewidth=2, marker="o", markersize=7,
                markeredgecolor=SURFACE, markeredgewidth=1.5, label=label(graph, rows), zorder=3)
        if ps:   # direct label at the end of each line
            ax.annotate(graph.replace("q3_", "").replace(".txt", ""), (ps[-1], ys[-1]),
                        xytext=(8, 0), textcoords="offset points", va="center",
                        color=INK_2, fontsize=9)
    ax.set_xscale("log", base=2)
    ax.set_xticks(procs)
    ax.set_xticklabels([str(p) for p in procs])
    ax.set_xlim(procs[0] / 1.25, procs[-1] * 1.6)
    ax.set_xlabel("P (mappers = reducers = SLURM tasks)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(loc="upper left", bbox_to_anchor=(0, -0.16), ncol=2, fontsize=9)


def save(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "perf_results")
    rows = load(results_dir)
    procs = sorted({p for _, p in rows})
    graphs = sorted({g for g, _ in rows},
                    key=lambda g: int(next(iter(rows[next(k for k in rows if k[0] == g)]))["num_edges"]))
    plot_dir = os.path.join(results_dir, "plots")
    os.makedirs(plot_dir, exist_ok=True)

    time = {k: median(v, "total_time_s") for k, v in rows.items()}
    seq = {g: statistics.median(float(r["seq_time_s"]) for (gg, _), rs in rows.items()
                                if gg == g for r in rs) for g in graphs}
    speedup = {(g, p): time[(g, 1)] / time[(g, p)] for (g, p) in time if (g, 1) in time}
    efficiency = {k: s / k[1] for k, s in speedup.items()}

    fig, ax = plt.subplots(figsize=(7, 4.6))
    line_chart(ax, graphs, procs, time, rows, "Execution time vs number of tasks",
               "Total time, median of runs (s)")
    ax.set_ylim(bottom=0)
    save(fig, os.path.join(plot_dir, "time.png"))

    if speedup:
        fig, ax = plt.subplots(figsize=(7, 4.6))
        line_chart(ax, graphs, procs, speedup, rows, "Speedup  S(P) = T(1) / T(P)",
                   "Speedup", ideal=lambda p: p)
        ax.set_ylim(bottom=0)
        save(fig, os.path.join(plot_dir, "speedup.png"))

        fig, ax = plt.subplots(figsize=(7, 4.6))
        line_chart(ax, graphs, procs, efficiency, rows, "Efficiency  E(P) = S(P) / P",
                   "Efficiency", ideal=lambda p: 1.0)
        ax.set_ylim(0, 1.15)
        save(fig, os.path.join(plot_dir, "efficiency.png"))

    # Time breakdown: one panel per graph, one stacked bar per P
    parts = {k: breakdown(v) for k, v in rows.items()}
    fig, axes = plt.subplots(1, len(graphs), figsize=(3.2 * len(graphs), 4.4), squeeze=False)
    for ax, graph in zip(axes[0], graphs):
        ps = [p for p in procs if (graph, p) in parts]
        bottom = [0.0] * len(ps)
        for i, (key, name) in enumerate(PARTS):
            heights = [parts[(graph, p)][key] for p in ps]
            ax.bar([str(p) for p in ps], heights, bottom=bottom, width=0.62, color=SERIES[i],
                   edgecolor=SURFACE, linewidth=2, label=name)
            bottom = [b + h for b, h in zip(bottom, heights)]
        for x, total in enumerate(bottom):   # total on top of each bar
            ax.annotate("%.2f" % total, (x, total), xytext=(0, 3), textcoords="offset points",
                        ha="center", fontsize=8, color=INK_2)
        ax.set_title(label(graph, rows), fontsize=10)
        ax.set_xlabel("P")
        ax.grid(axis="x", visible=False)
        ax.set_ylim(0, max(bottom) * 1.12 if bottom else 1)
    axes[0][0].set_ylabel("Seconds (median of runs)")
    handles, names = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, names, loc="lower center", ncol=3, fontsize=9, bbox_to_anchor=(0.5, -0.08))
    fig.suptitle("Where the time goes: computation vs communication", x=0.01, ha="left",
                 fontweight="bold", color=INK)
    save(fig, os.path.join(plot_dir, "breakdown.png"))

    # ---- Tables (table view of every chart) ----
    runs_total = sum(len(v) for v in rows.values())
    runs_wrong = sum(1 for v in rows.values() for r in v if r["correct"] != "yes")
    out = ["# Q3 benchmark results", "",
           "Median over %d runs per configuration. Speedup and efficiency are relative to the "
           "MapReduce pipeline with P = 1. Correctness: %d of %d runs match the sequential count."
           % (max(len(v) for v in rows.values()), runs_total - runs_wrong, runs_total), ""]

    out += ["## Execution time (s)", "",
            "| Graph | Edges | Triangles | Sequential | " + " | ".join("P=%d" % p for p in procs) + " |",
            "|---|---:|---:|---:|" + "---:|" * len(procs)]
    for g in graphs:
        any_row = next(r for (gg, _), rs in rows.items() if gg == g for r in rs)
        out.append("| %s | %s | %s | %.3f | %s |" % (
            g, format(int(any_row["num_edges"]), ","), any_row["seq_triangles"], seq[g],
            " | ".join("%.3f" % time[(g, p)] if (g, p) in time else "–" for p in procs)))

    if speedup:
        out += ["", "## Speedup / efficiency", "",
                "| Graph | " + " | ".join("P=%d" % p for p in procs) + " |",
                "|---|" + "---:|" * len(procs)]
        for g in graphs:
            out.append("| %s | %s |" % (g, " | ".join(
                "%.2f / %.0f%%" % (speedup[(g, p)], 100 * efficiency[(g, p)])
                if (g, p) in speedup else "–" for p in procs)))

    out += ["", "## Time breakdown (s, and share of total)", "",
            "| Graph | P | " + " | ".join(name for _, name in PARTS) + " | Shuffled data |",
            "|---|---:|" + "---:|" * (len(PARTS) + 1)]
    for g in graphs:
        for p in procs:
            if (g, p) not in parts:
                continue
            b, total = parts[(g, p)], sum(parts[(g, p)].values())
            out.append("| %s | %d | %s | %.1f MB |" % (g, p, " | ".join(
                "%.3f (%.0f%%)" % (b[k], 100 * b[k] / total if total else 0) for k, _ in PARTS),
                median(rows[(g, p)], "shuffle_bytes") / 1e6))

    with open(os.path.join(results_dir, "results.md"), "w") as f:
        f.write("\n".join(out) + "\n")
    print("wrote", os.path.join(results_dir, "results.md"))


if __name__ == "__main__":
    main()
