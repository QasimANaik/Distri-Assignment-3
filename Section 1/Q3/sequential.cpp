/*
 * Q3 - Sequential triangle counter: the reference used to verify the
 * MapReduce pipeline and the single-process baseline for the benchmarks.
 *
 *   build:  g++ -O2 -std=c++17 -o sequential sequential.cpp
 *   usage:  ./sequential < graph.txt        (prints the triangle count)
 *
 * Same input handling as the MapReduce version: the "V E" header is skipped,
 * self-loops are ignored and duplicate edges ("u v" twice, or "u v" and
 * "v u") are counted once.
 *
 * Algorithm (forward / degree-ordered): orient every edge from the endpoint
 * with the lower rank (deg, id) to the higher one; then each triangle
 * x < y < z (by rank) is found exactly once, as the edge x -> y plus the
 * common out-neighbour z of x and y. O(E^1.5) time, O(V + E) memory.
 */
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <utility>
#include <vector>

using namespace std;

// Read the next non-negative integer from stdin; false at end of input.
static bool readInt(long &x) {
    int c = getchar_unlocked();
    while (c != EOF && (c < '0' || c > '9')) c = getchar_unlocked();
    if (c == EOF) return false;
    x = 0;
    for (; c >= '0' && c <= '9'; c = getchar_unlocked()) x = x * 10 + (c - '0');
    return true;
}

int main() {
    long V, E, u, v;
    if (!readInt(V) || !readInt(E)) {
        puts("0");
        return 0;
    }

    vector<pair<long, long>> edges;
    edges.reserve(E);
    long maxId = V - 1;
    while (readInt(u) && readInt(v)) {
        if (u == v) continue;
        edges.emplace_back(min(u, v), max(u, v));
        maxId = max(maxId, max(u, v));
    }
    sort(edges.begin(), edges.end());
    edges.erase(unique(edges.begin(), edges.end()), edges.end());

    const size_t n = maxId + 1;
    vector<long> degree(n, 0);
    for (auto &e : edges) ++degree[e.first], ++degree[e.second];
    auto lower = [&](long a, long b) {
        return degree[a] != degree[b] ? degree[a] < degree[b] : a < b;
    };

    // Oriented adjacency lists in CSR form: out[start[x] .. start[x+1]) = out-neighbours of x
    vector<size_t> start(n + 1, 0);
    for (auto &e : edges) ++start[(lower(e.first, e.second) ? e.first : e.second) + 1];
    for (size_t i = 0; i < n; ++i) start[i + 1] += start[i];
    vector<long> out(edges.size());
    vector<size_t> fill(start.begin(), start.end() - 1);
    for (auto &e : edges) {
        bool forward = lower(e.first, e.second);
        long from = forward ? e.first : e.second, to = forward ? e.second : e.first;
        out[fill[from]++] = to;
    }

    // For each x: mark its out-neighbours, then for every out-neighbour y of x
    // count the out-neighbours of y that are marked.
    vector<long> mark(n, -1);
    uint64_t triangles = 0;
    for (size_t x = 0; x < n; ++x) {
        for (size_t i = start[x]; i < start[x + 1]; ++i) mark[out[i]] = x;
        for (size_t i = start[x]; i < start[x + 1]; ++i) {
            long y = out[i];
            for (size_t j = start[y]; j < start[y + 1]; ++j)
                if (mark[out[j]] == static_cast<long>(x)) ++triangles;
        }
    }
    printf("%llu\n", static_cast<unsigned long long>(triangles));
    return 0;
}
