/*
 * Q3 - Triangle Counting in an Undirected Graph using MapReduce.
 *
 * Every map / combine / reduce function of the pipeline lives in this file and
 * is selected by the first command-line argument, Hadoop-Streaming style:
 * records are read from stdin and written to stdout as "key<TAB>value" lines,
 * and the framework (run_q3.sh, or Hadoop) sorts/shuffles between the stages.
 *
 *   build:  g++ -O2 -std=c++17 -o q3 Q3.cpp
 *   usage:  ./q3 <phase> [args...]
 *
 * Algorithm (degree-ordered "NodeIterator++", 4 MapReduce jobs):
 *
 *   Job 1  Degree        map:     edge (u,v)            -> (u,1), (v,1)
 *                        combine/reduce: sum            -> (v, deg(v))
 *          The degree table (V <= 1e5 entries) is shipped to every mapper of
 *          job 2 as a distributed-cache file.
 *
 *   Job 2  Orient+Wedge  map:     edge (u,v)            -> (lo, hi), where lo is the
 *                                 endpoint with the smaller rank (deg, id)
 *                        reduce:  lo, [hi1, hi2, ...]   -> ("a,b", 1) for every pair
 *                                 a < b of higher-ranked neighbours of lo
 *                                 (a "wedge" a - lo - b that is open until checked)
 *
 *   Job 3  Close         map:     edge (u,v)            -> ("min,max", $)
 *                                 wedge line            -> unchanged
 *                        combine: merge wedge counts per key, keep one $
 *                        reduce:  "a,b", [$, c1, c2...] -> triangles += sum(c)
 *                                 if the edge marker $ is present
 *
 *   Job 4  Total         map:     identity
 *                        combine: partial sums
 *                        reduce:  global sum -> single integer (final answer)
 *
 * No double counting: ranks form a total order, so every triangle {x, y, z} with
 * rank(x) < rank(y) < rank(z) produces exactly one wedge - the one centred at x
 * with closing edge (y, z) - and is therefore counted exactly once.
 * Orienting edges from lower to higher degree bounds the number of wedges by
 * O(E^1.5) (a high-degree hub never has to enumerate all pairs of its neighbours).
 *
 * Robustness: self-loops are ignored, and duplicate edges (including "u v" and
 * "v u") are collapsed by the dedup in the wedge reducer and the single "$" marker.
 */
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

static const string EDGE_MARK = "$";

// ------------------------------------------------------------------ helpers
// Split a "key<TAB>value" line.
static void splitRecord(const string &line, string &key, string &value) {
    size_t tab = line.find('\t');
    key = line.substr(0, tab);
    value = tab == string::npos ? "" : line.substr(tab + 1);
}

// Call onGroup(key, values) for each run of equal keys in a key-sorted stream
// (reducer input).
static void forEachGroup(istream &in,
                         const function<void(const string &, vector<string> &)> &onGroup) {
    string line, key, value, current;
    vector<string> values;
    bool any = false;
    while (getline(in, line)) {
        if (line.empty()) continue;
        splitRecord(line, key, value);
        if (any && key != current) {
            onGroup(current, values);
            values.clear();
        }
        current = key;
        any = true;
        values.push_back(value);
    }
    if (any) onGroup(current, values);
}

// Parse a "u v" edge line; false for malformed lines and self-loops.
static bool parseEdge(const string &line, long &u, long &v) {
    const char *p = line.c_str();
    char *end;
    u = strtol(p, &end, 10);
    if (end == p) return false;
    p = end;
    v = strtol(p, &end, 10);
    if (end == p) return false;
    for (; *end; ++end)
        if (!isspace(static_cast<unsigned char>(*end))) return false;
    return u != v;
}

// ------------------------------------------------------------ job 1: degree
static void degreeMap() {
    string line;
    long u, v;
    while (getline(cin, line))
        if (parseEdge(line, u, v)) cout << u << "\t1\n" << v << "\t1\n";
}

// Combiner and reducer: sum the integer values of each key.
static void sumValues() {
    forEachGroup(cin, [](const string &key, vector<string> &values) {
        long long total = 0;
        for (const string &x : values) total += stoll(x);
        cout << key << '\t' << total << '\n';
    });
}

// ------------------------------------------------------ job 2: orient+wedge
static void orientMap(const string &degreeFile) {
    unordered_map<long, long> degree;
    ifstream f(degreeFile);
    if (!f) {
        cerr << "cannot open degree file " << degreeFile << '\n';
        exit(1);
    }
    string line, key, value;
    while (getline(f, line)) {
        if (line.empty()) continue;
        splitRecord(line, key, value);
        degree[stol(key)] = stol(value);
    }

    auto rank = [&](long x) {
        auto it = degree.find(x);
        return make_pair(it == degree.end() ? 0L : it->second, x);
    };

    long u, v;
    while (getline(cin, line)) {
        if (!parseEdge(line, u, v)) continue;
        if (rank(u) < rank(v)) cout << u << '\t' << v << '\n';
        else                   cout << v << '\t' << u << '\n';
    }
}

static void wedgeReduce() {
    vector<long> neighbours;
    forEachGroup(cin, [&](const string &, vector<string> &values) {
        neighbours.clear();
        for (const string &x : values) neighbours.push_back(stol(x));
        sort(neighbours.begin(), neighbours.end());
        neighbours.erase(unique(neighbours.begin(), neighbours.end()), neighbours.end());
        for (size_t i = 0; i < neighbours.size(); ++i)
            for (size_t j = i + 1; j < neighbours.size(); ++j)
                cout << neighbours[i] << ',' << neighbours[j] << "\t1\n";
    });
}

// ------------------------------------------------------------- job 3: close
static void joinMap() {
    string line;
    long u, v;
    while (getline(cin, line)) {
        if (line.find('\t') != string::npos) {       // wedge from job 2: pass through
            cout << line << '\n';
        } else if (parseEdge(line, u, v)) {           // raw edge: emit closing marker
            cout << min(u, v) << ',' << max(u, v) << '\t' << EDGE_MARK << '\n';
        }
    }
}

static void wedgesAndMark(const vector<string> &values, bool &closed, long long &wedges) {
    closed = false;
    wedges = 0;
    for (const string &x : values) {
        if (x == EDGE_MARK) closed = true;
        else                wedges += stoll(x);
    }
}

static void joinCombine() {
    forEachGroup(cin, [](const string &key, vector<string> &values) {
        bool closed;
        long long wedges;
        wedgesAndMark(values, closed, wedges);
        if (closed) cout << key << '\t' << EDGE_MARK << '\n';
        if (wedges) cout << key << '\t' << wedges << '\n';
    });
}

static void joinReduce() {
    long long triangles = 0;
    forEachGroup(cin, [&](const string &, vector<string> &values) {
        bool closed;
        long long wedges;
        wedgesAndMark(values, closed, wedges);
        if (closed) triangles += wedges;
    });
    cout << "triangles\t" << triangles << '\n';
}

// ------------------------------------------------------------- job 4: total
static void identity() {
    string line;
    while (getline(cin, line)) cout << line << '\n';
}

static void finalReduce() {
    long long total = 0;
    string line, key, value;
    while (getline(cin, line)) {
        if (line.empty()) continue;
        splitRecord(line, key, value);
        total += stoll(value);
    }
    cout << total << '\n';
}

// --------------------------------------------------------------- framework
// Deterministic FNV-1a hash, so every mapper routes a key to the same reducer.
static uint32_t fnv1a(const string &s) {
    uint32_t h = 2166136261u;
    for (unsigned char c : s) h = (h ^ c) * 16777619u;
    return h;
}

// Shuffle helper: route each record to reducer hash(key) % R.
static void partition(int reducers, const string &prefix) {
    vector<ofstream> files;
    for (int i = 0; i < reducers; ++i) {
        files.emplace_back(prefix + (i < 10 ? "0" : "") + to_string(i));
    }
    string line;
    while (getline(cin, line)) {
        if (line.empty()) continue;
        files[fnv1a(line.substr(0, line.find('\t'))) % reducers] << line << '\n';
    }
}

int main(int argc, char **argv) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    const map<string, function<void()>> phases = {
        {"degree_map",   degreeMap},
        {"sum",          sumValues},
        {"orient_map",   [&] { orientMap(argc > 2 ? argv[2] : "degrees"); }},
        {"wedge_reduce", wedgeReduce},
        {"join_map",     joinMap},
        {"join_combine", joinCombine},
        {"join_reduce",  joinReduce},
        {"identity",     identity},
        {"final_reduce", finalReduce},
        {"partition",    [&] {
             if (argc < 4) { cerr << "usage: partition <R> <prefix>\n"; exit(1); }
             partition(atoi(argv[2]), argv[3]);
         }},
    };

    auto it = argc > 1 ? phases.find(argv[1]) : phases.end();
    if (it == phases.end()) {
        cerr << "usage: " << argv[0] << " {";
        for (auto p = phases.begin(); p != phases.end(); ++p)
            cerr << (p == phases.begin() ? "" : "|") << p->first;
        cerr << "} [args...]\n";
        return 1;
    }
    it->second();
    return 0;
}
