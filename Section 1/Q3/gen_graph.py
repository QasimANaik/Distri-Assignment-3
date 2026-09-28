"""
Q3 - Test graph generator.

    usage: python3 gen_graph.py V E SEED > graph.txt

Writes a simple undirected graph (no self-loops, no duplicate edges) with V
vertices and E edges in the input format of the question ("V E" header, then
one "u v" edge per line). To give the pipeline realistic work, the edges are:
  - 20%: from 5 random hub vertices (high degree: stresses the degree ordering)
  - 30%: inside random groups of 30 consecutive vertices (dense: many triangles)
  - 50%: uniformly random
Each edge is written as "u v" or "v u" at random; the edge order is
scrambled (it is the iteration order of a set of integer pairs, which is
deterministic).
The output depends only on (V, E, SEED), so every graph can be regenerated.

The files in test_data/ were made with:
    python3 gen_graph.py 1000   10000   1 > test_data/q3_small.txt
    python3 gen_graph.py 10000  100000  2 > test_data/q3_medium.txt
    python3 gen_graph.py 100000 1000000 3 > test_data/q3_large.txt
(test_data/q3_sample.txt is the sample from the question.)
"""
import random
import sys


def generate(V, E, seed):
    rng = random.Random(seed)
    edges = set()

    def add(u, v):
        if u != v:
            edges.add((min(u, v), max(u, v)))

    hubs = rng.sample(range(V), 5)
    while len(edges) < E // 5:
        add(rng.choice(hubs), rng.randrange(V))
    while len(edges) < E // 2:
        base = rng.randrange(0, V - 30)
        add(base + rng.randrange(30), base + rng.randrange(30))
    while len(edges) < E:
        add(rng.randrange(V), rng.randrange(V))

    lines = ["%d %d" % (u, v) if rng.random() < .5 else "%d %d" % (v, u) for u, v in edges]
    return ["%d %d" % (V, E)] + lines


def main():
    if len(sys.argv) != 4:
        sys.exit("usage: python3 gen_graph.py V E SEED > graph.txt")
    V, E, seed = (int(x) for x in sys.argv[1:])
    if V < 31 or E > V * (V - 1) // 2:
        sys.exit("need V >= 31 and E <= V(V-1)/2")
    print("\n".join(generate(V, E, seed)))


if __name__ == "__main__":
    main()
