#!/usr/bin/env python3
"""DuckDB Weakly Connected Components benchmark.

Generates a Zipfian-skewed directed graph in DuckDB at a configurable scale
and runs label-propagation WCC, mirroring the algorithm used by the paper's
Ocient harness so the comparison is apples-to-apples.

Outputs a JSON line per scale with timings to stdout.

Usage:
    python duckdb_wcc_bench.py --scale 100m
    python duckdb_wcc_bench.py --scale 1b --db /mnt/work/duck.db --threads 32
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import duckdb


SCALES = {
    "100m": (10_000_000, 100_000_000),
    "1b": (100_000_000, 1_000_000_000),
    "10b": (1_000_000_000, 10_000_000_000),
}


def setup_db(db_path: str, threads: int, mem_gb: int) -> duckdb.DuckDBPyConnection:
    if Path(db_path).exists():
        Path(db_path).unlink()
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={threads}")
    con.execute(f"PRAGMA memory_limit='{mem_gb}GB'")
    con.execute(f"PRAGMA temp_directory='{os.path.dirname(db_path) or '.'}/duckdb_tmp'")
    return con


def generate_graph(con, nv: int, ne: int, alpha: float) -> tuple[float, float]:
    t0 = time.monotonic()
    con.execute(f"""
        CREATE TABLE vertices AS
        SELECT range AS id FROM range(1, {nv} + 1)
    """)
    t_v = time.monotonic() - t0

    t0 = time.monotonic()
    con.execute(f"""
        CREATE TABLE edges AS
        SELECT
          CAST(FLOOR({nv} * POWER(random(), {alpha})) + 1 AS BIGINT) AS src,
          CAST(FLOOR({nv} * POWER(random(), {alpha})) + 1 AS BIGINT) AS dest
        FROM range(1, {ne} + 1)
    """)
    t_e = time.monotonic() - t0
    return t_v, t_e


def run_wcc(con, max_iters: int = 100) -> tuple[float, int, int]:
    """Label-propagation WCC, identical structure to the Ocient harness."""
    t0 = time.monotonic()
    con.execute("CREATE OR REPLACE TABLE labels AS SELECT id AS node_id, id AS label FROM vertices")

    iters = 0
    for i in range(max_iters):
        iters += 1
        # Edges are directed but WCC ignores direction, so labels flow both ways.
        con.execute("""
            CREATE OR REPLACE TABLE updates AS
            SELECT node_id, MIN(label) AS new_label
            FROM (
              SELECT e.src AS node_id, l.label FROM edges e JOIN labels l ON e.dest = l.node_id
              UNION ALL
              SELECT e.dest AS node_id, l.label FROM edges e JOIN labels l ON e.src = l.node_id
            ) AS nbr
            GROUP BY node_id
        """)
        con.execute("""
            CREATE OR REPLACE TABLE labels_next AS
            SELECT l.node_id,
                   LEAST(COALESCE(u.new_label, l.label), l.label) AS label
            FROM labels l LEFT JOIN updates u ON l.node_id = u.node_id
        """)
        changes_row = con.execute("""
            SELECT count(*) FROM labels l, labels_next n
            WHERE l.node_id = n.node_id AND l.label != n.label
        """).fetchone()
        changes = changes_row[0] if changes_row else 0
        con.execute("DROP TABLE labels")
        con.execute("ALTER TABLE labels_next RENAME TO labels")
        con.execute("DROP TABLE updates")
        if changes == 0:
            break

    components = con.execute("SELECT count(DISTINCT label) FROM labels").fetchone()[0]
    return time.monotonic() - t0, iters, components


def count_cut_edges(con) -> int:
    """Edges whose endpoints ended up with different labels. 0 means every label
    class is closed under the edge relation, i.e. the labels are the weakly
    connected components (labels are member ids, so components never share one)."""
    return con.execute("""
        SELECT count(*) FROM edges e
        JOIN labels a ON e.src = a.node_id
        JOIN labels b ON e.dest = b.node_id
        WHERE a.label != b.label
    """).fetchone()[0]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--scale", required=True, choices=sorted(SCALES.keys()))
    p.add_argument("--alpha", type=float, default=1.5)
    p.add_argument("--db", default="/mnt/work/duck.db")
    p.add_argument("--threads", type=int, default=os.cpu_count() or 32)
    p.add_argument("--mem-gb", type=int, default=200)
    p.add_argument("--output", default="duckdb_results.jsonl")
    p.add_argument("--no-verify", dest="verify", action="store_false",
                   help="skip the post-run check that no edge joins two labels")
    args = p.parse_args()

    nv, ne = SCALES[args.scale]
    print(f"[duckdb_wcc_bench] scale={args.scale}, NV={nv:,}, NE={ne:,}", flush=True)

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)

    con = setup_db(args.db, args.threads, args.mem_gb)
    print(f"[duckdb_wcc_bench] generating Zipfian graph (alpha={args.alpha})...", flush=True)
    t_v, t_e = generate_graph(con, nv, ne, args.alpha)
    print(f"[duckdb_wcc_bench] vertices: {t_v:.1f}s, edges: {t_e:.1f}s", flush=True)

    print(f"[duckdb_wcc_bench] running WCC...", flush=True)
    t_wcc, iters, comps = run_wcc(con)
    print(f"[duckdb_wcc_bench] WCC: {t_wcc:.1f}s, {iters} iterations, {comps:,} components", flush=True)
    cut = None
    if args.verify:
        cut = count_cut_edges(con)  # outside the timed region
        print(f"[duckdb_wcc_bench] verify: {cut:,} edges join different labels", flush=True)

    result = {
        "engine": "duckdb",
        "version": duckdb.__version__,
        "scale": args.scale,
        "num_vertices": nv,
        "num_edges": ne,
        "alpha": args.alpha,
        "threads": args.threads,
        "mem_gb": args.mem_gb,
        "vertex_gen_seconds": round(t_v, 3),
        "edge_gen_seconds": round(t_e, 3),
        "wcc_seconds": round(t_wcc, 3),
        "wcc_iterations": iters,
        "components": comps,
        "cut_edges": cut,
    }
    with open(args.output, "a") as f:
        f.write(json.dumps(result) + "\n")
    print(json.dumps(result), flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
