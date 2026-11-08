#!/usr/bin/env python3
"""Umbra Weakly Connected Components benchmark.

Adapts the Vertica/DuckDB harness to Umbra (TUM, PostgreSQL-compatible
wire protocol). Streams Zipfian-skewed directed-graph rows into Umbra
via psycopg2 copy_expert and runs the same label-propagation WCC the
Ocient harness uses.

Usage:
    python3 umbra_wcc_bench.py --scale 100m
"""
from __future__ import annotations

import argparse
import io
import json
import random
import sys
import time

import numpy as np
import psycopg2


SCALES = {
    "100m": (10_000_000, 100_000_000),
    "1b": (100_000_000, 1_000_000_000),
    "10b": (1_000_000_000, 10_000_000_000),
}


class GeneratorFile(io.RawIOBase):
    """Wrap a generator yielding str chunks as a binary file-like object so
    psycopg2.copy_expert can stream from it without buffering the whole
    payload."""
    def __init__(self, generator):
        self.gen = generator
        self.buf = b""
        self._eof = False

    def readable(self):
        return True

    def read(self, size=-1):
        if size is None or size < 0:
            chunks = [self.buf]
            self.buf = b""
            for s in self.gen:
                chunks.append(s if isinstance(s, bytes) else s.encode("ascii"))
            self._eof = True
            return b"".join(chunks)
        while len(self.buf) < size and not self._eof:
            try:
                s = next(self.gen)
                self.buf += s if isinstance(s, bytes) else s.encode("ascii")
            except StopIteration:
                self._eof = True
        out, self.buf = self.buf[:size], self.buf[size:]
        return out


def conn(args):
    return psycopg2.connect(
        host=args.host, port=args.port,
        user=args.user, password=args.password, dbname=args.database,
    )


def execute(con, sql):
    with con.cursor() as c:
        c.execute(sql)


def execute_scalar(con, sql):
    with con.cursor() as c:
        c.execute(sql)
        row = c.fetchone()
        return row[0] if row else None


def setup_tables(con):
    for stmt in (
        "DROP TABLE IF EXISTS edges",
        "DROP TABLE IF EXISTS vertices",
        "DROP TABLE IF EXISTS labels",
        "DROP TABLE IF EXISTS labels_next",
        "DROP TABLE IF EXISTS updates_t",
        "CREATE TABLE vertices (id BIGINT NOT NULL)",
        "CREATE TABLE edges (src BIGINT NOT NULL, dest BIGINT NOT NULL)",
    ):
        execute(con, stmt)
    con.commit()


def vertex_chunks(nv, batch=2_000_000):
    """Vertices are 1..NV in order; emit as bytes for fastest copy path."""
    i = 1
    while i <= nv:
        end = min(i + batch, nv + 1)
        ids = np.arange(i, end, dtype=np.int64).tolist()
        yield b"".join(b"%d\n" % v for v in ids)
        i = end


def edge_chunks(ne, nv, alpha, batch=2_000_000):
    """Vectorized Zipfian edge generation. numpy random, then bytes-mode
    %-formatter for the per-row encoding (fastest CPython path)."""
    rng = np.random.default_rng(42)
    remaining = ne
    while remaining > 0:
        n = min(batch, remaining)
        u1 = rng.random(n)
        u2 = rng.random(n)
        src = (nv * (u1 ** alpha)).astype(np.int64) + 1
        dest = (nv * (u2 ** alpha)).astype(np.int64) + 1
        np.clip(src, 1, nv, out=src)
        np.clip(dest, 1, nv, out=dest)
        s_list = src.tolist()
        d_list = dest.tolist()
        parts = [b"%d\t%d\n" % (s, d) for s, d in zip(s_list, d_list)]
        yield b"".join(parts)
        remaining -= n


def copy_stream(con, table, columns, generator):
    with con.cursor() as cur:
        cur.copy_expert(
            f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t')",
            GeneratorFile(generator),
        )
    con.commit()


def generate_graph(con, nv, ne, alpha):
    t0 = time.monotonic()
    copy_stream(con, "vertices", "id", vertex_chunks(nv))
    t_v = time.monotonic() - t0

    t0 = time.monotonic()
    copy_stream(con, "edges", "src, dest", edge_chunks(ne, nv, alpha))
    t_e = time.monotonic() - t0
    return t_v, t_e


def run_wcc(con, max_iters=100):
    t0 = time.monotonic()
    execute(con, "DROP TABLE IF EXISTS labels")
    execute(con, "CREATE TABLE labels AS SELECT id AS node_id, id AS label FROM vertices")
    con.commit()

    iters = 0
    for _ in range(max_iters):
        iters += 1
        execute(con, "DROP TABLE IF EXISTS updates_t")
        # Edges are directed but WCC ignores direction, so labels flow both ways.
        execute(con, """
            CREATE TABLE updates_t AS
            SELECT node_id, MIN(label) AS new_label
            FROM (
              SELECT e.src AS node_id, l.label FROM edges e JOIN labels l ON e.dest = l.node_id
              UNION ALL
              SELECT e.dest AS node_id, l.label FROM edges e JOIN labels l ON e.src = l.node_id
            ) AS nbr
            GROUP BY node_id
        """)
        execute(con, "DROP TABLE IF EXISTS labels_next")
        execute(con, """
            CREATE TABLE labels_next AS
            SELECT l.node_id,
                   LEAST(COALESCE(u.new_label, l.label), l.label) AS label
            FROM labels l LEFT JOIN updates_t u ON l.node_id = u.node_id
        """)
        con.commit()
        changes = execute_scalar(con, """
            SELECT count(*) FROM labels l, labels_next n
            WHERE l.node_id = n.node_id AND l.label != n.label
        """)
        execute(con, "DROP TABLE labels")
        execute(con, "ALTER TABLE labels_next RENAME TO labels")
        con.commit()
        if (changes or 0) == 0:
            break

    components = execute_scalar(con, "SELECT count(DISTINCT label) FROM labels")
    return time.monotonic() - t0, iters, components or 0


def count_cut_edges(con):
    """Edges whose endpoints ended up with different labels (0 means the labels are the WCCs)."""
    return execute_scalar(con, """
        SELECT count(*) FROM edges e
        JOIN labels a ON e.src = a.node_id
        JOIN labels b ON e.dest = b.node_id
        WHERE a.label != b.label
    """) or 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--scale", required=True, choices=sorted(SCALES.keys()))
    p.add_argument("--alpha", type=float, default=1.5)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--user", default="postgres")
    p.add_argument("--password", default="postgres")
    p.add_argument("--database", default="postgres")
    p.add_argument("--output", default="umbra_results.jsonl")
    p.add_argument("--no-verify", dest="verify", action="store_false",
                   help="skip the post-run check that no edge joins two labels")
    args = p.parse_args()

    nv, ne = SCALES[args.scale]
    print(f"[umbra_wcc_bench] scale={args.scale} NV={nv:,} NE={ne:,}", flush=True)

    c = conn(args)
    setup_tables(c)

    print("[umbra_wcc_bench] generating Zipfian graph...", flush=True)
    t_v, t_e = generate_graph(c, nv, ne, args.alpha)
    print(f"[umbra_wcc_bench] vertices: {t_v:.1f}s, edges: {t_e:.1f}s", flush=True)

    print("[umbra_wcc_bench] running WCC...", flush=True)
    t_wcc, iters, comps = run_wcc(c)
    print(f"[umbra_wcc_bench] WCC: {t_wcc:.1f}s, {iters} iters, {comps:,} comps", flush=True)
    cut = None
    if args.verify:
        cut = count_cut_edges(c)  # outside the timed region
        print(f"[umbra_wcc_bench] verify: {cut:,} edges join different labels", flush=True)

    result = {
        "engine": "umbra",
        "scale": args.scale,
        "num_vertices": nv,
        "num_edges": ne,
        "alpha": args.alpha,
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
    c.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
