"""Correctness tests for the WCC label propagation in data/ and dataflows/wcc.sql.

Run with:  pip install duckdb numpy pytest scipy && pytest tests/
The Umbra harness and the Ocient dataflows are executed against DuckDB
(the Umbra harness through a psycopg2-style shim, the dataflows through a
small interpreter for its WHILE loop), so no Umbra or Ocient server is needed.
"""
import importlib.util
import re
import sys
import types
from pathlib import Path

import duckdb
import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


duck = _load("duckdb_wcc_bench", "data/duckdb_wcc_bench.py")
sys.modules.setdefault("psycopg2", types.ModuleType("psycopg2"))  # only needed for conn()
umbra = _load("umbra_wcc_bench", "data/umbra_wcc_bench.py")

# (n_vertices, directed edges, expected number of weakly connected components)
CASES = [
    (3, [(1, 2), (3, 2)], 1),             # two edges into a sink: one component
    (3, [(2, 1), (2, 3)], 1),             # source with two out-edges
    (4, [(2, 1), (3, 2), (4, 3)], 1),     # directed path, labels must travel 3 hops
    (4, [(1, 2), (2, 3), (3, 4)], 1),     # same path pointing the other way
    (6, [(1, 2), (3, 2), (5, 4)], 3),     # {1,2,3}, {4,5}, {6}
    (5, [], 5),                           # no edges
]


def _graph(con, nv, edges):
    con.execute("CREATE OR REPLACE TABLE vertices AS SELECT range AS id FROM range(1, ?)", [nv + 1])
    con.execute("CREATE OR REPLACE TABLE edges (src BIGINT, dest BIGINT)")
    if edges:
        con.executemany("INSERT INTO edges VALUES (?, ?)", edges)


def _reference(nv, edges):
    parent = list(range(nv + 1))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        parent[find(a)] = find(b)
    return len({find(v) for v in range(1, nv + 1)})


@pytest.mark.parametrize("nv,edges,expected", CASES)
def test_duckdb_harness_small_graphs(nv, edges, expected):
    assert _reference(nv, edges) == expected
    con = duckdb.connect()
    _graph(con, nv, edges)
    _, _, comps = duck.run_wcc(con)
    assert comps == expected
    assert duck.count_cut_edges(con) == 0


def test_duckdb_harness_matches_scipy_on_zipf_graph():
    sp = pytest.importorskip("scipy.sparse")
    csgraph = pytest.importorskip("scipy.sparse.csgraph")
    con = duckdb.connect()
    con.execute("SELECT setseed(0.42)")
    nv, ne = 20_000, 30_000  # sparse enough to leave many components
    duck.generate_graph(con, nv, ne, 1.5)
    e = con.execute("SELECT src, dest FROM edges").fetchnumpy()
    g = sp.coo_matrix((np.ones(ne), (e["src"], e["dest"])), shape=(nv + 1, nv + 1))
    n_true, _ = csgraph.connected_components(g, directed=True, connection="weak")
    _, _, comps = duck.run_wcc(con)
    assert comps == n_true - 1  # scipy also counts the unused id 0
    assert duck.count_cut_edges(con) == 0


class _Cursor:
    def __init__(self, con):
        self.con = con

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        self.res = self.con.execute(sql)

    def fetchone(self):
        return self.res.fetchone()


class _PgShim:
    """Just enough of a psycopg2 connection for umbra_wcc_bench.run_wcc."""

    def __init__(self):
        self.con = duckdb.connect()

    def cursor(self):
        return _Cursor(self.con)

    def commit(self):
        pass


@pytest.mark.parametrize("nv,edges,expected", CASES)
def test_umbra_harness_small_graphs(nv, edges, expected):
    shim = _PgShim()
    _graph(shim.con, nv, edges)
    _, _, comps = umbra.run_wcc(shim)
    assert comps == expected
    assert umbra.count_cut_edges(shim) == 0


def _dataflow_from_source(rel):
    """The WCC_DATAFLOW string literal of a harness, read from source (the harnesses
    connect to a cluster at import time, so they are not imported)."""
    src = (ROOT / rel).read_text()
    return re.search(r'WCC_DATAFLOW = f?"""(.*?)"""', src, re.S).group(1)


def _run_listing(con, sql, max_rounds=100):
    """Execute a WCC dataflow in DuckDB: #tables -> plain tables, WHILE -> Python loop."""
    sql = "\n".join(line.split("--")[0] for line in sql.splitlines()).replace("#", "")
    init = re.search(r"DECLARE[^;]*;(.*?)WHILE", sql, re.S).group(1)
    body = re.search(r"WHILE\s*\(@changes > 0\)\s*DO(.*?)END WHILE", sql, re.S).group(1)
    for stmt in filter(str.strip, init.split(";")):
        con.execute(stmt)
    changes, rounds = 1, 0
    while changes > 0 and rounds < max_rounds:
        rounds += 1
        for stmt in filter(str.strip, body.split(";")):
            m = re.match(r"\s*SET\s+@changes\s*=\s*(.*)", stmt, re.S)
            if m:
                changes = con.execute("SELECT " + m.group(1)).fetchone()[0]
            else:
                con.execute(stmt)
    return rounds, con.execute("SELECT count(DISTINCT label) FROM Labels").fetchone()[0]


def _tables(con, nv, edges, vtable, etable, src="src", dest="dest"):
    if "." in vtable:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {vtable.split('.')[0]}")
    con.execute(f"CREATE TABLE {vtable} AS SELECT range AS id FROM range(1, ?)", [nv + 1])
    con.execute(f"CREATE TABLE {etable} ({src} BIGINT, {dest} BIGINT)")
    if edges:
        con.executemany(f"INSERT INTO {etable} VALUES (?, ?)", edges)


LISTINGS = [
    # (name, sql, vertex table, edge table, src col, dest col)
    ("dataflows/wcc.sql", (ROOT / "dataflows/wcc.sql").read_text(), "Nodes", "NetworkEdges", "src", "dest"),
    ("harness_1t_skew.py", _dataflow_from_source("experiments/harness/harness_1t_skew.py"),
     "wcc1t.bench_vertices", "wcc1t.bench_edges", "src", "dest"),
    ("d2_supernode_skew_harness.py",
     _dataflow_from_source("data/d2_supernode_skew_harness.py").replace("{SCALE}", "wcc100b"),
     "wcc100b.bench_vertices", "wcc100b.bench_edges", "srcid", "destid"),
]


@pytest.mark.parametrize("name,sql,vt,et,sc,dc", LISTINGS, ids=[l[0] for l in LISTINGS])
@pytest.mark.parametrize("nv,edges,expected", CASES)
def test_dataflows_small_graphs(name, sql, vt, et, sc, dc, nv, edges, expected):
    con = duckdb.connect()
    _tables(con, nv, edges, vt, et, sc, dc)
    rounds, comps = _run_listing(con, sql)
    assert comps == expected
    assert rounds < 100
