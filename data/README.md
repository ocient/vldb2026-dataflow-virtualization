# Data and Comparison Harnesses

This directory holds the cross-engine WCC comparison harnesses used in
Section 9.6 of the paper, and acts as the staging area for input
datasets used by other experiments.

## Status

- **Experiments 1--4** generate their own input tables in-process. Each Java
  driver in `experiments/java/` builds its working set with a `CREATE TABLE
  ... AS SELECT ... FROM sys.dummyN` against the engine's built-in
  `sys.dummy*` row-source views, so no separate dataset is needed before
  launching them.

- **Experiment 5 (Hyperscale WCC)** on Ocient consumes pre-built
  `bench_vertices` / `bench_edges` tables (one schema per scale:
  `wcc100m`, `wcc1b`, `wcc10b`, `wcc100b`, `wcc1t`). The synthetic
  Zipfian (power-law) generator is bundled at
  `experiments/data_gen/generate_zipfian_graph.py`:

      cd experiments/data_gen
      python generate_zipfian_graph.py --scale wcc1b

  See `experiments/data_gen/README.md` for full usage.

- **Section 9.6 cross-engine comparison** (DuckDB and Umbra on GCP)
  uses the harnesses in this directory: `duckdb_wcc_bench.py` and
  `umbra_wcc_bench.py`. Each harness generates the same Zipfian
  distribution (`alpha = 1.5`, seed `42`) used by the Ocient runs, then
  loads it into the target engine and runs the label-propagation WCC
  shown in `../dataflows/wcc.sql`.

## Cross-engine harness usage

```sh
# DuckDB (single-node, in-process). v1.5 or later.
pip install duckdb numpy
python3 duckdb_wcc_bench.py --scale 1b

# Umbra (single-node, PostgreSQL wire protocol). Container image at
# umbradb/umbra:latest; default credentials postgres/postgres.
docker run -d --name umbra --ulimit memlock=-1:-1 -p 5432:5432 \
    umbradb/umbra:latest
pip install psycopg2-binary numpy
python3 umbra_wcc_bench.py --scale 1b
```

Each harness writes one JSONL line per run to `<engine>_results.jsonl`
with the WCC wall time, iteration count, and component count.

The generated edges are directed and WCC ignores direction, so both
harnesses (and `../dataflows/wcc.sql`) propagate labels along both
directions of every edge. After the timed run each harness counts the
edges whose endpoints ended up with different labels (`cut_edges` in the
JSONL line; 0 means the labels are the weakly connected components). Pass
`--no-verify` to skip that check. `../tests/test_wcc.py` runs both harnesses
and the dataflow listing against small graphs and scipy
(`pip install duckdb numpy scipy pytest && pytest tests/`). Both
harnesses accept `--scale {100m, 1b, 10b}` and pass through `--alpha`,
`--host`, etc.

The numbers in the paper's Section 9.6.2 (per-node and single-node
extrapolation tables) were collected on the `n2d-highmem-96` GCP shape
(96 vCPU AMD EPYC Milan, 755 GB RAM), the largest AMD-EPYC shape we
could obtain without a quota-increase ticket. Section 9.6.2 explains
the per-node hardware correction (5x) used to extrapolate those
measurements onto a single Ocient EPYC 9654 node.

## Note on a fourth engine

A reviewer-requested Vertica comparison was also conducted on the same
GCP shape. The OpenText (formerly Micro Focus) End-User License
Agreement that governs Vertica Community Edition forbids publishing
benchmark results without prior written consent, so those numbers are
not in the paper or this repository.
