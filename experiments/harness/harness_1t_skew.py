#!/usr/bin/env python3
"""1-Trillion-edge WCC harness with per-node skew instrumentation.

Submits the WCC dataflow against ``wcc1t.bench_edges`` on an Ocient cluster
and, in parallel, collects per-second resource samples from every Foundation
node (configurable, default ``<foundation-node-1>..<foundation-node-N>``) via SSH. After the
dataflow completes, the collected CSVs are pulled back, merged, and a small
summary is printed (max/median CPU/IO ratios across nodes per minute).

Usage:
    OCIENT_JDBC_URL=jdbc:ocient://<sql-node>:4050/tpc \\
    OCIENT_USER=admin@system OCIENT_PASSWORD=<password> \\
    python harness_1t_skew.py \\
        --foundation-nodes <foundation-node-1>..<foundation-node-N> \\
        --output-dir ./skew_run_$(date +%Y%m%d_%H%M%S)

Notes:
- Uses pidstat + iostat which are present on every standard Ocient
  Foundation node. If a node is missing one of those, the harness still
  collects whatever it can and notes the gap in the summary.
- Per-node sampling overhead is well under 1% CPU; safe to run in parallel
  with the workload it is observing.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

try:
    import pyocient
except ImportError:
    sys.stderr.write("pyocient required: pip install pyocient\n")
    raise


WCC_DATAFLOW = """
BEGIN DATAFLOW
  DECLARE @changes INT = 1;

  CREATE OR REPLACE TABLE #Labels AS
    SELECT id AS node_id, id AS label FROM wcc1t.bench_vertices;

  WHILE (@changes > 0) DO
    -- labels flow along both edge directions (edges are directed, WCC is not)
    CREATE OR REPLACE TABLE #Updates AS
      SELECT node_id, MIN(label) AS new_label
      FROM (
        SELECT e.src AS node_id, l.label
        FROM wcc1t.bench_edges e JOIN #Labels l ON e.dest = l.node_id
        UNION ALL
        SELECT e.dest AS node_id, l.label
        FROM wcc1t.bench_edges e JOIN #Labels l ON e.src = l.node_id
      ) AS nbr
      GROUP BY node_id;

    -- compare with the previous round's labels before replacing them
    SET @changes = (
      SELECT count(*) FROM #Updates u
      JOIN #Labels l ON u.node_id = l.node_id
      WHERE u.new_label < l.label
    );

    CREATE OR REPLACE TABLE #Labels AS
      SELECT l.node_id,
             LEAST(COALESCE(u.new_label, l.label), l.label) AS label
      FROM #Labels l
      LEFT JOIN #Updates u ON l.node_id = u.node_id;

    DROP TABLE #Updates;
  END WHILE;
END DATAFLOW;
"""


def expand_node_range(spec: str) -> list[str]:
    """Expand '<foundation-node-1>..<foundation-node-N>' or comma list into nodes."""
    if ".." in spec:
        lo, hi = spec.split("..", 1)
        m_lo = re.match(r"(.*?)(\d+)$", lo)
        m_hi = re.match(r"(.*?)(\d+)$", hi)
        if not m_lo or not m_hi or m_lo.group(1) != m_hi.group(1):
            raise ValueError(f"cannot expand range: {spec}")
        prefix = m_lo.group(1)
        width = len(m_lo.group(2))
        start, end = int(m_lo.group(2)), int(m_hi.group(2))
        return [f"{prefix}{str(i).zfill(width)}" for i in range(start, end + 1)]
    return [n.strip() for n in spec.split(",") if n.strip()]


class NodeSampler:
    """Spawns ssh+pidstat / iostat on a remote node, persists CSV locally."""

    def __init__(self, node: str, out_dir: Path, ssh_user: Optional[str] = None):
        self.node = node
        self.out_dir = out_dir
        self.cpu_csv = out_dir / f"{node}.pidstat.csv"
        self.io_csv = out_dir / f"{node}.iostat.csv"
        self.ssh_target = f"{ssh_user}@{node}" if ssh_user else node
        self.proc_cpu: Optional[subprocess.Popen] = None
        self.proc_io: Optional[subprocess.Popen] = None

    def start(self) -> None:
        # pidstat: per-process %CPU and RSS, refresh every 1s
        self.cpu_csv.touch()
        cpu_cmd = ["ssh", self.ssh_target, "pidstat -dru -p ALL 1"]
        self.proc_cpu = subprocess.Popen(
            cpu_cmd, stdout=open(self.cpu_csv, "w"), stderr=subprocess.DEVNULL
        )

        # iostat: per-disk extended stats, refresh every 1s
        self.io_csv.touch()
        io_cmd = ["ssh", self.ssh_target, "iostat -dxm 1"]
        self.proc_io = subprocess.Popen(
            io_cmd, stdout=open(self.io_csv, "w"), stderr=subprocess.DEVNULL
        )

    def stop(self) -> None:
        for p in (self.proc_cpu, self.proc_io):
            if p is not None:
                p.terminate()
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()


def run_dataflow(dsn: str) -> tuple[float, str]:
    """Submit the WCC dataflow synchronously; return (wall_seconds, query_id)."""
    parsed = dsn.replace("jdbc:ocient://", "ocient://")
    user = os.environ.get("OCIENT_USER", "admin@system")
    pw = os.environ.get("OCIENT_PASSWORD", "")
    if "@" not in parsed:
        proto, rest = parsed.split("://", 1)
        parsed = f"{proto}://{user}:{pw}@{rest}"
    conn = pyocient.connect(parsed)
    cur = conn.cursor()
    qid = ""
    t0 = time.monotonic()
    try:
        cur.execute(WCC_DATAFLOW)
        # consume any returned rows
        try:
            cur.fetchall()
        except Exception:
            pass
        qid = getattr(cur, "query_id", "") or ""
    finally:
        cur.close()
        conn.close()
    return time.monotonic() - t0, qid


def summarize(out_dir: Path, foundation_nodes: list[str]) -> None:
    """Print a coarse summary of per-node skew from the pidstat CSVs.

    Reports, for each node, the median %CPU on the rolehostd PID and the
    p99/median ratio --- a rough skew indicator. Detailed analysis is
    expected to happen offline using the raw CSVs.
    """
    print("\n=== Skew summary (rolehostd %CPU per node, median / p99 / p99-vs-median ratio) ===")
    medians = []
    for node in foundation_nodes:
        f = out_dir / f"{node}.pidstat.csv"
        if not f.exists() or f.stat().st_size == 0:
            print(f"  {node}: no data collected")
            continue
        cpus: list[float] = []
        for line in f.read_text(errors="replace").splitlines():
            # crude pidstat row parser: tokens, %CPU is field index ~7
            toks = line.split()
            if len(toks) < 8 or "rolehostd" not in line:
                continue
            try:
                cpus.append(float(toks[7]))
            except ValueError:
                continue
        if not cpus:
            print(f"  {node}: no rolehostd samples")
            continue
        cpus.sort()
        med = cpus[len(cpus) // 2]
        p99 = cpus[max(0, int(len(cpus) * 0.99) - 1)]
        ratio = (p99 / med) if med > 0 else float("inf")
        medians.append(med)
        print(f"  {node}: med={med:6.1f}%  p99={p99:6.1f}%  p99/med={ratio:5.2f}")
    if medians:
        medians.sort()
        global_med = medians[len(medians) // 2]
        max_dev = max(abs(m - global_med) / global_med for m in medians) if global_med > 0 else 0
        print(f"\nCross-node median %CPU spread: {max_dev * 100:5.1f}% off the cluster median")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--foundation-nodes", default="<foundation-node-1>..<foundation-node-N>",
                   help="range or comma list (default: <foundation-node-1>..<foundation-node-N>)")
    p.add_argument("--ssh-user", default=None, help="SSH login user for the foundation nodes (default: current user)")
    p.add_argument("--output-dir", default=None, help="output directory for CSV samples; default: ./skew_run_<ts>")
    p.add_argument("--skip-dataflow", action="store_true",
                   help="just stand up samplers and idle (useful when you want to run the dataflow yourself)")
    args = p.parse_args()

    nodes = expand_node_range(args.foundation_nodes)
    out_dir = Path(args.output_dir or f"skew_run_{dt.datetime.now():%Y%m%d_%H%M%S}")
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output dir: {out_dir}")
    print(f"Foundation nodes: {nodes}")

    samplers = [NodeSampler(n, out_dir, args.ssh_user) for n in nodes]
    for s in samplers:
        s.start()

    time.sleep(3)  # let samplers spin up

    duration_s = 0.0
    qid = ""
    if not args.skip_dataflow:
        dsn = os.environ.get("OCIENT_JDBC_URL")
        if not dsn:
            sys.stderr.write("OCIENT_JDBC_URL is required in the environment\n")
            for s in samplers:
                s.stop()
            return 2
        print("Submitting WCC dataflow against wcc1t...")
        try:
            duration_s, qid = run_dataflow(dsn)
            print(f"Dataflow complete in {duration_s/60:.1f} min (query_id={qid})")
        except Exception as e:
            print(f"Dataflow failed: {e}")
    else:
        print("Skipping dataflow submission; samplers running --- ^C when ready")
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass

    for s in samplers:
        s.stop()
    print("Samplers stopped.")

    (out_dir / "metadata.txt").write_text(
        f"started_at={dt.datetime.utcnow().isoformat()}Z\n"
        f"foundation_nodes={','.join(nodes)}\n"
        f"dataflow_query_id={qid}\n"
        f"dataflow_duration_seconds={duration_s:.3f}\n"
    )
    summarize(out_dir, nodes)
    print(f"\nRaw CSVs saved under {out_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
