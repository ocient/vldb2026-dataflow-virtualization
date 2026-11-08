#!/usr/bin/env python3
"""D2 v6: rename pattern + defensive DROP IF EXISTS to clear phantom
virtualized handles from the previous iteration."""
import datetime as dt
import json
import os
import sys
import time

import pyocient

SCALE = sys.argv[1] if len(sys.argv) > 1 else "wcc100b"
OUT_DIR = os.path.expanduser("~/vldb_experiments")
os.makedirs(OUT_DIR, exist_ok=True)
RESULTS = os.path.join(OUT_DIR, f"d2_v6_results_{SCALE}.json")

dsn = "ocient://admin@system:admin@olap-sql01.corp.ocient.com:4050/tpc"
print(f"[d2v6] connecting (scale={SCALE})")

conn = pyocient.connect(dsn)
cur = conn.cursor()

WCC_DATAFLOW = f"""
BEGIN DATAFLOW
  DECLARE @changes BIGINT = 1;

  CREATE TABLE #Labels AS
    SELECT id AS node_id, id AS label FROM {SCALE}.bench_vertices;

  WHILE (@changes > 0) DO
    DROP TABLE IF EXISTS #LabelsNew;
    -- labels flow along both edge directions (edges are directed, WCC is not)
    CREATE TABLE #Updates AS
      SELECT node_id, MIN(label) AS new_label
      FROM (
        SELECT e.srcid AS node_id, l.label
        FROM {SCALE}.bench_edges e JOIN #Labels l ON e.destid = l.node_id
        UNION ALL
        SELECT e.destid AS node_id, l.label
        FROM {SCALE}.bench_edges e JOIN #Labels l ON e.srcid = l.node_id
      ) AS nbr
      GROUP BY node_id;

    SET @changes = (
      SELECT count(*) FROM #Updates u
      JOIN #Labels l ON u.node_id = l.node_id
      WHERE u.new_label < l.label
    );

    CREATE TABLE #LabelsNew AS
      SELECT l.node_id,
             LEAST(COALESCE(u.new_label, l.label), l.label) AS label
      FROM #Labels l
      LEFT JOIN #Updates u ON l.node_id = u.node_id;

    DROP TABLE #Labels;
    ALTER TABLE #LabelsNew RENAME TO #Labels;
    DROP TABLE #Updates;
  END WHILE;
END DATAFLOW;
"""

start = dt.datetime.utcnow()
results = {"scale": SCALE, "started_at_utc": start.isoformat() + "Z", "wcc_sql": WCC_DATAFLOW.strip()}

print(f"[d2v6] running WCC dataflow on {SCALE}...")
t0 = time.monotonic()
try:
    cur.execute(WCC_DATAFLOW)
    try:
        cur.fetchall()
    except Exception:
        pass
except Exception as e:
    results["dataflow_error"] = repr(e)
    print(f"[d2v6] DATAFLOW FAILED: {e}")
wall = time.monotonic() - t0
end = dt.datetime.utcnow()
results["dataflow_wall_seconds"] = wall
results["finished_at_utc"] = end.isoformat() + "Z"
print(f"[d2v6] dataflow complete in {wall/60:.2f} min")

print("[d2v6] querying sys.completed_queries...")
start_str = start.strftime("%Y-%m-%d %H:%M:%S")
end_str = end.strftime("%Y-%m-%d %H:%M:%S")
q = f"""
SELECT query_id, sql, total_time, execution_time, queue_time,
       timestamp_start, timestamp_complete,
       node_id AS routed_through_node, participating_nodes,
       rows_returned, rows_inserted,
       approx_system_peak_vm_node_huge_mem_bytes,
       approx_system_peak_vm_cluster_huge_mem_bytes,
       temp_disk_consumed,
       concurrency_service_class_name, code, state, reason
FROM sys.completed_queries
WHERE timestamp_start >= TIMESTAMP '{start_str}'
  AND timestamp_start <= TIMESTAMP '{end_str}'
ORDER BY timestamp_start
"""
try:
    cur.execute(q)
    cols = [d[0] for d in cur.description]
    rows = []
    for r in cur.fetchall():
        row = {}
        for c, v in zip(cols, r):
            if isinstance(v, (dt.datetime, dt.date)):
                row[c] = v.isoformat()
            elif isinstance(v, (list, tuple)):
                row[c] = [str(x) for x in v]
            elif v is None or isinstance(v, (int, float, str, bool)):
                row[c] = v
            else:
                row[c] = str(v)
        rows.append(row)
    results["completed_queries"] = rows
    print(f"[d2v6] {len(rows)} child queries captured")
except Exception as e:
    results["completed_queries_error"] = repr(e)

with open(RESULTS, "w") as f:
    json.dump(results, f, indent=2, default=str)
print(f"[d2v6] wrote -> {RESULTS}")

cur.close()
conn.close()
