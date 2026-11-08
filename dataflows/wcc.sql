-- Weakly Connected Components (WCC) Dataflow
-- Reference implementation of the Weakly Connected Components case study from
-- "Virtualizing Recursion: JIT Graph Analytics in a Hyperscale Relational
-- Warehouse," VLDB 2026.
--
-- Section 8.3 of the paper. Label-propagation: every node initially
-- holds its own ID as its label, then in each iteration takes the
-- minimum label of its neighbors along either edge direction. Terminates when no labels change.
-- This is the algorithm measured at 100M, 1B, 10B, 100B, and 1T edges
-- in Section 9.6; the comparison harnesses in data/ implement the
-- same algorithm against DuckDB and Umbra.

BEGIN DATAFLOW
  DECLARE @changes INT = 1;

  -- Init: Every node's label is its own ID
  CREATE TABLE #Labels AS
    SELECT id as node_id, id as label FROM Nodes;

  WHILE (@changes > 0) DO
    -- 1. Propagate: min label over neighbors in either direction
    -- (edges are directed; WCC ignores direction)
    CREATE TABLE #Updates AS
      SELECT
        node_id,
        MIN(label) AS new_label
      FROM (
        SELECT e.src AS node_id, l.label
        FROM NetworkEdges e JOIN #Labels l ON e.dest = l.node_id
        UNION ALL
        SELECT e.dest AS node_id, l.label
        FROM NetworkEdges e JOIN #Labels l ON e.src = l.node_id
      ) AS nbr
      GROUP BY node_id;

    -- 2. Check Convergence against the previous round's labels
    -- (after step 3 every label is <= its update, so this must run first)
    SET @changes = (
      SELECT count(*) FROM #Updates u
      JOIN #Labels l ON u.node_id = l.node_id
      WHERE u.new_label < l.label);

    -- 3. Update: Apply strict improvement
    -- (Only update if neighbor's label is smaller)
    CREATE OR REPLACE TABLE #Labels AS
      SELECT
        l.node_id,
        LEAST(COALESCE(u.new_label, l.label), l.label) AS label
      FROM #Labels l
      LEFT JOIN #Updates u ON l.node_id = u.node_id;

    -- 4. Lifecycle Management
    DROP TABLE #Updates;
  END WHILE;
END DATAFLOW;
