-- Replace the complete table identifier using tables.<name>.table in manifest.json.
-- Run with the same local HadoopCatalog and warehouse configuration as the pipeline.
SELECT user, method, candidate, score, ranking_score, rank
FROM local.friend_recommendation.REPLACE_WITH_RUN_RECOMMENDATIONS_TABLE
WHERE user = 0
ORDER BY method, rank;

-- Snapshot metadata is available only for Iceberg, not the default Parquet files.
SELECT snapshot_id, committed_at, operation, summary
FROM local.friend_recommendation.REPLACE_WITH_RUN_RECOMMENDATIONS_TABLE.snapshots
ORDER BY committed_at DESC;

-- Use the exact recorded snapshot_id to inspect the committed version.
SELECT COUNT(*)
FROM local.friend_recommendation.REPLACE_WITH_RUN_RECOMMENDATIONS_TABLE
VERSION AS OF 1234567890123456789;
