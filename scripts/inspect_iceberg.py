"""Verify actual table snapshots and time-travel reads for a successful run."""
import argparse
import json
from pathlib import Path
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--run', type=Path, required=True)
parser.add_argument('--warehouse', required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
manifest = json.loads((args.run / 'manifest.json').read_text(encoding='utf-8'))
if manifest['format'] != 'iceberg' or manifest['status'] != 'success' or not (args.run / '_SUCCESS').exists():
    raise ValueError('Expected a published, successful Iceberg run')
spark = (SparkSession.builder.appName('inspect-iceberg-snapshots').master('local[2]')
         .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
         .config('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog')
         .config('spark.sql.catalog.local.type', 'hadoop')
         .config('spark.sql.catalog.local.warehouse', args.warehouse).getOrCreate())
spark.sparkContext.setLogLevel('WARN')
try:
    tables = {}
    for name, entry in manifest['tables'].items():
        table = entry['table']
        snapshots = spark.sql(f'SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at').collect()
        snapshot_id = entry['snapshot_id']
        if snapshot_id not in [int(row.snapshot_id) for row in snapshots]:
            raise AssertionError(f'Manifest snapshot not found for {table}')
        current_count = spark.table(table).count()
        version_count = spark.sql(f'SELECT COUNT(*) AS n FROM {table} VERSION AS OF {snapshot_id}').first().n
        oldest_id = int(snapshots[0].snapshot_id)
        oldest_count = spark.sql(f'SELECT COUNT(*) AS n FROM {table} VERSION AS OF {oldest_id}').first().n
        if current_count != version_count:
            raise AssertionError('Published snapshot and current table counts differ')
        tables[name] = {'table': table, 'snapshot_count': len(snapshots), 'manifest_snapshot': snapshot_id,
                        'current_rows': current_count, 'manifest_snapshot_rows': version_count,
                        'oldest_snapshot': oldest_id, 'oldest_snapshot_rows': oldest_count}
    report = {'status': 'PASS', 'tables': tables,
              'scope': 'Actual Iceberg snapshot metadata and VERSION AS OF SQL reads; no cross-table transaction claimed'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))
finally:
    spark.stop()
