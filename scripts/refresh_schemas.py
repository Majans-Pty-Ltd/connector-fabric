"""
Refresh static schema snapshots for Fabric semantic models.

Connects to each dataset via XMLA, queries $SYSTEM.DBSCHEMA_COLUMNS and
$SYSTEM.MDSCHEMA_MEASURES, and writes JSON to schemas/<dataset>.json.

Usage:
    python scripts/refresh_schemas.py              # refresh all 15 datasets
    python scripts/refresh_schemas.py SCANv2       # refresh single dataset
    python scripts/refresh_schemas.py SCANv2 FINANCIALv2  # refresh multiple
"""

import json
import os
import sys
from datetime import datetime, timezone

# Add parent dir so we can import from mcp_server
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_server import (
    SCHEMAS_DIR,
    _DATASET_INDEX,
    _execute,
    _resolve_dataset,
)


def refresh_dataset(dataset_name: str) -> dict:
    """Query XMLA for a dataset's schema and return the schema dict."""
    info = _resolve_dataset(dataset_name)
    ds_name = info["dataset"]
    print(f"  Querying {ds_name} ({info['workspace']})...")

    # Query columns
    col_query = """
    SELECT
        [TABLE_NAME],
        [COLUMN_NAME],
        [DATA_TYPE],
        [DESCRIPTION]
    FROM $SYSTEM.DBSCHEMA_COLUMNS
    WHERE LEFT([TABLE_NAME], 1) <> '$'
    ORDER BY [TABLE_NAME]
    """
    _, col_rows = _execute(col_query, dataset_name)

    tables = {}
    for row in col_rows:
        tbl = row[0]
        if tbl not in tables:
            tables[tbl] = []
        tables[tbl].append(
            {
                "name": row[1],
                "data_type": str(row[2]) if row[2] else "",
                "description": str(row[3]) if row[3] else "",
            }
        )

    # Query measures
    meas_query = """
    SELECT
        [MEASUREGROUP_NAME],
        [MEASURE_NAME],
        [DEFAULT_FORMAT_STRING],
        [DESCRIPTION]
    FROM $SYSTEM.MDSCHEMA_MEASURES
    WHERE [MEASURE_IS_VISIBLE]
    """
    _, meas_rows = _execute(meas_query, dataset_name)

    measures = []
    for row in meas_rows:
        measures.append(
            {
                "table": str(row[0]) if row[0] else "",
                "name": str(row[1]) if row[1] else "",
                "format_string": str(row[2]) if row[2] else "",
                "description": str(row[3]) if row[3] else "",
            }
        )

    schema = {
        "dataset": ds_name,
        "workspace": info["workspace"],
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "tables": [
            {"name": tbl, "columns": cols} for tbl, cols in sorted(tables.items())
        ],
        "measures": measures,
    }

    print(f"    {len(tables)} tables, {len(measures)} measures")
    return schema


def save_schema(schema: dict):
    """Write schema dict to schemas/<dataset>.json."""
    os.makedirs(SCHEMAS_DIR, exist_ok=True)
    path = os.path.join(SCHEMAS_DIR, f"{schema['dataset']}.json")
    with open(path, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"    Saved to {path}")


def main():
    args = sys.argv[1:]

    if args:
        # Refresh specific datasets
        datasets = args
    else:
        # Refresh all datasets
        datasets = list(_DATASET_INDEX.keys())

    print(f"Refreshing {len(datasets)} dataset(s)...\n")

    succeeded = 0
    failed = 0

    for ds in datasets:
        try:
            schema = refresh_dataset(ds)
            save_schema(schema)
            succeeded += 1
        except Exception as e:
            print(f"  FAILED: {ds} — {e}")
            failed += 1

    print(f"\nDone: {succeeded} succeeded, {failed} failed")


if __name__ == "__main__":
    main()
