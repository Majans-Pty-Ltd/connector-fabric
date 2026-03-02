"""Explore the SCANv2 dataset model structure."""

import os
import sys
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

ADOMD_DLL_PATH = os.path.join(PROJECT_ROOT, "adomd_package", "lib", "net45")
if os.path.isdir(ADOMD_DLL_PATH):
    sys.path.insert(0, ADOMD_DLL_PATH)
    os.environ["PATH"] = ADOMD_DLL_PATH + os.pathsep + os.environ.get("PATH", "")

import clr  # noqa: E402

dll_path = os.path.join(ADOMD_DLL_PATH, "Microsoft.AnalysisServices.AdomdClient.dll")
clr.AddReference(dll_path)

from pyadomd import Pyadomd  # noqa: E402

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
XMLA_ENDPOINT = os.getenv("PBI_XMLA_ENDPOINT")
DATASET_NAME = os.getenv("PBI_DATASET_NAME")

conn_str = (
    f"Provider=MSOLAP;"
    f"Data Source={XMLA_ENDPOINT};"
    f"Initial Catalog={DATASET_NAME};"
    f"User ID=app:{CLIENT_ID}@{TENANT_ID};"
    f"Password={CLIENT_SECRET};"
    f"Persist Security Info=True;"
    f"Impersonation Level=Impersonate;"
)

conn = Pyadomd(conn_str)
conn.open()

# Query 1: Get all columns per table
print("=== TABLES AND COLUMNS ===\n")
cur1 = conn.cursor()
cur1.execute("""
    SELECT [TABLE_NAME], [COLUMN_NAME], [DATA_TYPE], [DESCRIPTION]
    FROM $SYSTEM.DBSCHEMA_COLUMNS
    WHERE LEFT([TABLE_NAME], 1) = '$'
""")
rows = cur1.fetchall()
current_table = None
for r in rows:
    table, col, dtype, desc = r[0], r[1], r[2], r[3] if len(r) > 3 else ""
    if table != current_table:
        current_table = table
        print(f"\n  [{table}]")
    desc_str = f"  -- {desc}" if desc else ""
    print(f"    {col} ({dtype}){desc_str}")
cur1.close()

# Query 2: Get all measures
print("\n\n=== MEASURES ===\n")
cur2 = conn.cursor()
cur2.execute("""
    SELECT [MEASUREGROUP_NAME], [MEASURE_NAME], [DEFAULT_FORMAT_STRING], [DESCRIPTION]
    FROM $SYSTEM.MDSCHEMA_MEASURES
    WHERE [MEASURE_IS_VISIBLE]
""")
measures = cur2.fetchall()
for m in measures:
    group = m[0] if m[0] else "(no group)"
    name = m[1]
    fmt = m[2] if m[2] else ""
    desc = m[3] if len(m) > 3 and m[3] else ""
    fmt_str = f"  [{fmt}]" if fmt else ""
    desc_str = f"  -- {desc}" if desc else ""
    print(f"  {group} / {name}{fmt_str}{desc_str}")
cur2.close()

# Query 3: Get relationships
print("\n\n=== RELATIONSHIPS ===\n")
cur3 = conn.cursor()
cur3.execute("""
    SELECT [DIMENSION_UNIQUE_NAME], [MEASUREGROUP_NAME]
    FROM $SYSTEM.MDSCHEMA_MEASUREGROUP_DIMENSIONS
""")
rels = cur3.fetchall()
for r in rels:
    print(f"  {r[1]} -> {r[0]}")
cur3.close()

conn.close()
print("\nDone.")
