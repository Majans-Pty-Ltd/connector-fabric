"""Example DAX queries against the SCANv2 dataset."""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ADOMD_DLL_PATH = os.path.join(SCRIPT_DIR, "adomd_package", "lib", "net45")
if os.path.isdir(ADOMD_DLL_PATH):
    sys.path.insert(0, ADOMD_DLL_PATH)
    os.environ["PATH"] = ADOMD_DLL_PATH + os.pathsep + os.environ.get("PATH", "")

import clr
clr.AddReference(os.path.join(ADOMD_DLL_PATH, "Microsoft.AnalysisServices.AdomdClient.dll"))
from pyadomd import Pyadomd

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


def run_query(conn, name, dax):
    """Run a DAX query and print results."""
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")
    cur = conn.cursor()
    cur.execute(dax)
    cols = [col[0] for col in cur.description]
    rows = cur.fetchall()
    # Print header
    print("  " + " | ".join(cols))
    print("  " + "-" * (sum(len(c) + 3 for c in cols)))
    # Print rows (limit to 20)
    for i, row in enumerate(rows):
        if i >= 20:
            print(f"  ... ({len(rows) - 20} more rows)")
            break
        print("  " + " | ".join(str(v) for v in row))
    print(f"  ({len(rows)} total rows)")
    cur.close()


conn = Pyadomd(conn_str)
conn.open()

# 1. Total scan sales and units by customer
run_query(conn, "Sales & Units by Customer", """
EVALUATE
SUMMARIZECOLUMNS(
    'ITEMS'[CUSTOMER],
    "TotalSales", SUM('SCAN'[SALES]),
    "TotalUnits", SUM('SCAN'[UNITS])
)
""")

# 2. Top 10 products by scan units
run_query(conn, "Top 10 Products by Scan Units", """
EVALUATE
TOPN(
    10,
    SUMMARIZECOLUMNS(
        'ITEMS'[PRODUCT],
        'ITEMS'[EAN],
        "TotalUnits", SUM('SCAN'[UNITS]),
        "TotalSales", SUM('SCAN'[SALES]),
        "AvgPrice", AVERAGE('SCAN'[PRICE])
    ),
    [TotalUnits], DESC
)
""")

# 3. Scan data by fiscal year
run_query(conn, "Sales by Fiscal Year", """
EVALUATE
SUMMARIZECOLUMNS(
    'DATE'[FISCALYEAR],
    "TotalSales", SUM('SCAN'[SALES]),
    "TotalUnits", SUM('SCAN'[UNITS])
)
""")

# 4. Category breakdown
run_query(conn, "Sales by Category", """
EVALUATE
SUMMARIZECOLUMNS(
    'ITEMS'[CATEGORY],
    "TotalSales", SUM('SCAN'[SALES]),
    "TotalUnits", SUM('SCAN'[UNITS]),
    "SKUCount", DISTINCTCOUNT('ITEMS'[EAN])
)
""")

# 5. Demand units by customer
run_query(conn, "Demand Units by Customer", """
EVALUATE
SUMMARIZECOLUMNS(
    'CUSTOMER'[CUSTOMER],
    "DemandUnits", SUM('DEMAND'[UNITS])
)
""")

conn.close()
print("\nDone.")
