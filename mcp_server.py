"""
Power BI XMLA MCP Server for Claude Code.

Provides DAX query execution and model exploration against multiple
Majans Power BI workspaces via XMLA endpoints.

Requires: pip install mcp pyadomd python-dotenv pythonnet
"""

import os
import sys

# --- CLR INITIALIZATION (must happen before pyadomd import) ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ADOMD_DLL_PATH = os.path.join(SCRIPT_DIR, "adomd_package", "lib", "net45")
if os.path.isdir(ADOMD_DLL_PATH):
    sys.path.insert(0, ADOMD_DLL_PATH)
    os.environ["PATH"] = ADOMD_DLL_PATH + os.pathsep + os.environ.get("PATH", "")

import clr  # noqa: E402

clr.AddReference(
    os.path.join(ADOMD_DLL_PATH, "Microsoft.AnalysisServices.AdomdClient.dll")
)

from pyadomd import Pyadomd  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from mcp.server.fastmcp import FastMCP  # noqa: E402

# --- CONFIGURATION ---
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

# Workspace registry: short name -> (XMLA endpoint, default dataset)
WORKSPACES = {
    "SCAN": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/DEMAND",
        "dataset": "SCANv2",
        "description": "POS scan data model (SCANv2) — retail scan data, Coles/Woolworths, Bhuja/Infuzions",
    },
    "REVIEW": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/REVIEW",
        "dataset": "FINANCIALv2",
        "description": "Financial P&L model (FINANCIALv2) — budgets, forecasts, actuals, GL",
    },
    "SUPPLY": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/SUPPLY",
        "dataset": "MANUFACTURING V3",
        "description": "Manufacturing & supply chain model (MANUFACTURING V3)",
    },
    "DEMAND": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/DEMAND",
        "dataset": "SALESv2",
        "description": "Sales & demand model (SALESv2) — customer orders, invoicing, demand",
    },
    "IT_COST": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/IT COST",
        "dataset": "IT COST",
        "description": "IT cost management model — M365/D365 licenses, Azure spend, CC transactions, FY26 budget vs actual, savings opportunities",
    },
}

DEFAULT_WORKSPACE = "SCAN"


def _build_conn_str(workspace: str) -> str:
    """Build an XMLA connection string for the given workspace."""
    ws = WORKSPACES.get(workspace.upper())
    if not ws:
        available = ", ".join(WORKSPACES.keys())
        raise ValueError(f"Unknown workspace '{workspace}'. Available: {available}")
    if not ws["dataset"]:
        raise ValueError(
            f"Workspace '{workspace}' has no dataset configured. "
            f"Run pbi_test_connection with this workspace first to discover datasets."
        )
    return (
        f"Provider=MSOLAP;"
        f"Data Source={ws['endpoint']};"
        f"Initial Catalog={ws['dataset']};"
        f"User ID=app:{CLIENT_ID}@{TENANT_ID};"
        f"Password={CLIENT_SECRET};"
        f"Persist Security Info=True;"
        f"Impersonation Level=Impersonate;"
    )


def _build_conn_str_no_catalog(workspace: str) -> str:
    """Build connection string without Initial Catalog (for discovery)."""
    ws = WORKSPACES.get(workspace.upper())
    if not ws:
        available = ", ".join(WORKSPACES.keys())
        raise ValueError(f"Unknown workspace '{workspace}'. Available: {available}")
    return (
        f"Provider=MSOLAP;"
        f"Data Source={ws['endpoint']};"
        f"User ID=app:{CLIENT_ID}@{TENANT_ID};"
        f"Password={CLIENT_SECRET};"
        f"Persist Security Info=True;"
        f"Impersonation Level=Impersonate;"
    )


# --- MCP SERVER ---
mcp = FastMCP(
    "power-bi-xmla",
    instructions=(
        "Power BI XMLA endpoint for querying Majans' semantic models across multiple workspaces. "
        "Available workspaces: SCAN (SCANv2 — POS retail scan data, default), "
        "REVIEW (FINANCIALv2 — P&L, budgets, forecasts), "
        "SUPPLY (MANUFACTURING V3 — production, supply chain), DEMAND (SALESv2 — sales/demand). "
        "Default workspace is SCAN. Use the 'workspace' parameter to switch. "
        "Run pbi_list_tables first to discover available tables and columns, "
        "then pbi_list_measures to see defined measures."
    ),
)


def _execute(query: str, workspace: str = DEFAULT_WORKSPACE) -> tuple:
    """Execute a query and return (headers, rows). New connection per query."""
    conn_str = _build_conn_str(workspace)
    conn = Pyadomd(conn_str)
    conn.open()
    try:
        cur = conn.cursor()
        cur.execute(query)
        headers = [col[0] for col in cur.description] if cur.description else []
        rows = cur.fetchall()
        cur.close()
        return headers, rows
    finally:
        conn.close()


def _to_markdown_table(headers: list, rows: list, max_rows: int = 100) -> str:
    """Format query results as a markdown table."""
    if not headers:
        return "No results returned."

    def fmt(v):
        if v is None:
            return ""
        if isinstance(v, float):
            if v == int(v):
                return str(int(v))
            return f"{v:,.2f}"
        return str(v)

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")

    total = len(rows)
    display_rows = rows[:max_rows]
    for row in display_rows:
        lines.append("| " + " | ".join(fmt(v) for v in row) + " |")

    if total > max_rows:
        lines.append(
            f"\n*Showing {max_rows} of {total} rows. Use max_rows parameter to see more.*"
        )
    else:
        lines.append(f"\n*{total} row(s)*")

    return "\n".join(lines)


@mcp.tool()
def pbi_query(
    query: str, max_rows: int = 100, workspace: str = DEFAULT_WORKSPACE
) -> str:
    """Execute a DAX query against a Power BI semantic model.

    Args:
        query: DAX query string (use EVALUATE for tabular results).
        max_rows: Maximum rows to return (default 100).
        workspace: Workspace to query — REVIEW (default, FINANCIALv2), SUPPLY (MANUFACTURING V3), or DEMAND.

    Run pbi_list_tables first to discover available tables and columns.

    Example queries:
        EVALUATE ROW("Test", 1)
        EVALUATE SUMMARIZECOLUMNS('Table'[Column], "Metric", SUM('Table'[Value]))
    """
    try:
        headers, rows = _execute(query, workspace)
        return _to_markdown_table(headers, rows, max_rows)
    except Exception as e:
        return f"DAX query error ({workspace}): {e}\n\nQuery was:\n```\n{query}\n```"


@mcp.tool()
def pbi_list_tables(workspace: str = DEFAULT_WORKSPACE) -> str:
    """List all tables and columns in a Power BI semantic model with data types.

    Args:
        workspace: REVIEW (default, FINANCIALv2), SUPPLY (MANUFACTURING V3), or DEMAND.
    """
    try:
        ws = WORKSPACES[workspace.upper()]
        query = """
        SELECT
            [TABLE_NAME],
            [COLUMN_NAME],
            [DATA_TYPE],
            [DESCRIPTION]
        FROM $SYSTEM.DBSCHEMA_COLUMNS
        WHERE LEFT([TABLE_NAME], 1) <> '$'
        ORDER BY [TABLE_NAME]
        """
        headers, rows = _execute(query, workspace)

        tables = {}
        for row in rows:
            tbl = row[0]
            if tbl not in tables:
                tables[tbl] = []
            tables[tbl].append(row)

        lines = [f"## {ws['dataset']} Model Structure ({workspace})\n"]
        for tbl, cols in sorted(tables.items()):
            lines.append(f"### {tbl}")
            lines.append("| Column | Data Type | Description |")
            lines.append("| --- | --- | --- |")
            for row in cols:
                desc = row[3] if row[3] else ""
                lines.append(f"| {row[1]} | {row[2]} | {desc} |")
            lines.append("")

        return "\n".join(lines)
    except Exception as e:
        return f"Error listing tables ({workspace}): {e}"


@mcp.tool()
def pbi_list_measures(workspace: str = DEFAULT_WORKSPACE) -> str:
    """List all measures defined in a Power BI semantic model.

    Args:
        workspace: REVIEW (default, FINANCIALv2), SUPPLY (MANUFACTURING V3), or DEMAND.
    """
    try:
        ws = WORKSPACES[workspace.upper()]
        query = """
        SELECT
            [MEASUREGROUP_NAME],
            [MEASURE_NAME],
            [DEFAULT_FORMAT_STRING],
            [DESCRIPTION]
        FROM $SYSTEM.MDSCHEMA_MEASURES
        WHERE [MEASURE_IS_VISIBLE]
        """
        headers, rows = _execute(query, workspace)

        lines = [f"## {ws['dataset']} Measures ({workspace})\n"]
        lines.append("| Measure Group | Measure | Format | Description |")
        lines.append("| --- | --- | --- | --- |")
        for row in rows:
            fmt_str = row[2] if row[2] else ""
            desc = row[3] if row[3] else ""
            lines.append(f"| {row[0]} | {row[1]} | `{fmt_str}` | {desc} |")

        return "\n".join(lines)
    except Exception as e:
        return f"Error listing measures ({workspace}): {e}"


@mcp.tool()
def pbi_test_connection(workspace: str = DEFAULT_WORKSPACE) -> str:
    """Test XMLA connection to a Power BI workspace and discover datasets.

    Args:
        workspace: REVIEW (default), SUPPLY, or DEMAND.

    Use this to verify connectivity and discover dataset names in a workspace.
    """
    ws_key = workspace.upper()
    ws = WORKSPACES.get(ws_key)
    if not ws:
        available = ", ".join(WORKSPACES.keys())
        return f"Unknown workspace '{workspace}'. Available: {available}"

    result = [f"Workspace: {ws_key}", f"Endpoint: {ws['endpoint']}"]

    # If dataset is configured, test full connection
    if ws["dataset"]:
        try:
            headers, rows = _execute('EVALUATE ROW("Status", "Connected")', workspace)
            result.insert(0, "Connection: OK")
            result.append(f"Dataset: {ws['dataset']}")

            headers2, rows2 = _execute(
                """
                SELECT [TABLE_NAME]
                FROM $SYSTEM.DBSCHEMA_TABLES
                WHERE [TABLE_TYPE] = 'TABLE'
            """,
                workspace,
            )
            result.append(f"\nTables ({len(rows2)}):")
            for row in rows2:
                result.append(f"  - {row[0]}")

            return "\n".join(result)
        except Exception as e:
            result.insert(0, "Connection FAILED")
            result.append(f"Dataset: {ws['dataset']}")
            result.append(f"Error: {e}")
            return "\n".join(result)

    # No dataset configured — try to discover datasets via catalog query
    result.append("Dataset: Not configured — attempting discovery...")
    try:
        conn_str = _build_conn_str_no_catalog(workspace)
        conn = Pyadomd(conn_str)
        conn.open()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT [CATALOG_NAME]
                FROM $SYSTEM.DBSCHEMA_CATALOGS
            """)
            rows = cur.fetchall()
            cur.close()

            if rows:
                result.append(f"\nDiscovered datasets ({len(rows)}):")
                for row in rows:
                    result.append(f"  - {row[0]}")
                # Auto-configure the first dataset found
                first_dataset = rows[0][0]
                WORKSPACES[ws_key]["dataset"] = first_dataset
                result.append(f"\nAuto-configured dataset: {first_dataset}")
            else:
                result.append("\nNo datasets found in workspace.")
        finally:
            conn.close()

        return "\n".join(result)
    except Exception as e:
        result.append(f"Discovery FAILED: {e}")
        return "\n".join(result)


@mcp.tool()
def pbi_list_workspaces() -> str:
    """List all configured Power BI workspaces and their connection status."""
    lines = ["## Configured Workspaces\n"]
    lines.append("| Workspace | Dataset | Description |")
    lines.append("| --- | --- | --- |")
    for name, ws in WORKSPACES.items():
        dataset = ws["dataset"] or "(not configured)"
        lines.append(f"| {name} | {dataset} | {ws['description']} |")
    return "\n".join(lines)


@mcp.tool()
def pbi_discover_workspaces() -> str:
    """Discover all Power BI workspaces the service principal can access.

    Uses the Power BI REST API to list workspaces, then for each workspace
    lists the datasets available. Use this to find the correct workspace name
    for a dataset.
    """
    import requests

    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
    }

    try:
        resp = requests.post(token_url, data=token_data, timeout=30)
        if resp.status_code != 200:
            return f"Auth failed ({resp.status_code}): {resp.json().get('error_description', resp.text)}"
        token = resp.json()["access_token"]
    except Exception as e:
        return f"Auth error: {e}"

    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(
            "https://api.powerbi.com/v1.0/myorg/groups", headers=headers, timeout=30
        )
        if resp.status_code != 200:
            return f"REST API error ({resp.status_code}): {resp.text}"
        workspaces = resp.json().get("value", [])
    except Exception as e:
        return f"REST API error: {e}"

    lines = [f"## Power BI Workspaces ({len(workspaces)} found)\n"]
    for ws in workspaces:
        ws_id = ws["id"]
        ws_name = ws["name"]
        lines.append(f"### {ws_name}")
        lines.append(f"- ID: `{ws_id}`")
        lines.append(
            f"- XMLA endpoint: `powerbi://api.powerbi.com/v1.0/myorg/{ws_name}`"
        )

        # List datasets in this workspace
        try:
            ds_resp = requests.get(
                f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/datasets",
                headers=headers,
                timeout=30,
            )
            if ds_resp.status_code == 200:
                datasets = ds_resp.json().get("value", [])
                if datasets:
                    lines.append(f"- Datasets ({len(datasets)}):")
                    for ds in datasets:
                        lines.append(f"  - **{ds['name']}** (ID: {ds['id']})")
                else:
                    lines.append("- Datasets: (none)")
            else:
                lines.append(f"- Datasets: error ({ds_resp.status_code})")
        except Exception:
            lines.append("- Datasets: error fetching")

        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    mcp.run()
