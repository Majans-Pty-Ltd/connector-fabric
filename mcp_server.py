"""
Microsoft Fabric MCP Server for Claude Code.

Provides DAX query execution, model exploration, and Fabric REST API operations
against Majans' Microsoft Fabric workspaces — semantic models, pipelines,
lakehouses, and more.

Requires: pip install mcp python-dotenv requests
XMLA tools also require: pip install pyadomd pythonnet (Windows only)
"""

import os
import time

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
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

# --- LAZY XMLA INITIALIZATION ---
_clr_initialized = False
_Pyadomd = None


def _ensure_xmla():
    """Lazy-load ADOMD.NET and pyadomd. Only needed for XMLA tools (Windows-only)."""
    global _clr_initialized, _Pyadomd
    if _clr_initialized:
        return

    import sys

    adomd_dll_path = os.path.join(SCRIPT_DIR, "adomd_package", "lib", "net45")
    if not os.path.isdir(adomd_dll_path):
        raise RuntimeError(
            f"ADOMD.NET DLL directory not found: {adomd_dll_path}. "
            "XMLA tools require Windows with the bundled ADOMD.NET package."
        )

    sys.path.insert(0, adomd_dll_path)
    os.environ["PATH"] = adomd_dll_path + os.pathsep + os.environ.get("PATH", "")

    import clr

    clr.AddReference(
        os.path.join(adomd_dll_path, "Microsoft.AnalysisServices.AdomdClient.dll")
    )

    from pyadomd import Pyadomd

    _Pyadomd = Pyadomd
    _clr_initialized = True


# --- FABRIC REST API AUTH ---
_token_cache = {"token": None, "expires_at": 0}


def _get_fabric_token() -> str:
    """Get a cached Fabric REST API token. Refreshes when expired."""
    import requests

    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["token"]

    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 3600)
    return _token_cache["token"]


# --- XMLA HELPERS ---


def _build_conn_str(workspace: str) -> str:
    """Build an XMLA connection string for the given workspace."""
    ws = WORKSPACES.get(workspace.upper())
    if not ws:
        available = ", ".join(WORKSPACES.keys())
        raise ValueError(f"Unknown workspace '{workspace}'. Available: {available}")
    if not ws["dataset"]:
        raise ValueError(
            f"Workspace '{workspace}' has no dataset configured. "
            f"Run fabric_test_xmla with this workspace first to discover datasets."
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


def _execute(query: str, workspace: str = DEFAULT_WORKSPACE) -> tuple:
    """Execute a query and return (headers, rows). New connection per query."""
    _ensure_xmla()
    conn_str = _build_conn_str(workspace)
    conn = _Pyadomd(conn_str)
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


# --- MCP SERVER ---
mcp = FastMCP(
    "fabric",
    instructions=(
        "Microsoft Fabric MCP server for Majans — provides access to semantic models (DAX queries via XMLA), "
        "workspace management, pipeline operations, and dataset refresh via the Fabric REST API. "
        "Available XMLA workspaces: SCAN (SCANv2 — POS retail scan data, default), "
        "REVIEW (FINANCIALv2 — P&L, budgets, forecasts), "
        "SUPPLY (MANUFACTURING V3 — production, supply chain), "
        "DEMAND (SALESv2 — sales/demand), IT_COST (IT spend). "
        "Default workspace is SCAN. Use the 'workspace' parameter to switch. "
        "Run fabric_list_tables first to discover available tables and columns, "
        "then fabric_list_measures to see defined measures. "
        "Use fabric_list_workspace_items to explore all Fabric artefacts in a workspace."
    ),
)


# ===== XMLA TOOLS =====


@mcp.tool()
def fabric_dax_query(
    query: str, max_rows: int = 100, workspace: str = DEFAULT_WORKSPACE
) -> str:
    """Execute a DAX query against a Power BI semantic model via XMLA.

    Args:
        query: DAX query string (use EVALUATE for tabular results).
        max_rows: Maximum rows to return (default 100).
        workspace: Workspace to query — SCAN (default), REVIEW, SUPPLY, DEMAND, IT_COST.

    Run fabric_list_tables first to discover available tables and columns.

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
def fabric_list_tables(workspace: str = DEFAULT_WORKSPACE) -> str:
    """List all tables and columns in a semantic model with data types.

    Args:
        workspace: SCAN (default), REVIEW, SUPPLY, DEMAND, IT_COST.
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
def fabric_list_measures(workspace: str = DEFAULT_WORKSPACE) -> str:
    """List all measures defined in a semantic model.

    Args:
        workspace: SCAN (default), REVIEW, SUPPLY, DEMAND, IT_COST.
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
def fabric_test_xmla(workspace: str = DEFAULT_WORKSPACE) -> str:
    """Test XMLA connection to a workspace and discover datasets.

    Args:
        workspace: SCAN (default), REVIEW, SUPPLY, DEMAND, IT_COST.

    Use this to verify XMLA connectivity and discover dataset names.
    """
    ws_key = workspace.upper()
    ws = WORKSPACES.get(ws_key)
    if not ws:
        available = ", ".join(WORKSPACES.keys())
        return f"Unknown workspace '{workspace}'. Available: {available}"

    result = [f"Workspace: {ws_key}", f"Endpoint: {ws['endpoint']}"]

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

    result.append("Dataset: Not configured — attempting discovery...")
    try:
        _ensure_xmla()
        conn_str = _build_conn_str_no_catalog(workspace)
        conn = _Pyadomd(conn_str)
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


# ===== WORKSPACE & DISCOVERY TOOLS =====


@mcp.tool()
def fabric_list_configured_workspaces() -> str:
    """List all configured Fabric workspaces and their XMLA connection details."""
    lines = ["## Configured Workspaces\n"]
    lines.append("| Workspace | Dataset | Description |")
    lines.append("| --- | --- | --- |")
    for name, ws in WORKSPACES.items():
        dataset = ws["dataset"] or "(not configured)"
        lines.append(f"| {name} | {dataset} | {ws['description']} |")
    return "\n".join(lines)


@mcp.tool()
def fabric_discover_workspaces() -> str:
    """Discover all Fabric workspaces the service principal can access.

    Uses the Power BI REST API to list workspaces and their datasets.
    Use this to find workspace IDs needed for other Fabric REST tools.
    """
    import requests

    try:
        token = _get_fabric_token()
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

    lines = [f"## Fabric Workspaces ({len(workspaces)} found)\n"]
    for ws in workspaces:
        ws_id = ws["id"]
        ws_name = ws["name"]
        lines.append(f"### {ws_name}")
        lines.append(f"- ID: `{ws_id}`")
        lines.append(
            f"- XMLA endpoint: `powerbi://api.powerbi.com/v1.0/myorg/{ws_name}`"
        )

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


# ===== FABRIC REST API TOOLS =====


@mcp.tool()
def fabric_list_workspace_items(workspace_id: str, item_type: str = "") -> str:
    """List items in a Fabric workspace (semantic models, pipelines, lakehouses, etc.).

    Args:
        workspace_id: The workspace GUID (use fabric_discover_workspaces to find it).
        item_type: Optional filter — SemanticModel, DataPipeline, Lakehouse, Notebook, etc.
    """
    import requests

    try:
        token = _get_fabric_token()
    except Exception as e:
        return f"Auth error: {e}"

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    if item_type:
        url += f"?type={item_type}"

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return f"API error ({resp.status_code}): {resp.text[:500]}"
        items = resp.json().get("value", [])
    except Exception as e:
        return f"API error: {e}"

    if not items:
        filter_note = f" of type '{item_type}'" if item_type else ""
        return f"No items found{filter_note} in workspace `{workspace_id}`."

    lines = [f"## Workspace Items ({len(items)} found)\n"]
    lines.append("| Type | Name | ID |")
    lines.append("| --- | --- | --- |")
    for item in items:
        lines.append(
            f"| {item.get('type', '?')} | {item.get('displayName', '?')} | `{item.get('id', '?')}` |"
        )

    return "\n".join(lines)


@mcp.tool()
def fabric_get_refresh_history(
    workspace_id: str, dataset_id: str, top: int = 10
) -> str:
    """Get refresh history for a semantic model (dataset).

    Args:
        workspace_id: The workspace GUID.
        dataset_id: The dataset/semantic model GUID.
        top: Number of recent refreshes to return (default 10).
    """
    import requests

    try:
        token = _get_fabric_token()
    except Exception as e:
        return f"Auth error: {e}"

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top={top}"

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return f"API error ({resp.status_code}): {resp.text[:500]}"
        refreshes = resp.json().get("value", [])
    except Exception as e:
        return f"API error: {e}"

    if not refreshes:
        return "No refresh history found."

    lines = ["## Refresh History\n"]
    lines.append("| Status | Type | Start | End | Duration |")
    lines.append("| --- | --- | --- | --- | --- |")
    for r in refreshes:
        status = r.get("status", "?")
        refresh_type = r.get("refreshType", "?")
        start = r.get("startTime", "?")
        end = r.get("endTime", "?")
        # Extract just time portion for readability
        start_short = start[:19].replace("T", " ") if start != "?" else "?"
        end_short = end[:19].replace("T", " ") if end != "?" else "?"
        lines.append(f"| {status} | {refresh_type} | {start_short} | {end_short} | |")

    return "\n".join(lines)


@mcp.tool()
def fabric_trigger_refresh(workspace_id: str, dataset_id: str) -> str:
    """Trigger a refresh for a semantic model (dataset).

    Args:
        workspace_id: The workspace GUID.
        dataset_id: The dataset/semantic model GUID.
    """
    import requests

    try:
        token = _get_fabric_token()
    except Exception as e:
        return f"Auth error: {e}"

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

    try:
        resp = requests.post(url, headers=headers, timeout=30)
        if resp.status_code == 202:
            return f"Refresh triggered successfully for dataset `{dataset_id}`."
        else:
            return f"Refresh trigger failed ({resp.status_code}): {resp.text[:500]}"
    except Exception as e:
        return f"API error: {e}"


@mcp.tool()
def fabric_get_pipeline_runs(workspace_id: str, pipeline_id: str) -> str:
    """Get recent pipeline run history.

    Args:
        workspace_id: The workspace GUID.
        pipeline_id: The pipeline item GUID.
    """
    import requests

    try:
        token = _get_fabric_token()
    except Exception as e:
        return f"Auth error: {e}"

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances"

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return f"API error ({resp.status_code}): {resp.text[:500]}"
        runs = resp.json().get("value", [])
    except Exception as e:
        return f"API error: {e}"

    if not runs:
        return "No pipeline runs found."

    lines = ["## Pipeline Runs\n"]
    lines.append("| Status | Job Type | Start | End |")
    lines.append("| --- | --- | --- | --- |")
    for run in runs:
        status = run.get("status", "?")
        job_type = run.get("jobType", "?")
        start = run.get("startTimeUtc", "?")
        end = run.get("endTimeUtc", "?")
        start_short = start[:19].replace("T", " ") if start != "?" else "?"
        end_short = end[:19].replace("T", " ") if end != "?" else "?"
        lines.append(f"| {status} | {job_type} | {start_short} | {end_short} |")

    return "\n".join(lines)


@mcp.tool()
def fabric_trigger_pipeline(workspace_id: str, pipeline_id: str) -> str:
    """Trigger a pipeline run in Fabric.

    Args:
        workspace_id: The workspace GUID.
        pipeline_id: The pipeline item GUID.
    """
    import requests

    try:
        token = _get_fabric_token()
    except Exception as e:
        return f"Auth error: {e}"

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"

    try:
        resp = requests.post(url, headers=headers, timeout=30)
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            msg = f"Pipeline triggered successfully for `{pipeline_id}`."
            if location:
                msg += f"\nMonitor URL: {location}"
            return msg
        else:
            return f"Pipeline trigger failed ({resp.status_code}): {resp.text[:500]}"
    except Exception as e:
        return f"API error: {e}"


if __name__ == "__main__":
    mcp.run()
