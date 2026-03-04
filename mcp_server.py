"""
Microsoft Fabric MCP Server for Claude Code.

Provides DAX query execution, model exploration, static schema lookups,
and Fabric REST API operations against Majans' Microsoft Fabric workspaces —
semantic models, pipelines, lakehouses, and more.

Requires: pip install mcp python-dotenv requests
XMLA tools also require: pip install pyadomd pythonnet (Windows only)
"""

import json
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMAS_DIR = os.path.join(SCRIPT_DIR, "schemas")
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

# Workspace registry: 4 IBP-domain Fabric workspaces, 15 datasets
WORKSPACES = {
    "PRODUCT": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/PRODUCT",
        "datasets": {
            "CONSUMERv2": "Consumer insights model",
        },
    },
    "DEMAND": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/DEMAND",
        "datasets": {
            "SALESv2": "Sales & demand — customer orders, invoicing",
            "SCANv2": "POS retail scan data — Coles/Woolworths",
            "STORE": "Store-level data",
            "SCAN TOTAL GROCERY": "Total grocery scan data",
        },
    },
    "SUPPLY": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/SUPPLY",
        "datasets": {
            "AM": "Asset management",
            "CUSTOMER SERVICE v2": "Customer service metrics",
            "INVENTORYV2": "Inventory management",
            "MANUFACTURING V3": "Production & supply chain",
            "PURCHASINGV3": "Vendor SIFOT/DIFOT, PO delivery, supplier scoring",
        },
    },
    "REVIEW": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/REVIEW",
        "datasets": {
            "FINANCIALv2": "P&L, budgets, forecasts, actuals, GL",
            "PLANAUDIT": "Plan audit",
            "THREE-WAY": "Three-way match",
            "PRODUCTIONCOST": "Production costing",
            "COSTINGv2": "Costing model",
        },
    },
}

DEFAULT_DATASET = "SCANv2"

# Build reverse lookup: dataset name (upper) -> (workspace_key, endpoint, dataset_name)
_DATASET_INDEX = {}
for _ws_key, _ws_info in WORKSPACES.items():
    for _ds_name in _ws_info["datasets"]:
        _DATASET_INDEX[_ds_name.upper()] = {
            "workspace": _ws_key,
            "endpoint": _ws_info["endpoint"],
            "dataset": _ds_name,
        }


def _resolve_dataset(dataset: str) -> dict:
    """Resolve a dataset name to its workspace endpoint info (case-insensitive).

    Returns dict with keys: workspace, endpoint, dataset (canonical name).
    Raises ValueError if not found.
    """
    entry = _DATASET_INDEX.get(dataset.upper())
    if not entry:
        available = ", ".join(sorted(_DATASET_INDEX.keys()))
        raise ValueError(f"Unknown dataset '{dataset}'. Available: {available}")
    return entry


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


def _build_conn_str(dataset: str) -> str:
    """Build an XMLA connection string for the given dataset."""
    info = _resolve_dataset(dataset)
    return (
        f"Provider=MSOLAP;"
        f"Data Source={info['endpoint']};"
        f"Initial Catalog={info['dataset']};"
        f"User ID=app:{CLIENT_ID}@{TENANT_ID};"
        f"Password={CLIENT_SECRET};"
        f"Persist Security Info=True;"
        f"Impersonation Level=Impersonate;"
    )


def _build_conn_str_no_catalog(dataset: str) -> str:
    """Build connection string without Initial Catalog (for discovery)."""
    info = _resolve_dataset(dataset)
    return (
        f"Provider=MSOLAP;"
        f"Data Source={info['endpoint']};"
        f"User ID=app:{CLIENT_ID}@{TENANT_ID};"
        f"Password={CLIENT_SECRET};"
        f"Persist Security Info=True;"
        f"Impersonation Level=Impersonate;"
    )


def _execute(query: str, dataset: str = DEFAULT_DATASET) -> tuple:
    """Execute a query and return (headers, rows). New connection per query."""
    _ensure_xmla()
    conn_str = _build_conn_str(dataset)
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
        "Microsoft Fabric MCP server for Majans — provides access to 15 semantic models across 4 IBP workspaces "
        "(PRODUCT, DEMAND, SUPPLY, REVIEW) via DAX queries (XMLA), static schema lookups, "
        "workspace management, pipeline operations, and dataset refresh via the Fabric REST API.\n\n"
        "All XMLA tools take a 'dataset' parameter — the semantic model name. "
        "Default dataset is SCANv2. Use fabric_list_datasets() to see all 15 datasets.\n\n"
        "For fast schema lookups (no XMLA connection needed), use fabric_get_schema(dataset). "
        "For live queries, use fabric_list_tables(dataset) and fabric_list_measures(dataset).\n\n"
        "Datasets by workspace:\n"
        "  PRODUCT: CONSUMERv2\n"
        "  DEMAND: SALESv2, SCANv2, STORE, SCAN TOTAL GROCERY\n"
        "  SUPPLY: AM, CUSTOMER SERVICE v2, INVENTORYV2, MANUFACTURING V3, PURCHASINGV3\n"
        "  REVIEW: FINANCIALv2, PLANAUDIT, THREE-WAY, PRODUCTIONCOST, COSTINGv2"
    ),
)


# ===== XMLA TOOLS =====


@mcp.tool()
def fabric_dax_query(
    query: str, max_rows: int = 100, dataset: str = DEFAULT_DATASET
) -> str:
    """Execute a DAX query against a Power BI semantic model via XMLA.

    Args:
        query: DAX query string (use EVALUATE for tabular results).
        max_rows: Maximum rows to return (default 100).
        dataset: Semantic model to query — e.g. SCANv2 (default), FINANCIALv2, PURCHASINGV3.
            Use fabric_list_datasets() to see all 15 available datasets.

    Run fabric_list_tables or fabric_get_schema first to discover available tables and columns.

    Example queries:
        EVALUATE ROW("Test", 1)
        EVALUATE SUMMARIZECOLUMNS('Table'[Column], "Metric", SUM('Table'[Value]))
    """
    try:
        headers, rows = _execute(query, dataset)
        return _to_markdown_table(headers, rows, max_rows)
    except Exception as e:
        return f"DAX query error ({dataset}): {e}\n\nQuery was:\n```\n{query}\n```"


@mcp.tool()
def fabric_list_tables(dataset: str = DEFAULT_DATASET) -> str:
    """List all tables and columns in a semantic model with data types (live XMLA query).

    Args:
        dataset: Semantic model name — e.g. SCANv2 (default), FINANCIALv2, PURCHASINGV3.
            Use fabric_list_datasets() to see all 15 available datasets.

    For a faster offline alternative, use fabric_get_schema(dataset) instead.
    """
    try:
        info = _resolve_dataset(dataset)
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
        headers, rows = _execute(query, dataset)

        tables = {}
        for row in rows:
            tbl = row[0]
            if tbl not in tables:
                tables[tbl] = []
            tables[tbl].append(row)

        lines = [f"## {info['dataset']} Model Structure ({info['workspace']})\n"]
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
        return f"Error listing tables ({dataset}): {e}"


@mcp.tool()
def fabric_list_measures(dataset: str = DEFAULT_DATASET) -> str:
    """List all measures defined in a semantic model (live XMLA query).

    Args:
        dataset: Semantic model name — e.g. SCANv2 (default), FINANCIALv2, PURCHASINGV3.
            Use fabric_list_datasets() to see all 15 available datasets.

    For a faster offline alternative, use fabric_get_schema(dataset) instead.
    """
    try:
        info = _resolve_dataset(dataset)
        query = """
        SELECT
            [MEASUREGROUP_NAME],
            [MEASURE_NAME],
            [DEFAULT_FORMAT_STRING],
            [DESCRIPTION]
        FROM $SYSTEM.MDSCHEMA_MEASURES
        WHERE [MEASURE_IS_VISIBLE]
        """
        headers, rows = _execute(query, dataset)

        lines = [f"## {info['dataset']} Measures ({info['workspace']})\n"]
        lines.append("| Measure Group | Measure | Format | Description |")
        lines.append("| --- | --- | --- | --- |")
        for row in rows:
            fmt_str = row[2] if row[2] else ""
            desc = row[3] if row[3] else ""
            lines.append(f"| {row[0]} | {row[1]} | `{fmt_str}` | {desc} |")

        return "\n".join(lines)
    except Exception as e:
        return f"Error listing measures ({dataset}): {e}"


@mcp.tool()
def fabric_test_xmla(dataset: str = DEFAULT_DATASET) -> str:
    """Test XMLA connection to a dataset and list its tables.

    Args:
        dataset: Semantic model name — e.g. SCANv2 (default), FINANCIALv2, PURCHASINGV3.
            Use fabric_list_datasets() to see all 15 available datasets.
    """
    try:
        info = _resolve_dataset(dataset)
    except ValueError as e:
        return str(e)

    result = [
        f"Dataset: {info['dataset']}",
        f"Workspace: {info['workspace']}",
        f"Endpoint: {info['endpoint']}",
    ]

    try:
        headers, rows = _execute('EVALUATE ROW("Status", "Connected")', dataset)
        result.insert(0, "Connection: OK")

        headers2, rows2 = _execute(
            """
            SELECT [TABLE_NAME]
            FROM $SYSTEM.DBSCHEMA_TABLES
            WHERE [TABLE_TYPE] = 'TABLE'
        """,
            dataset,
        )
        result.append(f"\nTables ({len(rows2)}):")
        for row in rows2:
            result.append(f"  - {row[0]}")

        return "\n".join(result)
    except Exception as e:
        result.insert(0, "Connection FAILED")
        result.append(f"Error: {e}")
        return "\n".join(result)


# ===== DATASET & SCHEMA TOOLS =====


@mcp.tool()
def fabric_list_datasets() -> str:
    """List all 15 configured semantic models (datasets) grouped by Fabric workspace.

    Shows dataset names, descriptions, and which workspace they belong to.
    Use the dataset name as the 'dataset' parameter in other XMLA tools.
    """
    lines = ["## Configured Datasets\n"]
    total = 0
    for ws_name, ws_info in WORKSPACES.items():
        lines.append(f"### {ws_name}")
        lines.append(f"Endpoint: `{ws_info['endpoint']}`\n")
        lines.append("| Dataset | Description |")
        lines.append("| --- | --- |")
        for ds_name, ds_desc in ws_info["datasets"].items():
            lines.append(f"| {ds_name} | {ds_desc} |")
            total += 1
        lines.append("")
    lines.append(f"*{total} datasets across {len(WORKSPACES)} workspaces. Default: {DEFAULT_DATASET}*")
    return "\n".join(lines)


@mcp.tool()
def fabric_get_schema(dataset: str) -> str:
    """Get the static data dictionary for a semantic model — tables, columns, and measures.

    Returns the cached schema from the schemas/ directory. No XMLA connection needed.
    Use fabric_refresh_schema(dataset) to update the cached schema from a live XMLA query.

    Args:
        dataset: Semantic model name — e.g. SCANv2, FINANCIALv2, PURCHASINGV3.
    """
    info = _resolve_dataset(dataset)
    schema_path = os.path.join(SCHEMAS_DIR, f"{info['dataset']}.json")

    if not os.path.exists(schema_path):
        return (
            f"No cached schema found for '{info['dataset']}'. "
            f"Run fabric_refresh_schema(dataset='{info['dataset']}') to generate it, "
            f"or use fabric_list_tables(dataset='{info['dataset']}') for a live query."
        )

    with open(schema_path, "r") as f:
        schema = json.load(f)

    lines = [f"## {schema['dataset']} Schema ({schema['workspace']})\n"]
    lines.append(f"*Captured: {schema.get('captured_at', 'unknown')}*\n")

    # Tables and columns
    for table in schema.get("tables", []):
        lines.append(f"### {table['name']}")
        lines.append("| Column | Data Type | Description |")
        lines.append("| --- | --- | --- |")
        for col in table.get("columns", []):
            desc = col.get("description", "")
            lines.append(f"| {col['name']} | {col['data_type']} | {desc} |")
        lines.append("")

    # Measures
    measures = schema.get("measures", [])
    if measures:
        lines.append("### Measures\n")
        lines.append("| Table | Measure | Format | Description |")
        lines.append("| --- | --- | --- | --- |")
        for m in measures:
            fmt = m.get("format_string", "")
            desc = m.get("description", "")
            lines.append(f"| {m['table']} | {m['name']} | `{fmt}` | {desc} |")
        lines.append("")

    return "\n".join(lines)


@mcp.tool()
def fabric_refresh_schema(dataset: str) -> str:
    """Live-query XMLA and update the static schema JSON file for a dataset.

    Use this when a semantic model has been updated and the cached schema is stale.

    Args:
        dataset: Semantic model name — e.g. SCANv2, FINANCIALv2, PURCHASINGV3.
    """
    try:
        info = _resolve_dataset(dataset)
        ds_name = info["dataset"]

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
        _, col_rows = _execute(col_query, dataset)

        tables = {}
        for row in col_rows:
            tbl = row[0]
            if tbl not in tables:
                tables[tbl] = []
            tables[tbl].append({
                "name": row[1],
                "data_type": str(row[2]) if row[2] else "",
                "description": str(row[3]) if row[3] else "",
            })

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
        _, meas_rows = _execute(meas_query, dataset)

        measures = []
        for row in meas_rows:
            measures.append({
                "table": str(row[0]) if row[0] else "",
                "name": str(row[1]) if row[1] else "",
                "format_string": str(row[2]) if row[2] else "",
                "description": str(row[3]) if row[3] else "",
            })

        schema = {
            "dataset": ds_name,
            "workspace": info["workspace"],
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "tables": [
                {"name": tbl, "columns": cols}
                for tbl, cols in sorted(tables.items())
            ],
            "measures": measures,
        }

        os.makedirs(SCHEMAS_DIR, exist_ok=True)
        schema_path = os.path.join(SCHEMAS_DIR, f"{ds_name}.json")
        with open(schema_path, "w") as f:
            json.dump(schema, f, indent=2)

        return (
            f"Schema refreshed for {ds_name} ({info['workspace']}). "
            f"{len(tables)} tables, {len(measures)} measures. "
            f"Saved to schemas/{ds_name}.json"
        )
    except Exception as e:
        return f"Error refreshing schema ({dataset}): {e}"


# ===== WORKSPACE DISCOVERY TOOLS =====


@mcp.tool()
def fabric_discover_workspaces(format: str = "markdown") -> str:
    """Discover all Fabric workspaces the service principal can access.

    Uses the Power BI REST API to list workspaces and their datasets.
    Use this to find workspace IDs needed for other Fabric REST tools.

    Args:
        format: Output format — "markdown" (default) or "json" for structured data.
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

    if format == "json":
        result = []
        for ws in workspaces:
            ws_id = ws["id"]
            ws_entry = {"id": ws_id, "name": ws["name"], "datasets": []}
            try:
                ds_resp = requests.get(
                    f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/datasets",
                    headers=headers, timeout=30,
                )
                if ds_resp.status_code == 200:
                    for ds in ds_resp.json().get("value", []):
                        ws_entry["datasets"].append({"id": ds["id"], "name": ds["name"]})
            except Exception:
                pass
            result.append(ws_entry)
        return json.dumps(result)

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
def fabric_list_workspace_items(
    workspace_id: str, item_type: str = "", format: str = "markdown"
) -> str:
    """List items in a Fabric workspace (semantic models, pipelines, lakehouses, etc.).

    Args:
        workspace_id: The workspace GUID (use fabric_discover_workspaces to find it).
        item_type: Optional filter — SemanticModel, DataPipeline, Lakehouse, Notebook, etc.
        format: Output format — "markdown" (default) or "json" for structured data.
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
        if format == "json":
            return "[]"
        return f"No items found{filter_note} in workspace `{workspace_id}`."

    if format == "json":
        return json.dumps(
            [{"id": i.get("id"), "type": i.get("type"), "name": i.get("displayName")} for i in items]
        )

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
    workspace_id: str, dataset_id: str, top: int = 10, format: str = "markdown"
) -> str:
    """Get refresh history for a semantic model (dataset).

    Args:
        workspace_id: The workspace GUID.
        dataset_id: The dataset/semantic model GUID.
        top: Number of recent refreshes to return (default 10).
        format: Output format — "markdown" (default) or "json" for structured data.
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
        if format == "json":
            return "[]"
        return "No refresh history found."

    if format == "json":
        return json.dumps(refreshes)

    lines = ["## Refresh History\n"]
    lines.append("| Status | Type | Start | End | Duration |")
    lines.append("| --- | --- | --- | --- | --- |")
    for r in refreshes:
        status = r.get("status", "?")
        refresh_type = r.get("refreshType", "?")
        start = r.get("startTime", "?")
        end = r.get("endTime", "?")
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
def fabric_get_pipeline_runs(
    workspace_id: str, pipeline_id: str, format: str = "markdown"
) -> str:
    """Get recent pipeline run history.

    Args:
        workspace_id: The workspace GUID.
        pipeline_id: The pipeline item GUID.
        format: Output format — "markdown" (default) or "json" for structured data.
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
        if format == "json":
            return "[]"
        return "No pipeline runs found."

    if format == "json":
        return json.dumps(runs)

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
def fabric_list_dataflows(
    workspace_id: str, format: str = "markdown"
) -> str:
    """List dataflows in a Power BI workspace.

    Args:
        workspace_id: The workspace GUID.
        format: Output format — "markdown" (default) or "json" for structured data.
    """
    import requests

    try:
        token = _get_fabric_token()
    except Exception as e:
        return f"Auth error: {e}"

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dataflows"

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return f"API error ({resp.status_code}): {resp.text[:500]}"
        dataflows = resp.json().get("value", [])
    except Exception as e:
        return f"API error: {e}"

    if not dataflows:
        if format == "json":
            return "[]"
        return f"No dataflows found in workspace `{workspace_id}`."

    if format == "json":
        return json.dumps(
            [{"id": df.get("objectId"), "name": df.get("name")} for df in dataflows]
        )

    lines = [f"## Dataflows ({len(dataflows)} found)\n"]
    lines.append("| Name | ID |")
    lines.append("| --- | --- |")
    for df in dataflows:
        lines.append(f"| {df.get('name', '?')} | `{df.get('objectId', '?')}` |")

    return "\n".join(lines)


@mcp.tool()
def fabric_get_dataflow_transactions(
    workspace_id: str, dataflow_id: str, format: str = "markdown"
) -> str:
    """Get transaction history for a dataflow.

    Args:
        workspace_id: The workspace GUID.
        dataflow_id: The dataflow GUID (objectId from fabric_list_dataflows).
        format: Output format — "markdown" (default) or "json" for structured data.
    """
    import requests

    try:
        token = _get_fabric_token()
    except Exception as e:
        return f"Auth error: {e}"

    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/transactions"

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            return f"API error ({resp.status_code}): {resp.text[:500]}"
        transactions = resp.json().get("value", [])
    except Exception as e:
        return f"API error: {e}"

    if not transactions:
        if format == "json":
            return "[]"
        return "No transactions found."

    if format == "json":
        return json.dumps(transactions)

    lines = ["## Dataflow Transactions\n"]
    lines.append("| Status | Type | Start | End |")
    lines.append("| --- | --- | --- | --- |")
    for t in transactions:
        status = t.get("status", "?")
        refresh_type = t.get("refreshType", "?")
        start = t.get("startTime", "?")
        end = t.get("endTime", "?")
        start_short = start[:19].replace("T", " ") if start != "?" else "?"
        end_short = end[:19].replace("T", " ") if end != "?" else "?"
        lines.append(f"| {status} | {refresh_type} | {start_short} | {end_short} |")

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
