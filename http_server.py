"""
HTTP wrapper for connector-fabric — exposes MCP tools as HTTP endpoints
AND as a StreamableHTTP MCP server for per-user Fabric access.

Two auth paths:
  /mcp       → StreamableHTTP (MCP protocol). Three modes: Bearer MI JWT
               (validated via JWKS, SP path), Bearer user token (per-user
               Fabric calls), or X-API-Key (agent SP fallback).
  /call-tool → Legacy REST (backward compat for agents). X-API-Key required →
               SP token → PBI API with full access.

DAX execution: XMLA (preferred, if ADOMD.NET available on Windows) with
automatic fallback to Power BI REST API (executeQueries) on Linux.
XMLA bypasses PBI REST API permission chain issues with composite models.
"""

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel

from auth import MANAGED_IDENTITY_ENABLED, McpAuthMiddleware, user_token_var

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("connector-fabric")

# --- CONFIG ---
TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "")
API_KEY = os.getenv("FABRIC_API_KEY", "")
PORT = int(os.getenv("PORT", "8010"))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMAS_DIR = os.path.join(SCRIPT_DIR, "schemas")

# Workspace registry — same as mcp_server.py
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
    "HR": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/HR",
        "datasets": {
            "HR": "HR analytics — headcount, workforce, Employment Hero data",
        },
    },
    "FIELD": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/FIELD",
        "datasets": {
            "FIELD": "Field marketing — store sales, stock, ranging, distribution, Metcash B2B",
        },
    },
}

DEFAULT_DATASET = "SCANv2"

# Build reverse lookup
_DATASET_INDEX: dict[str, dict] = {}
for _ws_key, _ws_info in WORKSPACES.items():
    for _ds_name in _ws_info["datasets"]:
        _DATASET_INDEX[_ds_name.upper()] = {
            "workspace": _ws_key,
            "endpoint": _ws_info["endpoint"],
            "dataset": _ds_name,
        }

# Workspace GUID cache (discovered at startup via REST API)
_workspace_guids: dict[str, str] = {}  # workspace name -> GUID
_dataset_guids: dict[str, str] = {}  # "WORKSPACE/DATASET" -> dataset GUID


def _resolve_dataset(dataset: str) -> dict:
    entry = _DATASET_INDEX.get(dataset.upper())
    if not entry:
        available = ", ".join(sorted(_DATASET_INDEX.keys()))
        raise ValueError(f"Unknown dataset '{dataset}'. Available: {available}")
    return entry


# --- AUTH ---
_token_cache: dict[str, Any] = {"token": None, "expires_at": 0}


def _get_sp_token() -> str:
    """Get Service Principal token via client credentials flow."""
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


def _sp_headers() -> dict:
    """Headers using Service Principal token — for startup discovery and agent calls."""
    return {"Authorization": f"Bearer {_get_sp_token()}"}


def _request_headers() -> dict:
    """Headers using user token if available, else Service Principal token.

    When a user connects via /mcp with a Bearer token, the ASGI middleware
    stores it in user_token_var. Tool functions call this to make PBI API
    calls as that user, so Fabric enforces workspace roles natively.
    """
    user_tok = user_token_var.get()
    if user_tok:
        return {"Authorization": f"Bearer {user_tok}"}
    return _sp_headers()


# --- WORKSPACE/DATASET GUID DISCOVERY ---


def _discover_guids():
    """Discover workspace and dataset GUIDs via REST API.

    Always uses SP token — GUIDs are universal identifiers, not per-user.
    """
    global _workspace_guids, _dataset_guids
    try:
        resp = requests.get(
            "https://api.powerbi.com/v1.0/myorg/groups",
            headers=_sp_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        for ws in resp.json().get("value", []):
            name = ws.get("name", "")
            ws_id = ws.get("id", "")
            if name.upper() in WORKSPACES:
                _workspace_guids[name.upper()] = ws_id
                logger.info("Workspace %s -> %s", name, ws_id)

                # Discover datasets in this workspace
                ds_resp = requests.get(
                    f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/datasets",
                    headers=_sp_headers(),
                    timeout=30,
                )
                if ds_resp.ok:
                    for ds in ds_resp.json().get("value", []):
                        ds_name = ds.get("name", "")
                        ds_id = ds.get("id", "")
                        key = f"{name.upper()}/{ds_name.upper()}"
                        _dataset_guids[key] = ds_id
                        logger.info("  Dataset %s -> %s", ds_name, ds_id)
    except Exception as e:
        logger.error("Failed to discover workspace GUIDs: %s", e)


# --- XMLA (OPTIONAL — Windows only) ---

_xmla_available: bool | None = None  # None = not checked yet
_Pyadomd = None


def _check_xmla() -> bool:
    """Check if XMLA/ADOMD.NET is available (Windows + pyadomd + .NET DLLs)."""
    global _xmla_available, _Pyadomd
    if _xmla_available is not None:
        return _xmla_available

    try:
        adomd_dll_path = os.path.join(SCRIPT_DIR, "adomd_package", "lib", "net45")
        if not os.path.isdir(adomd_dll_path):
            logger.info("XMLA: ADOMD.NET DLLs not found — REST-only mode")
            _xmla_available = False
            return False

        import sys
        sys.path.insert(0, adomd_dll_path)
        os.environ["PATH"] = adomd_dll_path + os.pathsep + os.environ.get("PATH", "")

        import clr
        clr.AddReference(
            os.path.join(adomd_dll_path, "Microsoft.AnalysisServices.AdomdClient.dll")
        )

        from pyadomd import Pyadomd
        _Pyadomd = Pyadomd
        _xmla_available = True
        logger.info("XMLA: ADOMD.NET loaded — XMLA+REST dual mode")
        return True
    except Exception as e:
        logger.info("XMLA: Not available (%s) — REST-only mode", e)
        _xmla_available = False
        return False


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


def _execute_dax_xmla(
    query: str, dataset: str = DEFAULT_DATASET, max_rows: int = 500
) -> dict:
    """Execute DAX query via XMLA/ADOMD.NET.

    Connects directly to the Analysis Services endpoint using SP credentials
    in the connection string. Bypasses PBI REST API permission layer.
    """
    if not _check_xmla():
        raise RuntimeError("XMLA not available")

    conn_str = _build_conn_str(dataset)
    try:
        conn = _Pyadomd(conn_str)
        conn.open()
        try:
            cur = conn.cursor()
            cur.execute(query)
            headers = [col[0] for col in cur.description] if cur.description else []
            rows_raw = cur.fetchall()
            cur.close()

            # Convert to list of dicts (same format as REST API response)
            rows = []
            for row in rows_raw[:max_rows]:
                rows.append({h: v for h, v in zip(headers, row)})

            return {"columns": headers, "rows": rows, "total_rows": len(rows_raw), "method": "xmla"}
        finally:
            conn.close()
    except Exception as e:
        return {"error": f"XMLA error: {e}", "method": "xmla"}


# --- DAX EXECUTION VIA REST API ---


def _execute_dax_rest(
    query: str, dataset: str = DEFAULT_DATASET, max_rows: int = 500
) -> dict:
    """Execute DAX query via Power BI REST API executeQueries endpoint.

    Uses the calling user's token when available (per-user access control),
    falls back to SP token for agent calls.
    """
    info = _resolve_dataset(dataset)
    ws_name = info["workspace"]
    ds_name = info["dataset"]

    ws_guid = _workspace_guids.get(ws_name.upper())
    ds_key = f"{ws_name.upper()}/{ds_name.upper()}"
    ds_guid = _dataset_guids.get(ds_key)

    if not ws_guid or not ds_guid:
        # Re-discover with SP token (GUID cache is universal)
        _discover_guids()
        ws_guid = _workspace_guids.get(ws_name.upper())
        ds_guid = _dataset_guids.get(ds_key)
        if not ws_guid or not ds_guid:
            return {
                "error": f"Could not resolve GUIDs for {ws_name}/{ds_name}. "
                f"Available workspaces: {list(_workspace_guids.keys())}, "
                f"datasets: {list(_dataset_guids.keys())}"
            }

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_guid}/datasets/{ds_guid}/executeQueries"
    payload = {
        "queries": [{"query": query}],
        "serializerSettings": {"includeNulls": True},
    }

    try:
        resp = requests.post(url, json=payload, headers=_request_headers(), timeout=120)
        if resp.status_code == 400:
            error_data = resp.json()
            error_msg = error_data.get("error", {}).get("message", resp.text)
            return {"error": f"DAX error: {error_msg}", "query": query}
        if resp.status_code == 403:
            return {
                "error": f"Access denied to {ws_name}/{ds_name}. "
                "Your account does not have permission to query this dataset."
            }
        resp.raise_for_status()
        result = resp.json()

        # Parse Power BI REST API response format
        results_list = result.get("results", [])
        if not results_list:
            return {"rows": [], "columns": []}

        tables = results_list[0].get("tables", [])
        if not tables:
            return {"rows": [], "columns": []}

        rows = tables[0].get("rows", [])
        # Extract column names from first row
        columns = list(rows[0].keys()) if rows else []

        # Trim to max_rows
        if len(rows) > max_rows:
            rows = rows[:max_rows]

        return {"columns": columns, "rows": rows, "total_rows": len(rows)}

    except requests.exceptions.HTTPError as e:
        return {
            "error": f"REST API error ({e.response.status_code}): {e.response.text[:500]}"
        }
    except Exception as e:
        return {"error": str(e)}


# --- TOOL IMPLEMENTATIONS ---


def _tool_fabric_dax_query(
    query: str, max_rows: int = 500, dataset: str = DEFAULT_DATASET, **_
) -> dict:
    """Execute DAX — tries XMLA first (bypasses REST permission chain), falls back to REST."""
    if _check_xmla():
        result = _execute_dax_xmla(query, dataset, max_rows)
        if "error" not in result:
            return result
        # XMLA failed — log and fall back to REST
        logger.warning("XMLA failed for %s, falling back to REST: %s", dataset, result.get("error"))
    return _execute_dax_rest(query, dataset, max_rows)


def _tool_fabric_dax_query_xmla(
    query: str, max_rows: int = 500, dataset: str = DEFAULT_DATASET, **_
) -> dict:
    """Execute DAX via XMLA only (no REST fallback). Errors if XMLA unavailable."""
    if not _check_xmla():
        return {"error": "XMLA not available. Requires Windows with ADOMD.NET + pyadomd."}
    return _execute_dax_xmla(query, dataset, max_rows)


def _tool_fabric_dax_query_rest(
    query: str, max_rows: int = 500, dataset: str = DEFAULT_DATASET, **_
) -> dict:
    """Execute DAX via REST API only (no XMLA). For explicit REST-only calls."""
    return _execute_dax_rest(query, dataset, max_rows)


def _tool_fabric_list_tables(dataset: str = DEFAULT_DATASET, **_) -> dict:
    """Return schema from static cached files (no XMLA needed)."""
    info = _resolve_dataset(dataset)
    schema_path = os.path.join(SCHEMAS_DIR, f"{info['dataset']}.json")
    if not os.path.exists(schema_path):
        return {
            "error": f"No cached schema for {info['dataset']}. Available schemas: {os.listdir(SCHEMAS_DIR)}"
        }
    with open(schema_path) as f:
        return json.load(f)


def _tool_fabric_get_schema(dataset: str = DEFAULT_DATASET, **_) -> dict:
    return _tool_fabric_list_tables(dataset)


def _tool_fabric_list_datasets(**_) -> dict:
    result = {}
    for ws_name, ws_info in WORKSPACES.items():
        result[ws_name] = {
            "endpoint": ws_info["endpoint"],
            "datasets": ws_info["datasets"],
        }
    return result


def _tool_fabric_discover_workspaces(**_) -> dict:
    """Discover workspaces — uses calling user's token when available."""
    try:
        resp = requests.get(
            "https://api.powerbi.com/v1.0/myorg/groups",
            headers=_request_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return {"workspaces": resp.json().get("value", [])}
    except Exception as e:
        return {"error": str(e)}


# --- Fabric REST API tool wrappers (for /call-tool registry) ---


def _tool_fabric_list_workspace_items(workspace_id: str, item_type: str = "", **_) -> dict:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    if item_type:
        url += f"?type={item_type}"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 403:
            return {"error": f"Access denied to workspace {workspace_id}."}
        resp.raise_for_status()
        items = resp.json().get("value", [])
        return {"items": [{"id": i.get("id"), "type": i.get("type"), "name": i.get("displayName")} for i in items]}
    except Exception as e:
        return {"error": str(e)}


def _tool_fabric_get_refresh_history(workspace_id: str, dataset_id: str, top: int = 10, **_) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top={top}"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        resp.raise_for_status()
        return {"refreshes": resp.json().get("value", [])}
    except Exception as e:
        return {"error": str(e)}


def _tool_fabric_trigger_refresh(workspace_id: str, dataset_id: str, **_) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    try:
        resp = requests.post(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 202:
            return {"status": "triggered", "message": f"Refresh triggered for dataset {dataset_id}."}
        return {"error": f"Refresh trigger failed ({resp.status_code}): {resp.text[:500]}"}
    except Exception as e:
        return {"error": str(e)}


def _tool_fabric_list_dataflows(workspace_id: str, **_) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dataflows"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        resp.raise_for_status()
        dataflows = resp.json().get("value", [])
        return {"dataflows": [{"id": df.get("objectId"), "name": df.get("name"), "description": df.get("description", "")} for df in dataflows]}
    except Exception as e:
        return {"error": str(e)}


def _tool_fabric_get_dataflow_transactions(workspace_id: str, dataflow_id: str, **_) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/transactions"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        resp.raise_for_status()
        return {"transactions": resp.json().get("value", [])}
    except Exception as e:
        return {"error": str(e)}


def _tool_fabric_get_dataflow_definition(workspace_id: str, dataflow_id: str, **_) -> dict:
    import base64
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{dataflow_id}/getDefinition"
    try:
        resp = requests.post(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 200:
            definition = resp.json()
        elif resp.status_code == 202:
            location = resp.headers.get("Location", "")
            retry_after = int(resp.headers.get("Retry-After", "5"))
            if not location:
                return {"error": "Accepted (202) but no Location header to poll."}
            for _ in range(12):
                time.sleep(retry_after)
                poll = requests.get(location, headers=_request_headers(), timeout=30)
                if poll.status_code == 200:
                    definition = poll.json()
                    break
                elif poll.status_code == 202:
                    retry_after = int(poll.headers.get("Retry-After", "5"))
                else:
                    return {"error": f"Poll error ({poll.status_code}): {poll.text[:500]}"}
            else:
                return {"error": "Timed out waiting for definition (60s)."}
        else:
            return {"error": f"API error ({resp.status_code}): {resp.text[:500]}"}
    except Exception as e:
        return {"error": str(e)}

    parts = definition.get("definition", {}).get("parts", [])
    result = []
    for part in parts:
        payload = part.get("payload", "")
        try:
            content = base64.b64decode(payload).decode("utf-8")
        except Exception:
            content = "(unable to decode)"
        result.append({"path": part.get("path", "unknown"), "content": content})
    return {"parts": result}


def _tool_fabric_get_pipeline_runs(workspace_id: str, pipeline_id: str, **_) -> dict:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        resp.raise_for_status()
        return {"runs": resp.json().get("value", [])}
    except Exception as e:
        return {"error": str(e)}


def _tool_fabric_trigger_pipeline(workspace_id: str, pipeline_id: str, **_) -> dict:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    try:
        resp = requests.post(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            result = {"status": "triggered", "message": f"Pipeline {pipeline_id} triggered."}
            if location:
                result["monitor_url"] = location
            return result
        return {"error": f"Pipeline trigger failed ({resp.status_code}): {resp.text[:500]}"}
    except Exception as e:
        return {"error": str(e)}


# Tool registry (used by /call-tool REST endpoint)
TOOLS: dict[str, callable] = {
    "fabric_dax_query": _tool_fabric_dax_query,
    "fabric_dax_query_xmla": _tool_fabric_dax_query_xmla,
    "fabric_dax_query_rest": _tool_fabric_dax_query_rest,
    "fabric_list_tables": _tool_fabric_list_tables,
    "fabric_get_schema": _tool_fabric_get_schema,
    "fabric_list_datasets": _tool_fabric_list_datasets,
    "fabric_discover_workspaces": _tool_fabric_discover_workspaces,
    "fabric_list_workspace_items": _tool_fabric_list_workspace_items,
    "fabric_get_refresh_history": _tool_fabric_get_refresh_history,
    "fabric_trigger_refresh": _tool_fabric_trigger_refresh,
    "fabric_list_dataflows": _tool_fabric_list_dataflows,
    "fabric_get_dataflow_transactions": _tool_fabric_get_dataflow_transactions,
    "fabric_get_dataflow_definition": _tool_fabric_get_dataflow_definition,
    "fabric_get_pipeline_runs": _tool_fabric_get_pipeline_runs,
    "fabric_trigger_pipeline": _tool_fabric_trigger_pipeline,
}


# --- MCP SERVER (StreamableHTTP) ---

CONTAINER_FQDN = (
    "connector-fabric.proudplant-b5864354.australiaeast.azurecontainerapps.io"
)
CUSTOM_DOMAIN = "fabric.majans.com"

mcp = FastMCP(
    "connector-fabric",
    stateless_http=True,
    json_response=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            f"{CUSTOM_DOMAIN}",
            f"{CUSTOM_DOMAIN}:*",
            f"{CONTAINER_FQDN}",
            f"{CONTAINER_FQDN}:*",
            "127.0.0.1:*",
            "localhost:*",
        ],
    ),
)
mcp.settings.streamable_http_path = "/"


@mcp.tool()
def fabric_dax_query(query: str, max_rows: int = 500, dataset: str = "SCANv2") -> dict:
    """Execute a DAX query against a Fabric semantic model.

    Uses EVALUATE syntax. Results are returned as columns + rows.
    Automatically uses XMLA (preferred, bypasses permission chain issues)
    with fallback to REST API if XMLA is unavailable.
    """
    return _tool_fabric_dax_query(query=query, max_rows=max_rows, dataset=dataset)


@mcp.tool()
def fabric_dax_query_xmla(query: str, max_rows: int = 500, dataset: str = "SCANv2") -> dict:
    """Execute a DAX query via XMLA/ADOMD.NET only (no REST fallback).

    XMLA connects directly to the Analysis Services endpoint using SP
    credentials. Bypasses PBI REST API permission layer — works even on
    composite models with DirectQuery chains that block the REST API.
    Requires Windows with ADOMD.NET.
    """
    return _tool_fabric_dax_query_xmla(query=query, max_rows=max_rows, dataset=dataset)


@mcp.tool()
def fabric_dax_query_rest(query: str, max_rows: int = 500, dataset: str = "SCANv2") -> dict:
    """Execute a DAX query via Power BI REST API only (no XMLA).

    Uses the PBI executeQueries endpoint. Subject to PBI REST API permission
    requirements (Build permission needed on the dataset and all upstream
    models in the chain for composite/DirectQuery models).
    """
    return _tool_fabric_dax_query_rest(query=query, max_rows=max_rows, dataset=dataset)


@mcp.tool()
def fabric_list_tables(dataset: str = "SCANv2") -> dict:
    """List tables and columns from cached schema for a dataset."""
    return _tool_fabric_list_tables(dataset=dataset)


@mcp.tool()
def fabric_get_schema(dataset: str = "SCANv2") -> dict:
    """Get cached schema (tables, columns, measures) for a dataset."""
    return _tool_fabric_get_schema(dataset=dataset)


@mcp.tool()
def fabric_list_datasets() -> dict:
    """List all configured datasets grouped by workspace."""
    return _tool_fabric_list_datasets()


@mcp.tool()
def fabric_discover_workspaces() -> dict:
    """Discover all workspaces accessible to the current user.

    Returns only workspaces the calling user has permission to access.
    """
    return _tool_fabric_discover_workspaces()


# --- FABRIC REST API TOOLS (MCP only) ---


@mcp.tool()
def fabric_list_workspace_items(
    workspace_id: str,
    item_type: str = "",
) -> dict:
    """List items in a Fabric workspace (semantic models, pipelines, lakehouses, dataflows, etc.).

    Args:
        workspace_id: The workspace GUID (use fabric_discover_workspaces to find it).
        item_type: Optional filter — SemanticModel, DataPipeline, Lakehouse, Notebook, Dataflow, etc.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    if item_type:
        url += f"?type={item_type}"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 403:
            return {"error": f"Access denied to workspace {workspace_id}."}
        resp.raise_for_status()
        items = resp.json().get("value", [])
        return {
            "items": [
                {"id": i.get("id"), "type": i.get("type"), "name": i.get("displayName")}
                for i in items
            ]
        }
    except requests.exceptions.HTTPError as e:
        return {
            "error": f"API error ({e.response.status_code}): {e.response.text[:500]}"
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def fabric_get_refresh_history(
    workspace_id: str,
    dataset_id: str,
    top: int = 10,
) -> dict:
    """Get refresh history for a semantic model (dataset).

    Args:
        workspace_id: The workspace GUID.
        dataset_id: The dataset/semantic model GUID.
        top: Number of recent refreshes to return (default 10).
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top={top}"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 403:
            return {"error": "Access denied to this dataset."}
        resp.raise_for_status()
        return {"refreshes": resp.json().get("value", [])}
    except requests.exceptions.HTTPError as e:
        return {
            "error": f"API error ({e.response.status_code}): {e.response.text[:500]}"
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def fabric_trigger_refresh(workspace_id: str, dataset_id: str) -> dict:
    """Trigger a refresh for a semantic model (dataset).

    Args:
        workspace_id: The workspace GUID.
        dataset_id: The dataset/semantic model GUID.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    try:
        resp = requests.post(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 202:
            return {
                "status": "triggered",
                "message": f"Refresh triggered for dataset {dataset_id}.",
            }
        return {
            "error": f"Refresh trigger failed ({resp.status_code}): {resp.text[:500]}"
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def fabric_list_dataflows(workspace_id: str) -> dict:
    """List dataflows (Gen1 and Gen2) in a Power BI workspace.

    Args:
        workspace_id: The workspace GUID.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dataflows"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 403:
            return {"error": f"Access denied to workspace {workspace_id}."}
        resp.raise_for_status()
        dataflows = resp.json().get("value", [])
        return {
            "dataflows": [
                {
                    "id": df.get("objectId"),
                    "name": df.get("name"),
                    "description": df.get("description", ""),
                }
                for df in dataflows
            ]
        }
    except requests.exceptions.HTTPError as e:
        return {
            "error": f"API error ({e.response.status_code}): {e.response.text[:500]}"
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def fabric_get_dataflow_transactions(
    workspace_id: str,
    dataflow_id: str,
) -> dict:
    """Get transaction/refresh history for a dataflow.

    Args:
        workspace_id: The workspace GUID.
        dataflow_id: The dataflow GUID (objectId from fabric_list_dataflows).
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/transactions"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 403:
            return {"error": "Access denied to this dataflow."}
        resp.raise_for_status()
        return {"transactions": resp.json().get("value", [])}
    except requests.exceptions.HTTPError as e:
        return {
            "error": f"API error ({e.response.status_code}): {e.response.text[:500]}"
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def fabric_get_dataflow_definition(workspace_id: str, dataflow_id: str) -> dict:
    """Get the M-query definition of a Fabric Gen2 dataflow.

    Retrieves the Power Query (M) expressions that define each table/query
    in the dataflow. Useful for inspecting filters, source connections,
    and transformation logic.

    Args:
        workspace_id: The workspace GUID.
        dataflow_id: The dataflow item GUID (from fabric_list_workspace_items).
    """
    import base64

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{dataflow_id}/getDefinition"
    try:
        resp = requests.post(url, headers=_request_headers(), timeout=30)

        if resp.status_code == 200:
            definition = resp.json()
        elif resp.status_code == 202:
            location = resp.headers.get("Location", "")
            retry_after = int(resp.headers.get("Retry-After", "5"))
            if not location:
                return {"error": "Accepted (202) but no Location header to poll."}
            import time

            for _ in range(12):
                time.sleep(retry_after)
                poll = requests.get(location, headers=_request_headers(), timeout=30)
                if poll.status_code == 200:
                    definition = poll.json()
                    break
                elif poll.status_code == 202:
                    retry_after = int(poll.headers.get("Retry-After", "5"))
                    continue
                else:
                    return {
                        "error": f"Poll error ({poll.status_code}): {poll.text[:500]}"
                    }
            else:
                return {"error": "Timed out waiting for definition (60s)."}
        elif resp.status_code == 403:
            return {"error": "Access denied to this dataflow definition."}
        else:
            return {"error": f"API error ({resp.status_code}): {resp.text[:500]}"}
    except Exception as e:
        return {"error": str(e)}

    parts = definition.get("definition", {}).get("parts", [])
    result = []
    for part in parts:
        path = part.get("path", "unknown")
        payload = part.get("payload", "")
        try:
            content = base64.b64decode(payload).decode("utf-8")
        except Exception:
            content = "(unable to decode)"
        result.append({"path": path, "content": content})
    return {"parts": result}


@mcp.tool()
def fabric_get_pipeline_runs(
    workspace_id: str,
    pipeline_id: str,
) -> dict:
    """Get recent pipeline run history.

    Args:
        workspace_id: The workspace GUID.
        pipeline_id: The pipeline item GUID.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances"
    try:
        resp = requests.get(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 403:
            return {"error": "Access denied to this pipeline."}
        resp.raise_for_status()
        return {"runs": resp.json().get("value", [])}
    except requests.exceptions.HTTPError as e:
        return {
            "error": f"API error ({e.response.status_code}): {e.response.text[:500]}"
        }
    except Exception as e:
        return {"error": str(e)}


@mcp.tool()
def fabric_trigger_pipeline(workspace_id: str, pipeline_id: str) -> dict:
    """Trigger a pipeline run in Fabric.

    Args:
        workspace_id: The workspace GUID.
        pipeline_id: The pipeline item GUID.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    try:
        resp = requests.post(url, headers=_request_headers(), timeout=30)
        if resp.status_code == 202:
            location = resp.headers.get("Location", "")
            result = {
                "status": "triggered",
                "message": f"Pipeline {pipeline_id} triggered.",
            }
            if location:
                result["monitor_url"] = location
            return result
        return {
            "error": f"Pipeline trigger failed ({resp.status_code}): {resp.text[:500]}"
        }
    except Exception as e:
        return {"error": str(e)}


# --- FASTAPI APP ---


class CallToolRequest(BaseModel):
    name: str
    arguments: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Discovering workspace/dataset GUIDs...")
    _discover_guids()
    logger.info(
        "Found %d workspaces, %d datasets", len(_workspace_guids), len(_dataset_guids)
    )
    async with mcp.session_manager.run():
        yield


app = FastAPI(title="connector-fabric", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.exception_handler(404)
async def custom_404(request: Request, exc):
    """Return 404 with 'error' field so MCP clients (Claude Code) can parse it
    during OAuth discovery instead of choking on FastAPI's default {"detail":"Not Found"}."""
    return JSONResponse(status_code=404, content={"error": "Not Found"})


# Mount MCP StreamableHTTP at /mcp with dual auth (Bearer + API key)
app.mount("/mcp", McpAuthMiddleware(mcp.streamable_http_app(), api_key=API_KEY))


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "workspaces": len(_workspace_guids),
        "datasets": len(_dataset_guids),
        "xmla_available": _check_xmla(),
        "dax_method": "xmla+rest" if _check_xmla() else "rest",
    }


@app.get("/tools")
async def list_tools():
    """List available tools with their parameter schemas."""
    dax_params = {
        "query": {"type": "string", "description": "DAX EVALUATE query", "required": True},
        "max_rows": {"type": "integer", "description": "Max rows to return", "default": 500},
        "dataset": {"type": "string", "description": "Dataset name", "default": DEFAULT_DATASET},
    }
    tool_schemas = {
        "fabric_dax_query": {
            "description": "Execute DAX (XMLA preferred, REST fallback)",
            "parameters": dax_params,
        },
        "fabric_dax_query_xmla": {
            "description": "Execute DAX via XMLA only (bypasses REST permission chain)",
            "parameters": dax_params,
        },
        "fabric_dax_query_rest": {
            "description": "Execute DAX via REST API only",
            "parameters": dax_params,
        },
        "fabric_list_tables": {
            "description": "List tables and columns from cached schema",
            "parameters": {
                "dataset": {
                    "type": "string",
                    "description": "Dataset name",
                    "default": DEFAULT_DATASET,
                },
            },
        },
        "fabric_get_schema": {
            "description": "Get cached schema for a dataset",
            "parameters": {
                "dataset": {
                    "type": "string",
                    "description": "Dataset name",
                    "default": DEFAULT_DATASET,
                },
            },
        },
        "fabric_list_datasets": {
            "description": "List all configured datasets grouped by workspace",
            "parameters": {},
        },
        "fabric_discover_workspaces": {
            "description": "Discover all workspaces accessible to the current identity",
            "parameters": {},
        },
    }
    return {"tools": [{"name": k, **v} for k, v in tool_schemas.items()]}


@app.post("/call-tool")
async def call_tool(req: CallToolRequest, request: Request):
    """Execute an MCP tool by name with given arguments.

    Requires X-API-Key header or valid MI JWT Bearer token for authentication.
    Returns MCP-style response.
    """
    # Auth guard — agents must authenticate via API key or MI JWT
    if API_KEY:
        provided_key = request.headers.get("x-api-key", "")
        if provided_key != API_KEY:
            # API key missing/invalid — try MI JWT as fallback
            authed_via_mi = False
            if MANAGED_IDENTITY_ENABLED:
                auth_header = request.headers.get("authorization", "")
                if auth_header.lower().startswith("bearer "):
                    from jwt_validator import validate_mi_token

                    mi_claims = validate_mi_token(auth_header[7:])
                    if mi_claims is not None:
                        authed_via_mi = True
                        logger.info(
                            "MI auth on /call-tool — appid=%s",
                            mi_claims.get("appid", mi_claims.get("azp", "unknown")),
                        )

            if not authed_via_mi:
                return JSONResponse(
                    status_code=401,
                    content={"error": "Invalid or missing X-API-Key header"},
                )

    tool_fn = TOOLS.get(req.name)
    if not tool_fn:
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "error": f"Unknown tool: {req.name}. Available: {list(TOOLS.keys())}"
                        }
                    ),
                }
            ],
            "isError": True,
        }

    try:
        result = tool_fn(**req.arguments)
        return {
            "content": [{"type": "text", "text": json.dumps(result, default=str)}],
        }
    except Exception as e:
        logger.error("Tool %s failed: %s", req.name, e)
        return {
            "content": [{"type": "text", "text": json.dumps({"error": str(e)})}],
            "isError": True,
        }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
