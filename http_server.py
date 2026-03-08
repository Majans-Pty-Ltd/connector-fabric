"""
HTTP wrapper for connector-fabric — exposes MCP tools as HTTP endpoints.

Designed for deployment as an Azure Container App so that agent-scandata
can call tools via HTTP POST /call-tool.

DAX execution uses the Power BI REST API (executeQueries) instead of XMLA,
so this runs on Linux without Windows/.NET dependencies.
"""

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import requests
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("connector-fabric")

# --- CONFIG ---
TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "")
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


def _get_token() -> str:
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


def _headers() -> dict:
    return {"Authorization": f"Bearer {_get_token()}"}


# --- WORKSPACE/DATASET GUID DISCOVERY ---


def _discover_guids():
    """Discover workspace and dataset GUIDs via REST API."""
    global _workspace_guids, _dataset_guids
    try:
        resp = requests.get(
            "https://api.powerbi.com/v1.0/myorg/groups",
            headers=_headers(),
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
                    headers=_headers(),
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


# --- DAX EXECUTION VIA REST API ---


def _execute_dax_rest(query: str, dataset: str = DEFAULT_DATASET, max_rows: int = 500) -> dict:
    """Execute DAX query via Power BI REST API executeQueries endpoint."""
    info = _resolve_dataset(dataset)
    ws_name = info["workspace"]
    ds_name = info["dataset"]

    ws_guid = _workspace_guids.get(ws_name.upper())
    ds_key = f"{ws_name.upper()}/{ds_name.upper()}"
    ds_guid = _dataset_guids.get(ds_key)

    if not ws_guid or not ds_guid:
        # Try rediscovery
        _discover_guids()
        ws_guid = _workspace_guids.get(ws_name.upper())
        ds_guid = _dataset_guids.get(ds_key)
        if not ws_guid or not ds_guid:
            return {"error": f"Could not resolve GUIDs for {ws_name}/{ds_name}. "
                    f"Available workspaces: {list(_workspace_guids.keys())}, "
                    f"datasets: {list(_dataset_guids.keys())}"}

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{ws_guid}/datasets/{ds_guid}/executeQueries"
    payload = {
        "queries": [{"query": query}],
        "serializerSettings": {"includeNulls": True},
    }

    try:
        resp = requests.post(url, json=payload, headers=_headers(), timeout=120)
        if resp.status_code == 400:
            error_data = resp.json()
            error_msg = error_data.get("error", {}).get("message", resp.text)
            return {"error": f"DAX error: {error_msg}", "query": query}
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
        return {"error": f"REST API error ({e.response.status_code}): {e.response.text[:500]}"}
    except Exception as e:
        return {"error": str(e)}


# --- TOOL IMPLEMENTATIONS ---

def _tool_fabric_dax_query(query: str, max_rows: int = 500, dataset: str = DEFAULT_DATASET, **_) -> dict:
    return _execute_dax_rest(query, dataset, max_rows)


def _tool_fabric_list_tables(dataset: str = DEFAULT_DATASET, **_) -> dict:
    """Return schema from static cached files (no XMLA needed)."""
    info = _resolve_dataset(dataset)
    schema_path = os.path.join(SCHEMAS_DIR, f"{info['dataset']}.json")
    if not os.path.exists(schema_path):
        return {"error": f"No cached schema for {info['dataset']}. Available schemas: {os.listdir(SCHEMAS_DIR)}"}
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
    try:
        resp = requests.get(
            "https://api.powerbi.com/v1.0/myorg/groups",
            headers=_headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return {"workspaces": resp.json().get("value", [])}
    except Exception as e:
        return {"error": str(e)}


# Tool registry
TOOLS: dict[str, callable] = {
    "fabric_dax_query": _tool_fabric_dax_query,
    "fabric_list_tables": _tool_fabric_list_tables,
    "fabric_get_schema": _tool_fabric_get_schema,
    "fabric_list_datasets": _tool_fabric_list_datasets,
    "fabric_discover_workspaces": _tool_fabric_discover_workspaces,
}


# --- FASTAPI APP ---

class CallToolRequest(BaseModel):
    name: str
    arguments: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Discovering workspace/dataset GUIDs...")
    _discover_guids()
    logger.info("Found %d workspaces, %d datasets", len(_workspace_guids), len(_dataset_guids))
    yield


app = FastAPI(title="connector-fabric", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "workspaces": len(_workspace_guids),
        "datasets": len(_dataset_guids),
    }


@app.get("/tools")
async def list_tools():
    """List available tools with their parameter schemas."""
    tool_schemas = {
        "fabric_dax_query": {
            "description": "Execute a DAX query against a Fabric semantic model",
            "parameters": {
                "query": {"type": "string", "description": "DAX EVALUATE query", "required": True},
                "max_rows": {"type": "integer", "description": "Max rows to return", "default": 500},
                "dataset": {"type": "string", "description": "Dataset name", "default": DEFAULT_DATASET},
            },
        },
        "fabric_list_tables": {
            "description": "List tables and columns from cached schema",
            "parameters": {
                "dataset": {"type": "string", "description": "Dataset name", "default": DEFAULT_DATASET},
            },
        },
        "fabric_get_schema": {
            "description": "Get cached schema for a dataset",
            "parameters": {
                "dataset": {"type": "string", "description": "Dataset name", "default": DEFAULT_DATASET},
            },
        },
        "fabric_list_datasets": {
            "description": "List all configured datasets grouped by workspace",
            "parameters": {},
        },
        "fabric_discover_workspaces": {
            "description": "Discover all workspaces accessible to the service principal",
            "parameters": {},
        },
    }
    return {"tools": [{"name": k, **v} for k, v in tool_schemas.items()]}


@app.post("/call-tool")
async def call_tool(req: CallToolRequest):
    """Execute an MCP tool by name with given arguments. Returns MCP-style response."""
    tool_fn = TOOLS.get(req.name)
    if not tool_fn:
        return {
            "content": [{"type": "text", "text": json.dumps({"error": f"Unknown tool: {req.name}. Available: {list(TOOLS.keys())}"})}],
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
