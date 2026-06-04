"""
HTTP wrapper for connector-fabric — exposes MCP tools as HTTP endpoints
AND as a StreamableHTTP MCP server for per-user Fabric access.

Two auth paths:
  /mcp       → StreamableHTTP (MCP protocol). Three modes: Bearer MI JWT
               (validated via JWKS, SP path), Bearer user token (per-user
               Fabric calls), or X-API-Key (agent SP fallback).
  /call-tool → REST (backward compat for agents). Three modes: Bearer MI JWT
               (SP path), Bearer delegated user token (per-user Fabric calls),
               or X-API-Key (SP fallback).

DAX execution: XMLA (preferred, if ADOMD.NET available on Windows) with
automatic fallback to Power BI REST API (executeQueries) on Linux.
XMLA bypasses PBI REST API permission chain issues with composite models.
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import BaseModel

import http_client
import lro
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
            "MAGIC": "NPD pipeline, gate process, product management",
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
            "MACHINE": "OEE / SCADA machine performance — Ignition historian tags, machine states, alarms, A/P/Q measures, linked to D365 production orders",
            "MANUFACTURING V3": "Production & supply chain",
            "MCPHEE_COST": "McPhee 3PL warehousing & distribution invoice costs — pallet storage/despatch charges, cost/pallet, by warehouse",
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
    "Majans Fabric": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/Majans Fabric",
        "datasets": {
            "MajansLakehouse": "Quantium store-level data (Snowflake mirror) — daily/weekly scan, lost sales, stock, builds",
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
    """Get Service Principal token via client credentials flow.

    Stays sync because the XMLA conn-string builder needs it from sync code
    paths. Refresh-ahead: we return the cached token while >60s of life
    remains, so async tools that may run for many seconds won't get a
    just-expired token mid-call.
    """
    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["token"]

    # httpx.Client has the same explicit timeout tiers as the async client.
    with httpx.Client(timeout=http_client.TOKEN_TIMEOUT) as client:
        resp = client.post(
            f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope": "https://analysis.windows.net/powerbi/api/.default",
            },
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


async def _discover_guids() -> None:
    """Discover workspace and dataset GUIDs via REST API.

    Always uses SP token — GUIDs are universal identifiers, not per-user.
    Runs through http_client so transient capacity-throttling on startup
    doesn't permanently break dataset resolution.
    """
    global _workspace_guids, _dataset_guids
    try:
        resp = await http_client.get(
            "https://api.powerbi.com/v1.0/myorg/groups",
            headers=_sp_headers(),
        )
        resp.raise_for_status()
        for ws in resp.json().get("value", []):
            name = ws.get("name", "")
            ws_id = ws.get("id", "")
            if name.upper() in WORKSPACES:
                _workspace_guids[name.upper()] = ws_id
                logger.info("Workspace %s -> %s", name, ws_id)

                # Discover datasets in this workspace
                ds_resp = await http_client.get(
                    f"https://api.powerbi.com/v1.0/myorg/groups/{ws_id}/datasets",
                    headers=_sp_headers(),
                )
                if ds_resp.is_success:
                    for ds in ds_resp.json().get("value", []):
                        ds_name = ds.get("name", "")
                        ds_id = ds.get("id", "")
                        key = f"{name.upper()}/{ds_name.upper()}"
                        _dataset_guids[key] = ds_id
                        logger.info("  Dataset %s -> %s", ds_name, ds_id)
    except Exception as e:
        # Discovery failures are non-fatal — we'll retry on first dataset
        # resolution miss and surface the error to the caller then.
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
    """Build an XMLA connection string for the given dataset.

    Uses an MSAL access token in the Password field instead of the legacy
    app:client_id@tenant_id pattern, which is incompatible with newer
    ADOMD.NET DLLs (NuGet/SSMS 22+).
    """
    info = _resolve_dataset(dataset)
    token = _get_sp_token()
    return (
        f"Provider=MSOLAP;"
        f"Data Source={info['endpoint']};"
        f"Initial Catalog={info['dataset']};"
        f"Password={token};"
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

            return {
                "columns": headers,
                "rows": rows,
                "total_rows": len(rows_raw),
                "method": "xmla",
            }
        finally:
            conn.close()
    except Exception as e:
        return {"error": f"XMLA error: {e}", "method": "xmla"}


# --- DAX EXECUTION VIA REST API ---


async def _execute_dax_rest(
    query: str,
    dataset: str = DEFAULT_DATASET,
    max_rows: int = 500,
    ctx: Context | None = None,
) -> dict:
    """Execute DAX query via Power BI REST API executeQueries endpoint.

    Uses the calling user's token when available (per-user access control),
    falls back to SP token for agent calls. Reports progress to the MCP
    client (if ctx provided) so the connection stays warm during slow
    queries — Power BI executeQueries can legitimately take a minute or
    more on large models, and the Envoy ingress between us and the client
    closes silent sockets.
    """
    info = _resolve_dataset(dataset)
    ws_name = info["workspace"]
    ds_name = info["dataset"]

    ws_guid = _workspace_guids.get(ws_name.upper())
    ds_key = f"{ws_name.upper()}/{ds_name.upper()}"
    ds_guid = _dataset_guids.get(ds_key)

    if not ws_guid or not ds_guid:
        # Re-discover with SP token (GUID cache is universal)
        await _discover_guids()
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

    # Progress heartbeat — emits "running" every 15s while the query is in
    # flight. Even if the actual query takes 90s, the MCP client (Claude
    # Code, agent) will keep its read timeout open because progress
    # notifications can reset the deadline per spec.
    async def _heartbeat() -> None:
        steps = 0
        while True:
            await asyncio.sleep(15)
            steps += 1
            if ctx is not None:
                try:
                    await ctx.report_progress(
                        progress=steps,
                        message=f"Power BI executeQueries running (~{steps * 15}s elapsed)",
                    )
                except Exception:  # noqa: BLE001 — don't fail the query for a progress error
                    return

    heartbeat = asyncio.create_task(_heartbeat()) if ctx is not None else None

    try:
        resp = await http_client.post(
            url,
            json=payload,
            headers=_request_headers(),
            timeout=http_client.DAX_TIMEOUT,
        )
        if resp.status_code == 400:
            try:
                error_data = resp.json()
                error_msg = error_data.get("error", {}).get("message", resp.text)
            except Exception:
                error_msg = resp.text
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
        truncated = len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]

        return {
            "columns": columns,
            "rows": rows,
            "total_rows": len(rows),
            "truncated": truncated,
        }

    except http_client.CircuitOpenError as e:
        return {
            "error": f"Power BI is unreachable: {e}. Retry in ~1 minute.",
            "transient": True,
        }
    except httpx.HTTPStatusError as e:
        return {
            "error": f"REST API error ({e.response.status_code}): {e.response.text[:500]}"
        }
    except httpx.HTTPError as e:
        return {"error": f"REST transport error: {e}", "transient": True}
    except Exception as e:
        return {"error": str(e)}
    finally:
        if heartbeat is not None:
            heartbeat.cancel()
            try:
                await heartbeat
            except (asyncio.CancelledError, Exception):
                pass


# --- TOOL IMPLEMENTATIONS ---


async def _tool_fabric_dax_query(
    query: str,
    max_rows: int = 500,
    dataset: str = DEFAULT_DATASET,
    ctx: Context | None = None,
    **_,
) -> dict:
    """Execute DAX — tries XMLA first (bypasses REST permission chain), falls back to REST."""
    if _check_xmla():
        # XMLA path is sync (pyadomd CLR interop). Run in a thread so we
        # don't block the event loop.
        result = await asyncio.to_thread(_execute_dax_xmla, query, dataset, max_rows)
        if "error" not in result:
            return result
        # XMLA failed — log and fall back to REST
        logger.warning(
            "XMLA failed for %s, falling back to REST: %s",
            dataset,
            result.get("error"),
        )
    return await _execute_dax_rest(query, dataset, max_rows, ctx=ctx)


async def _tool_fabric_dax_query_xmla(
    query: str,
    max_rows: int = 500,
    dataset: str = DEFAULT_DATASET,
    ctx: Context | None = None,
    **_,
) -> dict:
    """Execute DAX via XMLA only (no REST fallback). Errors if XMLA unavailable."""
    if not _check_xmla():
        return {
            "error": "XMLA not available. Requires Windows with ADOMD.NET + pyadomd."
        }
    return await asyncio.to_thread(_execute_dax_xmla, query, dataset, max_rows)


async def _tool_fabric_dax_query_rest(
    query: str,
    max_rows: int = 500,
    dataset: str = DEFAULT_DATASET,
    ctx: Context | None = None,
    **_,
) -> dict:
    """Execute DAX via REST API only (no XMLA). For explicit REST-only calls."""
    return await _execute_dax_rest(query, dataset, max_rows, ctx=ctx)


async def _tool_fabric_list_tables(dataset: str = DEFAULT_DATASET, **_) -> dict:
    """Return schema from static cached files (no XMLA needed)."""
    info = _resolve_dataset(dataset)
    schema_path = os.path.join(SCHEMAS_DIR, f"{info['dataset']}.json")
    if not os.path.exists(schema_path):
        return {
            "error": f"No cached schema for {info['dataset']}. Available schemas: {os.listdir(SCHEMAS_DIR)}"
        }
    with open(schema_path) as f:
        return json.load(f)


async def _tool_fabric_get_schema(dataset: str = DEFAULT_DATASET, **_) -> dict:
    return await _tool_fabric_list_tables(dataset)


async def _tool_fabric_list_datasets(**_) -> dict:
    result = {}
    for ws_name, ws_info in WORKSPACES.items():
        result[ws_name] = {
            "endpoint": ws_info["endpoint"],
            "datasets": ws_info["datasets"],
        }
    return result


async def _tool_fabric_discover_workspaces(**_) -> dict:
    """Discover workspaces — uses calling user's token when available."""
    try:
        resp = await http_client.get(
            "https://api.powerbi.com/v1.0/myorg/groups",
            headers=_request_headers(),
        )
        resp.raise_for_status()
        return {"workspaces": resp.json().get("value", [])}
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
    except Exception as e:
        return {"error": str(e)}


# --- Fabric REST API tool wrappers (for /call-tool registry) ---


async def _tool_fabric_list_workspace_items(
    workspace_id: str, item_type: str = "", **_
) -> dict:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    if item_type:
        url += f"?type={item_type}"
    try:
        resp = await http_client.get(url, headers=_request_headers())
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
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
    except Exception as e:
        return {"error": str(e)}


async def _tool_fabric_get_refresh_history(
    workspace_id: str, dataset_id: str, top: int = 10, **_
) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top={top}"
    try:
        resp = await http_client.get(url, headers=_request_headers())
        resp.raise_for_status()
        return {"refreshes": resp.json().get("value", [])}
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
    except Exception as e:
        return {"error": str(e)}


async def _tool_fabric_trigger_refresh(
    workspace_id: str, dataset_id: str, wait: bool = False, **_
) -> dict:
    """Trigger dataset refresh.

    Default behaviour: fire-and-forget (returns immediately with `triggered`).
    With `wait=True`, polls upstream until completion and blocks. For the
    Claude-friendly LRO pattern (submit-then-poll), prefer the MCP tool
    `fabric_refresh_dataset` instead of /call-tool.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    try:
        resp = await http_client.post(url, headers=_request_headers())
        if resp.status_code == 202:
            return {
                "status": "triggered",
                "message": f"Refresh triggered for dataset {dataset_id}.",
            }
        return {
            "error": f"Refresh trigger failed ({resp.status_code}): {resp.text[:500]}"
        }
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
    except Exception as e:
        return {"error": str(e)}


async def _tool_fabric_list_dataflows(workspace_id: str, **_) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dataflows"
    try:
        resp = await http_client.get(url, headers=_request_headers())
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
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
    except Exception as e:
        return {"error": str(e)}


async def _tool_fabric_get_dataflow_transactions(
    workspace_id: str, dataflow_id: str, **_
) -> dict:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dataflows/{dataflow_id}/transactions"
    try:
        resp = await http_client.get(url, headers=_request_headers())
        resp.raise_for_status()
        return {"transactions": resp.json().get("value", [])}
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
    except Exception as e:
        return {"error": str(e)}


async def _tool_fabric_get_dataflow_definition(
    workspace_id: str, dataflow_id: str, ctx: Context | None = None, **_
) -> dict:
    """Fetch a Gen2 dataflow's M-query definition.

    Fabric returns 202 + Location for cold reads; we poll up to 5 minutes
    (was 60s, often insufficient on cold dataflows) and emit progress so
    the MCP client stays connected.
    """
    import base64

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{dataflow_id}/getDefinition"
    try:
        resp = await http_client.post(url, headers=_request_headers())
        if resp.status_code == 200:
            definition = resp.json()
        elif resp.status_code == 202:
            location = resp.headers.get("Location", "")
            retry_after = _safe_int(resp.headers.get("Retry-After"), default=5)
            if not location:
                return {"error": "Accepted (202) but no Location header to poll."}
            # 60 polls * up to 5s = 5 minutes worst case (capped by per-iteration
            # exponential growth of retry_after if server escalates).
            definition = None
            for attempt in range(60):
                await asyncio.sleep(retry_after)
                if ctx is not None and attempt > 0:
                    try:
                        await ctx.report_progress(
                            progress=attempt,
                            total=60,
                            message=f"Waiting for dataflow definition (attempt {attempt + 1})",
                        )
                    except Exception:  # noqa: BLE001
                        pass
                poll = await http_client.get(location, headers=_request_headers())
                if poll.status_code == 200:
                    definition = poll.json()
                    break
                if poll.status_code == 202:
                    retry_after = _safe_int(
                        poll.headers.get("Retry-After"), default=retry_after
                    )
                    continue
                return {"error": f"Poll error ({poll.status_code}): {poll.text[:500]}"}
            if definition is None:
                return {
                    "error": (
                        "Timed out waiting for dataflow definition (5 minutes). "
                        "Dataflow may be cold or capacity-throttled."
                    )
                }
        else:
            return {"error": f"API error ({resp.status_code}): {resp.text[:500]}"}
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
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


async def _tool_fabric_get_pipeline_runs(
    workspace_id: str, pipeline_id: str, **_
) -> dict:
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances"
    try:
        resp = await http_client.get(url, headers=_request_headers())
        resp.raise_for_status()
        return {"runs": resp.json().get("value", [])}
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
    except Exception as e:
        return {"error": str(e)}


async def _tool_fabric_trigger_pipeline(
    workspace_id: str, pipeline_id: str, **_
) -> dict:
    """Trigger pipeline run. Fire-and-forget.

    For status tracking use `fabric_run_pipeline` (LRO MCP tool) instead —
    it returns a job_id and polls upstream to completion.
    """
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    try:
        resp = await http_client.post(url, headers=_request_headers())
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
    except http_client.CircuitOpenError as e:
        return {"error": str(e), "transient": True}
    except Exception as e:
        return {"error": str(e)}


def _safe_int(value: Any, *, default: int) -> int:
    """Best-effort int parse — used for Retry-After which can be malformed."""
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return default


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
async def fabric_dax_query(
    query: str,
    max_rows: int = 500,
    dataset: str = "SCANv2",
    ctx: Context | None = None,
) -> dict:
    """Execute a DAX query against a Fabric semantic model.

    Uses EVALUATE syntax. Results are returned as columns + rows.
    Automatically uses XMLA (preferred, bypasses permission chain issues)
    with fallback to REST API if XMLA is unavailable. The tool emits
    progress notifications every ~15s for long-running queries so the MCP
    client keeps the connection open.
    """
    return await _tool_fabric_dax_query(
        query=query, max_rows=max_rows, dataset=dataset, ctx=ctx
    )


@mcp.tool()
async def fabric_dax_query_xmla(
    query: str,
    max_rows: int = 500,
    dataset: str = "SCANv2",
    ctx: Context | None = None,
) -> dict:
    """Execute a DAX query via XMLA/ADOMD.NET only (no REST fallback).

    XMLA connects directly to the Analysis Services endpoint using SP
    credentials. Bypasses PBI REST API permission layer — works even on
    composite models with DirectQuery chains that block the REST API.
    Requires Windows with ADOMD.NET.
    """
    return await _tool_fabric_dax_query_xmla(
        query=query, max_rows=max_rows, dataset=dataset, ctx=ctx
    )


@mcp.tool()
async def fabric_dax_query_rest(
    query: str,
    max_rows: int = 500,
    dataset: str = "SCANv2",
    ctx: Context | None = None,
) -> dict:
    """Execute a DAX query via Power BI REST API only (no XMLA).

    Uses the PBI executeQueries endpoint. Subject to PBI REST API permission
    requirements (Build permission needed on the dataset and all upstream
    models in the chain for composite/DirectQuery models).
    """
    return await _tool_fabric_dax_query_rest(
        query=query, max_rows=max_rows, dataset=dataset, ctx=ctx
    )


@mcp.tool()
async def fabric_list_tables(dataset: str = "SCANv2") -> dict:
    """List tables and columns from cached schema for a dataset."""
    return await _tool_fabric_list_tables(dataset=dataset)


@mcp.tool()
async def fabric_get_schema(dataset: str = "SCANv2") -> dict:
    """Get cached schema (tables, columns, measures) for a dataset."""
    return await _tool_fabric_get_schema(dataset=dataset)


@mcp.tool()
async def fabric_list_datasets() -> dict:
    """List all configured datasets grouped by workspace."""
    return await _tool_fabric_list_datasets()


@mcp.tool()
async def fabric_discover_workspaces() -> dict:
    """Discover all workspaces accessible to the current user.

    Returns only workspaces the calling user has permission to access.
    """
    return await _tool_fabric_discover_workspaces()


# --- FABRIC REST API TOOLS (MCP only) ---


@mcp.tool()
async def fabric_list_workspace_items(
    workspace_id: str,
    item_type: str = "",
) -> dict:
    """List items in a Fabric workspace (semantic models, pipelines, lakehouses, dataflows, etc.).

    Args:
        workspace_id: The workspace GUID (use fabric_discover_workspaces to find it).
        item_type: Optional filter — SemanticModel, DataPipeline, Lakehouse, Notebook, Dataflow, etc.
    """
    return await _tool_fabric_list_workspace_items(
        workspace_id=workspace_id, item_type=item_type
    )


@mcp.tool()
async def fabric_get_refresh_history(
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
    return await _tool_fabric_get_refresh_history(
        workspace_id=workspace_id, dataset_id=dataset_id, top=top
    )


@mcp.tool()
async def fabric_trigger_refresh(workspace_id: str, dataset_id: str) -> dict:
    """Fire-and-forget: trigger a refresh and return immediately.

    Returns `{status: triggered}` without waiting for completion. For
    end-to-end progress tracking, use `fabric_refresh_dataset` instead
    (LRO pattern — returns a job_id you can poll with `fabric_check_job`).

    Args:
        workspace_id: The workspace GUID.
        dataset_id: The dataset/semantic model GUID.
    """
    return await _tool_fabric_trigger_refresh(
        workspace_id=workspace_id, dataset_id=dataset_id
    )


@mcp.tool()
async def fabric_list_dataflows(workspace_id: str) -> dict:
    """List dataflows (Gen1 and Gen2) in a Power BI workspace.

    Args:
        workspace_id: The workspace GUID.
    """
    return await _tool_fabric_list_dataflows(workspace_id=workspace_id)


@mcp.tool()
async def fabric_get_dataflow_transactions(
    workspace_id: str,
    dataflow_id: str,
) -> dict:
    """Get transaction/refresh history for a dataflow.

    Args:
        workspace_id: The workspace GUID.
        dataflow_id: The dataflow GUID (objectId from fabric_list_dataflows).
    """
    return await _tool_fabric_get_dataflow_transactions(
        workspace_id=workspace_id, dataflow_id=dataflow_id
    )


@mcp.tool()
async def fabric_get_dataflow_definition(
    workspace_id: str,
    dataflow_id: str,
    ctx: Context | None = None,
) -> dict:
    """Get the M-query definition of a Fabric Gen2 dataflow.

    Retrieves the Power Query (M) expressions that define each table/query
    in the dataflow. Useful for inspecting filters, source connections,
    and transformation logic. On cold dataflows the API returns 202 and
    we poll for up to 5 minutes; progress notifications keep the MCP
    connection alive.

    Args:
        workspace_id: The workspace GUID.
        dataflow_id: The dataflow item GUID (from fabric_list_workspace_items).
    """
    return await _tool_fabric_get_dataflow_definition(
        workspace_id=workspace_id, dataflow_id=dataflow_id, ctx=ctx
    )


@mcp.tool()
async def fabric_get_pipeline_runs(
    workspace_id: str,
    pipeline_id: str,
) -> dict:
    """Get recent pipeline run history.

    Args:
        workspace_id: The workspace GUID.
        pipeline_id: The pipeline item GUID.
    """
    return await _tool_fabric_get_pipeline_runs(
        workspace_id=workspace_id, pipeline_id=pipeline_id
    )


@mcp.tool()
async def fabric_trigger_pipeline(workspace_id: str, pipeline_id: str) -> dict:
    """Fire-and-forget: trigger a pipeline and return immediately.

    For end-to-end status, use `fabric_run_pipeline` instead (LRO pattern —
    returns a job_id you can poll with `fabric_check_job`).

    Args:
        workspace_id: The workspace GUID.
        pipeline_id: The pipeline item GUID.
    """
    return await _tool_fabric_trigger_pipeline(
        workspace_id=workspace_id, pipeline_id=pipeline_id
    )


# --- LONG-RUNNING OPERATION (LRO) TOOLS ---
#
# Microsoft's APIs for dataset refresh and pipeline runs are async upstream:
# POST returns 202, status is polled until terminal. Wrapping that into a
# single blocking tool means a flaky ~10 minute hang on every refresh. These
# LRO tools instead return a job_id immediately and run the polling in a
# background asyncio task. Call `fabric_check_job(job_id)` to inspect.
#
# State is in-process — survives within one Container App replica's lifetime
# (~24h depending on revision rollover). If max-replicas > 1 a job submitted
# to replica A is not visible from replica B (we'll add Redis if that
# becomes load-bearing).
#
# Power BI refresh semantics
# --------------------------
# POST .../datasets/{id}/refreshes -> 202 + x-ms-request-id header (UUID)
# GET  .../datasets/{id}/refreshes?$top=N -> array; find the one whose
#      requestId matches; status field: Unknown | InProgress | Completed
#      | Failed | Disabled | Cancelled.
#
# Fabric pipeline semantics
# -------------------------
# POST .../items/{pipeline}/jobs/instances?jobType=Pipeline -> 202 + Location
#      header to the job instance URL.
# GET  {Location} -> {status: NotStarted|InProgress|Completed|Failed|...}.


# Tunables for LRO pollers — visible at top of module to make it easy to
# adjust without re-reading the worker code.
_LRO_POLL_INTERVAL_S = 15.0  # how often to poll upstream
_LRO_MAX_DURATION_S = 60 * 60  # one hour ceiling — bail out if longer
_LRO_HEARTBEAT_EVERY_POLLS = 1  # update progress on every poll


async def _refresh_worker(
    job: lro.Job, workspace_id: str, dataset_id: str
) -> dict[str, Any]:
    """LRO worker: trigger a Power BI dataset refresh and poll to completion."""
    trigger_url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/refreshes"
    )
    history_url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        f"/datasets/{dataset_id}/refreshes?$top=10"
    )

    # Step 1: trigger
    job.update(progress={"step": "triggering"})
    resp = await http_client.post(trigger_url, headers=_request_headers())
    if resp.status_code != 202:
        raise RuntimeError(
            f"Refresh trigger returned {resp.status_code}: {resp.text[:500]}"
        )
    request_id = resp.headers.get("x-ms-request-id") or resp.headers.get(
        "RequestId", ""
    )
    job.update(
        progress={
            "step": "triggered",
            "request_id": request_id,
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
        }
    )

    # Step 2: poll
    started = time.monotonic()
    polls = 0
    while True:
        if time.monotonic() - started > _LRO_MAX_DURATION_S:
            raise TimeoutError(
                f"Refresh did not complete within {_LRO_MAX_DURATION_S}s"
            )
        await asyncio.sleep(_LRO_POLL_INTERVAL_S)
        polls += 1

        hist = await http_client.get(history_url, headers=_request_headers())
        hist.raise_for_status()
        entries = hist.json().get("value", [])

        # Find our refresh — prefer matching by requestId, fall back to most
        # recent if header was absent (some Fabric replicas don't echo it).
        match: dict[str, Any] | None = None
        if request_id:
            for e in entries:
                if e.get("requestId") == request_id:
                    match = e
                    break
        if match is None and entries:
            match = entries[0]

        if match is None:
            job.update(
                progress={
                    "step": "polling",
                    "polls": polls,
                    "request_id": request_id,
                    "note": "no refresh history entries yet",
                }
            )
            continue

        status = match.get("status", "Unknown")
        job.update(
            progress={
                "step": "polling",
                "polls": polls,
                "elapsed_s": round(time.monotonic() - started, 1),
                "upstream_status": status,
                "refresh_type": match.get("refreshType"),
                "request_id": match.get("requestId"),
            }
        )

        if status == "Completed":
            return {"status": "Completed", "refresh": match}
        if status == "Failed":
            raise RuntimeError(
                f"Refresh failed upstream: {match.get('serviceExceptionJson', match)}"
            )
        if status in ("Cancelled", "Disabled"):
            raise RuntimeError(f"Refresh ended with status {status}: {match}")
        # InProgress or Unknown — keep polling


async def _pipeline_worker(
    job: lro.Job, workspace_id: str, pipeline_id: str
) -> dict[str, Any]:
    """LRO worker: trigger a Fabric pipeline run and poll the job instance to completion."""
    trigger_url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
        f"/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    )

    # Step 1: trigger
    job.update(progress={"step": "triggering"})
    resp = await http_client.post(trigger_url, headers=_request_headers())
    if resp.status_code != 202:
        raise RuntimeError(
            f"Pipeline trigger returned {resp.status_code}: {resp.text[:500]}"
        )
    monitor_url = resp.headers.get("Location", "")
    if not monitor_url:
        raise RuntimeError("Pipeline 202 response missing Location header")
    job.update(
        progress={
            "step": "triggered",
            "monitor_url": monitor_url,
            "workspace_id": workspace_id,
            "pipeline_id": pipeline_id,
        }
    )

    # Step 2: poll
    started = time.monotonic()
    polls = 0
    while True:
        if time.monotonic() - started > _LRO_MAX_DURATION_S:
            raise TimeoutError(
                f"Pipeline did not complete within {_LRO_MAX_DURATION_S}s"
            )
        await asyncio.sleep(_LRO_POLL_INTERVAL_S)
        polls += 1

        poll = await http_client.get(monitor_url, headers=_request_headers())
        poll.raise_for_status()
        body = poll.json()
        status = body.get("status", "Unknown")
        job.update(
            progress={
                "step": "polling",
                "polls": polls,
                "elapsed_s": round(time.monotonic() - started, 1),
                "upstream_status": status,
            }
        )

        if status == "Completed":
            return {"status": "Completed", "instance": body}
        if status == "Failed":
            raise RuntimeError(
                f"Pipeline failed upstream: {body.get('failureReason', body)}"
            )
        if status in ("Cancelled", "Deduped"):
            raise RuntimeError(f"Pipeline ended with status {status}: {body}")
        # NotStarted / InProgress / Unknown — keep polling


@mcp.tool()
async def fabric_refresh_dataset(workspace_id: str, dataset_id: str) -> dict:
    """Refresh a semantic model and track to completion (LRO).

    Returns a job_id immediately. The refresh runs as a background task on
    the server, polling Power BI every 15 seconds. To get the result:

        job = fabric_check_job(job_id)
        if job.status == "completed": ...
        if job.status == "running": # check again later
        if job.status == "failed": # see job.error

    Use this instead of `fabric_trigger_refresh` when you need to know
    whether the refresh actually succeeded.

    Args:
        workspace_id: The workspace GUID.
        dataset_id: The dataset/semantic model GUID.
    """
    job = lro.submit(
        name="fabric_refresh_dataset",
        coro_factory=lambda j: _refresh_worker(j, workspace_id, dataset_id),
    )
    return job.to_dict()


@mcp.tool()
async def fabric_run_pipeline(workspace_id: str, pipeline_id: str) -> dict:
    """Run a Fabric pipeline and track to completion (LRO).

    Returns a job_id immediately. The pipeline runs upstream; we poll the
    Fabric job-instance API every 15 seconds. To get the result, call
    `fabric_check_job(job_id)`.

    Use this instead of `fabric_trigger_pipeline` when you need to know
    whether the pipeline actually succeeded.

    Args:
        workspace_id: The workspace GUID.
        pipeline_id: The pipeline item GUID.
    """
    job = lro.submit(
        name="fabric_run_pipeline",
        coro_factory=lambda j: _pipeline_worker(j, workspace_id, pipeline_id),
    )
    return job.to_dict()


@mcp.tool()
async def fabric_check_job(job_id: str) -> dict:
    """Check on a previously-submitted long-running job.

    Returns the job's current state:
    - status: pending | running | completed | failed | cancelled
    - progress: arbitrary dict from the worker (varies by job type)
    - result: payload when status == completed
    - error: message when status == failed

    For `fabric_refresh_dataset` and `fabric_run_pipeline`, the typical
    flow is: submit -> check every 30-60s -> get result.
    """
    job = lro.get(job_id)
    if job is None:
        return {"error": f"Unknown job_id: {job_id}", "status": "unknown"}
    return job.to_dict()


@mcp.tool()
async def fabric_list_jobs() -> dict:
    """List all known long-running jobs (newest first).

    Useful for finding the job_id of a job submitted earlier, or for
    spotting stuck jobs.
    """
    return {"jobs": lro.snapshot()}


@mcp.tool()
async def fabric_cancel_job(job_id: str) -> dict:
    """Cancel a running long-running job.

    Best-effort: cancellation aborts the local polling loop but does NOT
    cancel the underlying upstream operation (Power BI / Fabric will
    finish what they started).
    """
    cancelled = lro.cancel(job_id)
    job = lro.get(job_id)
    return {
        "cancelled": cancelled,
        "job": job.to_dict() if job else None,
    }


# --- FASTAPI APP ---


class CallToolRequest(BaseModel):
    name: str
    arguments: dict = {}


async def _lro_gc_loop() -> None:
    """Periodically purge expired LRO jobs so the registry doesn't grow forever."""
    while True:
        try:
            await asyncio.sleep(60 * 60)  # hourly
            lro.gc()
        except asyncio.CancelledError:
            return
        except Exception:  # noqa: BLE001
            logger.exception("LRO gc loop error — continuing")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Discovering workspace/dataset GUIDs...")
    await _discover_guids()
    logger.info(
        "Found %d workspaces, %d datasets", len(_workspace_guids), len(_dataset_guids)
    )

    # Background LRO janitor — keeps the in-process job dict bounded.
    gc_task = asyncio.create_task(_lro_gc_loop(), name="lro-gc")

    try:
        async with mcp.session_manager.run():
            yield
    finally:
        # Graceful shutdown order:
        # 1. Stop the GC loop.
        # 2. Cancel any in-flight LRO worker tasks (best-effort; their state
        #    is in-process so it would be lost anyway).
        # 3. Close the shared httpx client to drain in-flight connections.
        gc_task.cancel()
        try:
            await gc_task
        except (asyncio.CancelledError, Exception):
            pass
        # Cancel outstanding jobs so their tasks don't leak.
        for job_dict in lro.snapshot():
            jid = job_dict.get("job_id")
            if jid and job_dict.get("status") in ("pending", "running"):
                lro.cancel(jid)
        await http_client.close_client()
        logger.info("Shutdown complete")


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
    """Liveness + reliability diagnostics.

    Exposes circuit-breaker state and live LRO job count so operators can
    spot degraded downstreams or runaway jobs from outside.
    """
    breaker_state = http_client.get_breaker_state()
    # If ANY downstream circuit is open we're in a degraded state but still
    # alive — Container App liveness probes should treat 200 as ok regardless.
    any_open = any(b.get("state") == "open" for b in breaker_state.values())
    jobs = lro.snapshot()
    job_counts: dict[str, int] = {}
    for j in jobs:
        s = j.get("status", "unknown")
        job_counts[s] = job_counts.get(s, 0) + 1
    return {
        "status": "degraded" if any_open else "ok",
        "workspaces": len(_workspace_guids),
        "datasets": len(_dataset_guids),
        "xmla_available": _check_xmla(),
        "dax_method": "xmla+rest" if _check_xmla() else "rest",
        "circuit_breakers": breaker_state,
        "lro_jobs": job_counts,
    }


@app.get("/tools")
async def list_tools():
    """List available tools with their parameter schemas."""
    dax_params = {
        "query": {
            "type": "string",
            "description": "DAX EVALUATE query",
            "required": True,
        },
        "max_rows": {
            "type": "integer",
            "description": "Max rows to return",
            "default": 500,
        },
        "dataset": {
            "type": "string",
            "description": "Dataset name",
            "default": DEFAULT_DATASET,
        },
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

    Auth modes (checked in order):
      1. Bearer token → MI JWT validation (if enabled) → SP path
      2. Bearer token → delegated user token → user_token_var (per-user Fabric permissions)
      3. X-API-Key → SP path (backward compat for agents)
    """
    # --- Auth guard ---
    auth_header = request.headers.get("authorization", "")
    has_bearer = auth_header.lower().startswith("bearer ")
    delegated_token: str | None = None

    if has_bearer:
        token = auth_header[7:]

        # Try MI JWT first (service-to-service)
        if MANAGED_IDENTITY_ENABLED:
            from jwt_validator import validate_mi_token

            mi_claims = validate_mi_token(token)
            if mi_claims is not None:
                logger.info(
                    "MI auth on /call-tool — appid=%s",
                    mi_claims.get("appid", mi_claims.get("azp", "unknown")),
                )
                # Valid MI → proceed on SP path (no user_token_var)
            else:
                # Not a valid MI JWT → treat as delegated user token
                delegated_token = token
                logger.info("Delegated user auth on /call-tool")
        else:
            # MI not enabled → any Bearer is a delegated user token
            delegated_token = token
            logger.info("Delegated user auth on /call-tool (MI disabled)")

    elif API_KEY:
        # No Bearer — fall back to X-API-Key
        provided_key = request.headers.get("x-api-key", "")
        if provided_key != API_KEY:
            return JSONResponse(
                status_code=401,
                content={
                    "error": "Invalid or missing authentication (Bearer token or X-API-Key)"
                },
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

    # If delegated token, set context var so tool functions use user's Fabric permissions
    tok = user_token_var.set(delegated_token) if delegated_token else None
    started = time.monotonic()
    try:
        # All TOOLS entries are now async coroutines — await unconditionally.
        result = await tool_fn(**req.arguments)
        duration_ms = (time.monotonic() - started) * 1000
        if duration_ms > 5000:
            logger.info("Tool %s completed in %.0fms", req.name, duration_ms)
        return {
            "content": [{"type": "text", "text": json.dumps(result, default=str)}],
        }
    except http_client.CircuitOpenError as e:
        logger.warning("Tool %s short-circuited: %s", req.name, e)
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {"error": str(e), "transient": True, "tool": req.name}
                    ),
                }
            ],
            "isError": True,
        }
    except Exception as e:
        duration_ms = (time.monotonic() - started) * 1000
        logger.error("Tool %s failed in %.0fms: %s", req.name, duration_ms, e)
        return {
            "content": [{"type": "text", "text": json.dumps({"error": str(e)})}],
            "isError": True,
        }
    finally:
        if tok is not None:
            user_token_var.reset(tok)


if __name__ == "__main__":
    import uvicorn

    # Tunings vs uvicorn defaults:
    # - timeout_keep_alive=75s — exceeds typical AWS/Azure ELB idle defaults
    #   so keep-alive sockets don't break mid-conversation.
    # - timeout_graceful_shutdown=30s — gives in-flight LRO and DAX calls a
    #   chance to finish before the worker is killed on SIGTERM. Container
    #   Apps' default terminationGracePeriodSeconds is 30, so we match.
    # - access_log=False — FastMCP and our tool functions already log
    #   structured info; Uvicorn's access log is noisy.
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=PORT,
        timeout_keep_alive=75,
        timeout_graceful_shutdown=30,
        access_log=False,
        log_level="info",
    )
    # uvicorn.Server.run() installs its own SIGINT/SIGTERM handlers that
    # flip `should_exit`, which causes the running app's lifespan to exit
    # cleanly. Our lifespan finally-block then closes http_client and
    # cancels outstanding LRO tasks. No extra signal wiring needed.
    uvicorn.Server(config).run()
