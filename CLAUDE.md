# connector-fabric — Project Instructions

## What Is This?
Python MCP server providing Claude Code with access to Microsoft Fabric artefacts — semantic models (DAX via XMLA), workspace management, pipeline operations, dataset refresh, and item discovery via the Fabric REST API.

## Tech Stack
- Python 3.x with `mcp` (FastMCP) — MCP server framework
- `pyadomd` + `pythonnet` (CLR) — ADOMD.NET bridge for XMLA connections (Windows-only)
- `ADOMD.NET` DLL — bundled locally at `adomd_package/lib/net45/` (net45 build)
- `requests` — Fabric REST API calls
- `python-dotenv` — `.env` loading

## Architecture
- Single file server: `mcp_server.py` — all tools defined here with `@mcp.tool()`
- **Two API paths**:
  - **XMLA** (DAX queries): lazy-loaded ADOMD.NET via `_ensure_xmla()` — Windows-only, loads CLR on first XMLA tool call
  - **REST** (Fabric API): `_get_fabric_token()` with token caching — cross-platform
- Auth: Service Principal (client credentials) for both paths
- Workspace registry hardcoded in `WORKSPACES` dict — maps short names to XMLA endpoints + datasets

## Configured Workspaces (XMLA)
| Key | Workspace | Dataset | Content |
|-----|-----------|---------|---------|
| SCAN | DEMAND | SCANv2 | POS retail scan data (Coles/Woolworths) |
| REVIEW | REVIEW | FINANCIALv2 | P&L, budgets, forecasts, GL |
| SUPPLY | SUPPLY | MANUFACTURING V3 | Production & supply chain |
| PURCHASING | SUPPLY | PURCHASINGV3 | Vendor SIFOT/DIFOT, PO delivery performance, supplier scoring |
| DEMAND | DEMAND | SALESv2 | Customer orders, invoicing |
| IT_COST | IT COST | IT COST | M365/D365/Azure spend, FY26 budget |

## MCP Tools

### XMLA (DAX Queries)
- `fabric_dax_query(query, max_rows, workspace)` — execute DAX (EVALUATE syntax)
- `fabric_list_tables(workspace)` — list tables + columns + data types
- `fabric_list_measures(workspace)` — list visible measures
- `fabric_test_xmla(workspace)` — test XMLA connectivity + discover datasets

### Workspace Discovery
- `fabric_list_configured_workspaces()` — show configured XMLA workspaces
- `fabric_discover_workspaces()` — REST API discovery of all SP-accessible workspaces

### Fabric REST API
- `fabric_list_workspace_items(workspace_id, item_type?)` — list items (semantic models, pipelines, lakehouses, etc.)
- `fabric_get_refresh_history(workspace_id, dataset_id, top)` — dataset refresh history
- `fabric_trigger_refresh(workspace_id, dataset_id)` — trigger semantic model refresh
- `fabric_get_pipeline_runs(workspace_id, pipeline_id)` — pipeline run history
- `fabric_trigger_pipeline(workspace_id, pipeline_id)` — trigger pipeline run

## Commands
```bash
# Install dependencies
pip install -r requirements.txt
# Also requires: pip install pythonnet (for XMLA tools, Windows-only)

# Test XMLA connectivity (verifies auth + ADOMD.NET stack)
python scripts/test_connection.py

# Run example DAX queries against SCANv2
python scripts/example_queries.py

# Explore model structure
python scripts/explore_model.py

# Start MCP server (Claude Code invokes this via mcp config)
python mcp_server.py
```

## Configuration
`.env` file required (copy from `.env.example`):
```
AZURE_TENANT_ID=d54794b1-f598-4c0f-a276-6039a39774ac
AZURE_CLIENT_ID=6028b4a4-5849-4425-91fa-b1768a8b8b51
AZURE_CLIENT_SECRET=<from Entra — secret name: xmla>
# These two are used by scripts/test_connection.py and scripts/example_queries.py only:
PBI_XMLA_ENDPOINT=powerbi://api.powerbi.com/v1.0/myorg/DEMAND
PBI_DATASET_NAME=SCANv2
```
Note: `mcp_server.py` uses the workspace registry (not `PBI_XMLA_ENDPOINT`/`PBI_DATASET_NAME`).

## Registering with Claude Code
Add to `.claude.json` MCP config:
```json
{
  "command": "python",
  "args": ["C:\\Users\\Amit\\OneDrive - Majans Pty Ltd\\Documents 1\\GitHub\\connector-fabric\\mcp_server.py"]
}
```

## Key Dependency Notes
- `pythonnet` must be installed for `clr` import to work — not in `requirements.txt`
- ADOMD.NET DLL is bundled locally (`adomd_package/lib/net45/`) — no system install needed
- XMLA tools are Windows-only (MSOLAP + CLR/pythonnet), REST tools work cross-platform
- ADOMD.NET is lazy-loaded — REST-only tools work without pythonnet installed
