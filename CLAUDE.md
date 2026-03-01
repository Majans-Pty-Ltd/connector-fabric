# connector-powerbi — Project Instructions

## What Is This?
Python MCP server that gives Claude Code DAX query access to Majans' Power BI semantic models via XMLA endpoints, using a Service Principal and ADOMD.NET.

## Tech Stack
- Python 3.x with `mcp` (FastMCP) — MCP server framework
- `pyadomd` + `pythonnet` (CLR) — ADOMD.NET bridge for XMLA connections
- `ADOMD.NET` DLL — bundled locally at `adomd_package/lib/net45/` (net45 build)
- `azure-identity`, `requests` — REST API calls for workspace discovery
- `python-dotenv` — `.env` loading

## Architecture
- Single file server: `mcp_server.py` — all tools defined here with `@mcp.tool()`
- ADOMD.NET DLL must be loaded into CLR **before** `pyadomd` is imported (top of file)
- New XMLA connection opened per query (no persistent connection pool)
- Auth: client credentials → MSOLAP connection string (`User ID=app:{CLIENT_ID}@{TENANT_ID}`)
- Workspace registry hardcoded in `WORKSPACES` dict — maps short names to XMLA endpoints + datasets

## Configured Workspaces
| Key | Workspace | Dataset | Content |
|-----|-----------|---------|---------|
| SCAN | DEMAND | SCANv2 | POS retail scan data (Coles/Woolworths) |
| REVIEW | REVIEW | FINANCIALv2 | P&L, budgets, forecasts, GL |
| SUPPLY | SUPPLY | MANUFACTURING V3 | Production & supply chain |
| DEMAND | DEMAND | SALESv2 | Customer orders, invoicing |
| IT_COST | IT COST | IT COST | M365/D365/Azure spend, FY26 budget |

## MCP Tools
- `pbi_query(query, max_rows, workspace)` — execute DAX (EVALUATE syntax)
- `pbi_list_tables(workspace)` — list tables + columns + data types
- `pbi_list_measures(workspace)` — list visible measures
- `pbi_test_connection(workspace)` — test XMLA connectivity + discover datasets
- `pbi_list_workspaces()` — show all configured workspaces
- `pbi_discover_workspaces()` — REST API discovery of all SP-accessible workspaces

## Commands
```bash
# Install dependencies
pip install -r requirements.txt
# Also requires: pip install pythonnet

# Test XMLA connectivity (verifies auth + ADOMD.NET stack)
python test_connection.py

# Run example DAX queries against SCANv2
python example_queries.py

# Start MCP server (Claude Code invokes this via mcp config)
python mcp_server.py

# Explore model structure
python explore_model.py
```

## Configuration
`.env` file required (copy from `.env.example`):
```
AZURE_TENANT_ID=d54794b1-f598-4c0f-a276-6039a39774ac
AZURE_CLIENT_ID=6028b4a4-5849-4425-91fa-b1768a8b8b51
AZURE_CLIENT_SECRET=<from Entra — secret name: xmla>
# These two are used by test_connection.py / example_queries.py only:
PBI_XMLA_ENDPOINT=powerbi://api.powerbi.com/v1.0/myorg/DEMAND
PBI_DATASET_NAME=SCANv2
```
Note: `mcp_server.py` uses the workspace registry (not `PBI_XMLA_ENDPOINT`/`PBI_DATASET_NAME`).

## Registering with Claude Code
Add to `.claude.json` MCP config:
```json
{
  "command": "python",
  "args": ["C:\\Users\\Amit\\OneDrive - Majans Pty Ltd\\Documents 1\\GitHub\\connector-powerbi\\mcp_server.py"]
}
```

## Key Dependency Notes
- `pythonnet` must be installed for `clr` import to work — not in `requirements.txt`
- ADOMD.NET DLL is bundled locally (`adomd_package/lib/net45/`) — no system install needed
- Windows-only: MSOLAP provider and CLR/pythonnet require Windows
