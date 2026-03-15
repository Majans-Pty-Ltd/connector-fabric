# connector-fabric — Project Instructions

## What Is This?
Python MCP server + HTTP API providing access to Microsoft Fabric semantic models — DAX queries, schema lookups, workspace discovery, pipeline operations, and dataset refresh.

**Two deployment modes**:
- **Local (stdio)**: `mcp_server.py` — full-featured, Windows-only XMLA + REST tools
- **Remote (Container App)**: `http_server.py` — REST API + StreamableHTTP MCP, Linux-compatible

## Architecture

```
AGENTS (Container Apps, internal)           TEAM MEMBERS (Windows, external)
────────────────────────────────            ────────────────────────────────
agent-scandata, agent-costing...            Claude Code
        │                                         │ stdio
        │ POST /call-tool                         ▼
        │ + X-API-Key header               start-mcp.cmd
        │ (SP token, full access)           → get-user-token.py (MSAL device code, cached)
        ▼                                   → npx mcp-remote https://fabric.majans.com/mcp
┌─────────────────────────────────────────────────────────────────┐
│  connector-fabric (Azure Container App, external ingress)       │
│  https://fabric.majans.com                                      │
│                                                                 │
│  /mcp        → FastMCP StreamableHTTP (MCP protocol)            │
│                Bearer token from user → PBI API as that user    │
│                Fabric enforces workspace roles natively          │
│                                                                 │
│  /call-tool  → Legacy REST (backward compat for agents)         │
│                X-API-Key required → SP token → PBI API          │
│                                                                 │
│  /health     → Container App probe                              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                  Power BI REST API (executeQueries)
                  Enforces workspace role per token identity
```

## Tech Stack
- Python 3.12 with `mcp` (FastMCP) — MCP server + StreamableHTTP
- `pyadomd` + `pythonnet` (CLR) — ADOMD.NET bridge for XMLA (Windows-only, `mcp_server.py`)
- `FastAPI` + `uvicorn` — HTTP server (`http_server.py`)
- `requests` — Fabric REST API calls
- Auth: Service Principal (agents) + User tokens via MSAL device code (team members)

## File Structure

```
http_server.py          # HTTP server: /call-tool (REST) + /mcp (StreamableHTTP) + /health
auth.py                 # Token context var + ASGI middleware for Bearer extraction
mcp_server.py           # Local MCP server — all tools via @mcp.tool() (stdio, Windows)
get-user-token.py       # MSAL device-code flow for user auth (client script)
start-mcp.cmd           # Wrapper: get token → launch mcp-remote as stdio proxy
schemas/                # Cached table/column/measure JSON per dataset
adomd_package/
  lib/net45/            # Bundled ADOMD.NET DLL (Windows-only)
Dockerfile              # Container image (http_server.py + auth.py + schemas)
requirements-http.txt   # Server dependencies (FastAPI, MCP, requests)
requirements-client.txt # Client dependencies (msal)
requirements.txt        # Full local dependencies
.env.template           # 1Password op:// references
.github/workflows/
  ci.yml                # Lint (ruff)
  deploy.yml            # 1Password → ACR build → Container App update
```

## Auth Model

### Agent calls (SP token, full access)
- `POST /call-tool` with `X-API-Key` header
- Server uses Service Principal client credentials for PBI API
- API key stored in 1Password: `Fabric MCP API Key`

### User calls (per-user Fabric permissions)
- `/mcp` StreamableHTTP with `Authorization: Bearer <user_token>`
- ASGI middleware extracts token → stored in `contextvars.ContextVar`
- Tool functions call PBI API with user's token
- Fabric enforces workspace roles: user with Viewer on DEMAND can query SCANv2, gets 403 on FINANCIALv2
- Entra app: `Fabric-MCP-User` (`cf4685ef-d594-4ede-961d-5c3554be3974`), public client, delegated `Dataset.Read.All` + `Workspace.Read.All`

## Configured Workspaces & Datasets

5 Fabric workspaces, 16+ datasets:

| Workspace | Datasets |
|-----------|----------|
| PRODUCT | CONSUMERv2 |
| DEMAND | SALESv2, SCANv2, STORE, SCAN TOTAL GROCERY |
| SUPPLY | AM, CUSTOMER SERVICE v2, INVENTORYV2, MANUFACTURING V3, PURCHASINGV3 |
| REVIEW | FINANCIALv2, PLANAUDIT, THREE-WAY, PRODUCTIONCOST, COSTINGv2 |
| HR | HR |

Default dataset: **SCANv2**

## HTTP Server Tools (http_server.py)

| Tool | Description |
|------|-------------|
| `fabric_dax_query` | Execute DAX query via REST API (EVALUATE syntax) |
| `fabric_list_tables` | List tables/columns from cached schema |
| `fabric_get_schema` | Get cached schema for a dataset |
| `fabric_list_datasets` | List all datasets grouped by workspace |
| `fabric_discover_workspaces` | Discover accessible workspaces (per-user when Bearer token provided) |

## Local MCP Tools (mcp_server.py)

Additional tools available in stdio mode (Windows-only XMLA):
- `fabric_list_measures` — list visible measures (live XMLA)
- `fabric_test_xmla` — test XMLA connectivity
- `fabric_refresh_schema` — live-query XMLA and update cached schema
- `fabric_list_workspace_items` — list items in workspace
- `fabric_get_refresh_history` / `fabric_trigger_refresh` — dataset refresh
- `fabric_get_pipeline_runs` / `fabric_trigger_pipeline` — pipeline operations
- `fabric_list_dataflows` / `fabric_get_dataflow_transactions` — dataflow operations

## Commands

```bash
# Local MCP server (Claude Code stdio)
python mcp_server.py

# HTTP server (local testing)
op run --env-file=.env.template -- python http_server.py

# Refresh static schemas (Windows-only, XMLA)
python scripts/refresh_schemas.py
python scripts/refresh_schemas.py SCANv2 FINANCIALv2

# User token (first-time device code auth)
pip install -r requirements-client.txt
python get-user-token.py
```

## Team Member Onboarding (5 min)

1. `pip install msal`
2. `python get-user-token.py` → follow device code prompt (browser login)
3. Add to `~/.claude/.mcp.json`:
   ```json
   "fabric": {
     "type": "stdio",
     "command": "cmd",
     "args": ["/c", "C:\\...\\connector-fabric\\start-mcp.cmd"]
   }
   ```
4. Restart Claude Code → `fabric_dax_query`, `fabric_list_datasets` etc. available

## Deployment

- **Container App**: `connector-fabric` in `rg-majans-agents` (Australia East)
- **Ingress**: External, port 8010, min 1 / max 3 replicas
- **Custom domain**: `fabric.majans.com` (managed SSL cert)
- **CI/CD**: Push to main → `deploy.yml` → 1Password secrets → ACR build → Container App update
- **GitHub secrets**: `OP_SERVICE_ACCOUNT_TOKEN`, `AZURE_CREDENTIALS` (repo-level)

## Registering with Claude Code

**Local stdio (Windows, full tools):**
```json
{
  "command": "python",
  "args": ["C:\\...\\connector-fabric\\mcp_server.py"]
}
```

**Remote via start-mcp.cmd (per-user permissions):**
```json
{
  "type": "stdio",
  "command": "cmd",
  "args": ["/c", "C:\\...\\connector-fabric\\start-mcp.cmd"]
}
```

## Key Notes
- MCP SDK requires `transport_security` with `allowed_hosts` for custom domains (DNS rebinding protection)
- `pythonnet` must be installed for XMLA tools — not in `requirements.txt`
- ADOMD.NET DLL is bundled locally (`adomd_package/lib/net45/`) — no system install needed
- XMLA tools are Windows-only, REST tools work cross-platform
- Token cache for user auth: `~/.connector-fabric/token_cache.bin`
