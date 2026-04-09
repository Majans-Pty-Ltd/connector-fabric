# Connect to Microsoft Fabric from Claude Code

This guide sets up connector-fabric so you can use Fabric tools (DAX queries, workspace discovery, dataset schemas) from Claude Code — using **your own Majans account**. You only see workspaces you have permission to access.

## Prerequisites

1. **Claude Code** installed and working
2. **Python 3.10+** — check with `python --version`
3. **Python packages** — `pip install msal requests`

Node.js is no longer required.

## Quick Setup (one command)

From the GitHub workspace root (the folder containing `connector-d365/`, `connector-fabric/`, etc.):

```bash
pip install msal requests
python setup-claude-mcp.py
```

This configures D365, Fabric, and Graph connectors in one step. Follow the device-code prompts to authenticate. Then restart Claude Code.

## Manual Setup

### Step 1: Install dependencies

```bash
pip install msal requests
```

### Step 2: Add to Claude Code MCP config

Edit `~/.claude/.mcp.json` and add (adjust the path to where you cloned this repo):

```json
{
  "fabric": {
    "type": "stdio",
    "command": "python",
    "args": [
      "C:\\path\\to\\connector-fabric\\mcp-proxy.py",
      "--url", "https://fabric.majans.com/mcp/",
      "--client-id", "cf4685ef-d594-4ede-961d-5c3554be3974",
      "--scopes", "https://analysis.windows.net/powerbi/api/Dataset.Read.All https://analysis.windows.net/powerbi/api/Workspace.Read.All",
      "--cache-dir", "~/.connector-fabric"
    ]
  }
}
```

### Step 3: Restart Claude Code and authenticate

On first launch, Claude Code will show a device-code prompt in the MCP server logs:

```
To sign in, use a web browser to open the page
https://login.microsoft.com/device and enter the code XXXXXXXX
```

Sign in with your **Majans Entra ID** (e.g. `yourname@majans.com`). Token is cached and auto-refreshes on every call — no more mid-session expiry.

### Step 4: Verify it works

In Claude Code, ask:

> "List my Fabric workspaces"

Claude will call `fabric_discover_workspaces` and should return only the workspaces **you** have access to.

## What you can do

| Category | Example prompts |
|----------|----------------|
| **DAX Queries** | "Run a DAX query on the DEMAND dataset" |
| **Workspaces** | "List my Fabric workspaces" |
| **Datasets** | "Show tables in the SCANv2 dataset" |
| **Schema** | "What columns are in the STORE table?" |
| **Refresh** | "Trigger a refresh on the SALESv2 dataset" |

## What you CAN'T see (by design)

With delegated auth, you only access **workspaces you have permission to** in Fabric:

- You can query datasets in workspaces where you're a Viewer, Contributor, or Admin
- You cannot see workspaces you haven't been granted access to
- You cannot trigger refreshes unless you have Contributor or Admin role

This matches the same permissions you'd see in the Fabric portal.

## How It Works

1. `mcp-proxy.py` bridges Claude Code stdio to the remote Fabric MCP server over HTTPS
2. Before each call, MSAL silently refreshes your token from cache (no expiry mid-session)
3. Your token is sent as a Bearer header → Fabric API runs as you
4. Fabric enforces your workspace roles — you only see what your role permits

## Troubleshooting

### Token expired / need to re-authenticate
Delete the cached token and restart Claude Code:
```
del %USERPROFILE%\.connector-fabric\token_cache.bin
```
You'll get a new device-code prompt on next launch.

### "Access denied" on a specific workspace or dataset
Your Entra ID account doesn't have Fabric permission for that resource. Ask Amit to grant access in the Fabric portal (Workspace > Manage access).

### Claude Code doesn't show Fabric tools
1. Check that the MCP entry in `~/.claude/.mcp.json` has the correct path to `mcp-proxy.py`
2. Restart Claude Code
3. Check that Python is on your PATH

## Need help?

Contact Amit (amit@majans.com) for:
- Fabric workspace access requests
- Adding the connector to a new machine
- Reporting bugs or unexpected behavior
