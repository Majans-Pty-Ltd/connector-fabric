"""One-command setup for Majans MCP connectors in Claude Code.

Run from the GitHub workspace root (the folder containing connector-d365/, connector-fabric/, etc.):

    python setup-claude-mcp.py

What it does:
  1. Checks Python dependencies (msal, requests)
  2. Locates connector repos in the current directory
  3. Adds d365, fabric, and graph entries to ~/.claude/.mcp.json
  4. Triggers first-time device-code auth for d365 and fabric (browser login)
  5. Verifies connectivity to all 3 connectors

After running, restart Claude Code. D365, Fabric, and Graph tools will be available.
"""

import importlib
import json
import os
import sys


CLAUDE_MCP_CONFIG = os.path.join(os.path.expanduser("~"), ".claude", ".mcp.json")

# Connector definitions
CONNECTORS = {
    "d365": {
        "repo": "connector-d365",
        "url": "https://d365.majans.com/mcp/",
        "client_id": "0e1a97da-1750-41d3-b11f-f2b1fb678495",
        "scopes": "https://majans.operations.dynamics.com/.default",
        "cache_dir": "~/.connector-d365",
    },
    "fabric": {
        "repo": "connector-fabric",
        "url": "https://fabric.majans.com/mcp/",
        "client_id": "cf4685ef-d594-4ede-961d-5c3554be3974",
        "scopes": "https://analysis.windows.net/powerbi/api/Dataset.Read.All https://analysis.windows.net/powerbi/api/Workspace.Read.All",
        "cache_dir": "~/.connector-fabric",
    },
}

GRAPH_CONFIG = {
    "url": "https://graph.majans.com/mcp",
    "env_var": "GRAPH_API_KEY",
}


def check_dependencies():
    """Check that msal and requests are installed."""
    missing = []
    for pkg in ["msal", "requests"]:
        try:
            importlib.import_module(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"Missing dependencies: {', '.join(missing)}")
        print(f"Install with: pip install {' '.join(missing)}")
        sys.exit(1)

    print("[OK] Dependencies: msal, requests")


def find_workspace():
    """Find the GitHub workspace root (directory containing connector-d365/)."""
    cwd = os.getcwd()

    # Check if we're in the workspace root
    if os.path.isdir(os.path.join(cwd, "connector-d365")):
        return cwd

    # Check if we're inside a repo
    parent = os.path.dirname(cwd)
    if os.path.isdir(os.path.join(parent, "connector-d365")):
        return parent

    print("ERROR: Run this script from the GitHub workspace root")
    print("       (the folder containing connector-d365/, connector-fabric/, etc.)")
    sys.exit(1)


def find_proxy_script(workspace: str, repo: str) -> str:
    """Find mcp-proxy.py in a connector repo."""
    proxy = os.path.join(workspace, repo, "mcp-proxy.py")
    if not os.path.exists(proxy):
        print(f"ERROR: {proxy} not found. Pull latest from GitHub.")
        sys.exit(1)
    return proxy


def update_mcp_config(workspace: str):
    """Add d365, fabric, and graph entries to ~/.claude/.mcp.json."""
    config_dir = os.path.dirname(CLAUDE_MCP_CONFIG)
    os.makedirs(config_dir, exist_ok=True)

    # Load existing config
    if os.path.exists(CLAUDE_MCP_CONFIG):
        with open(CLAUDE_MCP_CONFIG) as f:
            config = json.load(f)
    else:
        config = {"mcpServers": {}}

    servers = config.setdefault("mcpServers", {})
    changed = False

    # Add d365 and fabric via mcp-proxy
    for name, conn in CONNECTORS.items():
        proxy_path = find_proxy_script(workspace, conn["repo"])

        new_entry = {
            "type": "stdio",
            "command": "python",
            "args": [
                proxy_path,
                "--url", conn["url"],
                "--client-id", conn["client_id"],
                "--scopes", conn["scopes"],
                "--cache-dir", conn["cache_dir"],
            ],
        }

        if name in servers:
            # Check if it's already using mcp-proxy
            existing_args = servers[name].get("args", [])
            if "mcp-proxy.py" in str(existing_args):
                print(f"[OK] {name}: already configured with mcp-proxy")
                continue
            else:
                print(f"[UPDATE] {name}: replacing old config with mcp-proxy")
        else:
            print(f"[ADD] {name}: adding to .mcp.json")

        servers[name] = new_entry
        changed = True

    # Add graph via URL mode (API key)
    if "connector-graph" not in servers and "graph" not in servers:
        servers["connector-graph"] = {
            "url": GRAPH_CONFIG["url"],
            "headers": {
                "X-API-Key": f"${{{GRAPH_CONFIG['env_var']}}}",
            },
        }
        print(f"[ADD] connector-graph: added (set {GRAPH_CONFIG['env_var']} env var)")
        changed = True
    else:
        graph_name = "connector-graph" if "connector-graph" in servers else "graph"
        print(f"[OK] {graph_name}: already configured")

    if changed:
        with open(CLAUDE_MCP_CONFIG, "w") as f:
            json.dump(config, f, indent=2)
            f.write("\n")
        print(f"\nConfig written to {CLAUDE_MCP_CONFIG}")
    else:
        print("\nNo changes needed.")

    return changed


def trigger_auth():
    """Trigger first-time device-code auth for d365 and fabric."""
    import msal

    print("\n--- First-time authentication ---")
    print("You'll be asked to log in via browser for each connector.\n")

    tenant_id = "d54794b1-f598-4c0f-a276-6039a39774ac"

    for name, conn in CONNECTORS.items():
        cache_dir = os.path.expanduser(conn["cache_dir"])
        cache_file = os.path.join(cache_dir, "token_cache.bin")

        # Load existing cache
        cache = msal.SerializableTokenCache()
        if os.path.exists(cache_file):
            with open(cache_file) as f:
                cache.deserialize(f.read())

        app = msal.PublicClientApplication(
            conn["client_id"],
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            token_cache=cache,
        )

        scopes = conn["scopes"].split()

        # Try silent first
        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(scopes, account=accounts[0])
            if result and "access_token" in result:
                if cache.has_state_changed:
                    os.makedirs(cache_dir, exist_ok=True)
                    with open(cache_file, "w") as f:
                        f.write(cache.serialize())
                print(f"[OK] {name}: token refreshed (already authenticated)")
                continue

        # Need interactive auth
        flow = app.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            print(f"[ERROR] {name}: could not initiate device flow")
            continue

        print(f"\n[{name.upper()}] {flow['message']}")
        result = app.acquire_token_by_device_flow(flow)

        if "access_token" in result:
            os.makedirs(cache_dir, exist_ok=True)
            with open(cache_file, "w") as f:
                f.write(cache.serialize())
            print(f"[OK] {name}: authenticated successfully")
        else:
            print(f"[ERROR] {name}: {result.get('error_description', 'unknown error')}")


def verify_connectivity():
    """Quick connectivity check against each connector."""
    import requests as req

    print("\n--- Connectivity check ---")

    for name, conn in CONNECTORS.items():
        cache_dir = os.path.expanduser(conn["cache_dir"])
        cache_file = os.path.join(cache_dir, "token_cache.bin")

        if not os.path.exists(cache_file):
            print(f"[SKIP] {name}: no token cache (auth not completed)")
            continue

        import msal
        cache = msal.SerializableTokenCache()
        with open(cache_file) as f:
            cache.deserialize(f.read())

        app = msal.PublicClientApplication(
            conn["client_id"],
            authority=f"https://login.microsoftonline.com/d54794b1-f598-4c0f-a276-6039a39774ac",
            token_cache=cache,
        )

        accounts = app.get_accounts()
        if not accounts:
            print(f"[SKIP] {name}: no accounts in cache")
            continue

        result = app.acquire_token_silent(conn["scopes"].split(), account=accounts[0])
        if not result or "access_token" not in result:
            print(f"[WARN] {name}: token refresh failed — re-run setup")
            continue

        # Test tools/list
        try:
            resp = req.post(
                conn["url"],
                json={"jsonrpc": "2.0", "method": "initialize", "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "setup-check", "version": "1.0"},
                }, "id": 1},
                headers={
                    "Authorization": f"Bearer {result['access_token']}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=15,
            )
            if resp.status_code == 200:
                server_info = resp.json().get("result", {}).get("serverInfo", {})
                print(f"[OK] {name}: connected ({server_info.get('name', '?')} v{server_info.get('version', '?')})")
            else:
                print(f"[WARN] {name}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"[WARN] {name}: {e}")

    # Check graph (API key — just test reachability)
    try:
        resp = req.post(
            GRAPH_CONFIG["url"] + "/",
            json={"jsonrpc": "2.0", "method": "initialize", "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "setup-check", "version": "1.0"},
            }, "id": 1},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=15,
        )
        if resp.status_code in (200, 401, 404, 406):
            print(f"[OK] graph: reachable (set GRAPH_API_KEY env var for auth)")
        else:
            print(f"[WARN] graph: HTTP {resp.status_code}")
    except Exception as e:
        print(f"[WARN] graph: {e}")


def main():
    print("=== Majans MCP Connector Setup for Claude Code ===\n")

    check_dependencies()
    workspace = find_workspace()
    print(f"[OK] Workspace: {workspace}\n")

    update_mcp_config(workspace)
    trigger_auth()
    verify_connectivity()

    print("\n=== Setup complete ===")
    print("Restart Claude Code to activate the connectors.")
    print("D365, Fabric, and Graph tools will be available in your next session.")


if __name__ == "__main__":
    main()
