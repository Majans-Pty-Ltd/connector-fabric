"""MSAL device-code flow for Fabric MCP user authentication.

First run: prompts user to visit microsoft.com/devicelogin and enter a code.
Subsequent runs: silently refreshes via cached refresh token (~1 hour tokens).

Outputs the access token to stdout (consumed by start-mcp.cmd).
All user-facing messages go to stderr so stdout is clean for piping.

Prerequisites:
  pip install msal
  Entra app "Fabric-MCP-User" with delegated PBI permissions + public client flow enabled.

Environment variables (optional — defaults to Majans tenant):
  FABRIC_MCP_CLIENT_ID  — Entra app client ID for Fabric-MCP-User
  AZURE_TENANT_ID       — Entra tenant ID
"""

import json
import os
import sys

import msal

# Entra app "Fabric-MCP-User" — set after creating the app in Azure Portal
CLIENT_ID = os.getenv("FABRIC_MCP_CLIENT_ID", "cf4685ef-d594-4ede-961d-5c3554be3974")
TENANT_ID = os.getenv("AZURE_TENANT_ID", "d54794b1-f598-4c0f-a276-6039a39774ac")

SCOPES = [
    "https://analysis.windows.net/powerbi/api/Dataset.Read.All",
    "https://analysis.windows.net/powerbi/api/Workspace.Read.All",
]

CACHE_DIR = os.path.join(os.path.expanduser("~"), ".connector-fabric")
CACHE_FILE = os.path.join(CACHE_DIR, "token_cache.bin")


def _load_cache() -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            cache.deserialize(f.read())
    return cache


def _save_cache(cache: msal.SerializableTokenCache):
    if cache.has_state_changed:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            f.write(cache.serialize())


def main():
    if not CLIENT_ID:
        print(
            "ERROR: FABRIC_MCP_CLIENT_ID not set. "
            "Create the Entra app 'Fabric-MCP-User' and set this env var to its client ID.",
            file=sys.stderr,
        )
        sys.exit(1)

    cache = _load_cache()
    app = msal.PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        token_cache=cache,
    )

    # Try silent token acquisition first (cached refresh token)
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result and "access_token" in result:
            _save_cache(cache)
            print(result["access_token"])
            return

    # Fall back to device code flow (interactive, first time only)
    flow = app.initiate_device_flow(scopes=SCOPES)
    if "user_code" not in flow:
        print(
            f"ERROR: Could not initiate device flow: {json.dumps(flow, indent=2)}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Show device code prompt on stderr (stdout reserved for token)
    print(flow["message"], file=sys.stderr)

    result = app.acquire_token_by_device_flow(flow)
    if "access_token" in result:
        _save_cache(cache)
        print(result["access_token"])
    else:
        print(
            f"ERROR: {result.get('error_description', json.dumps(result))}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
