"""MCP stdio-to-HTTP proxy with built-in MSAL token refresh.

Replaces get-user-token.py + mcp-remote with a single, reliable script.
Bridges Claude Code's stdio MCP transport to a remote StreamableHTTP server,
auto-refreshing the delegated user token before each call.

Usage:
  python mcp-proxy.py \
    --url https://d365.majans.com/mcp/ \
    --client-id 0e1a97da-1750-41d3-b11f-f2b1fb678495 \
    --scopes "https://majans.operations.dynamics.com/.default" \
    --cache-dir ~/.connector-d365

Prerequisites: pip install msal requests
"""

import argparse
import json
import os
import sys
import threading
import time

import msal
import requests


def log(msg: str):
    """Log to stderr (stdout reserved for MCP JSON-RPC)."""
    print(f"[mcp-proxy] {msg}", file=sys.stderr, flush=True)


class TokenManager:
    """MSAL token manager with silent refresh."""

    def __init__(self, client_id: str, tenant_id: str, scopes: list[str], cache_dir: str):
        self.scopes = scopes
        self.cache_dir = os.path.expanduser(cache_dir)
        self.cache_file = os.path.join(self.cache_dir, "token_cache.bin")

        self.cache = msal.SerializableTokenCache()
        if os.path.exists(self.cache_file):
            with open(self.cache_file) as f:
                self.cache.deserialize(f.read())

        self.app = msal.PublicClientApplication(
            client_id,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            token_cache=self.cache,
        )

    def get_token(self) -> str:
        """Get a valid access token, refreshing silently if possible."""
        accounts = self.app.get_accounts()
        if accounts:
            result = self.app.acquire_token_silent(self.scopes, account=accounts[0])
            if result and "access_token" in result:
                self._save_cache()
                return result["access_token"]

        # No cached token — need interactive device code flow
        flow = self.app.initiate_device_flow(scopes=self.scopes)
        if "user_code" not in flow:
            raise RuntimeError(f"Could not initiate device flow: {json.dumps(flow)}")

        log(flow["message"])
        result = self.app.acquire_token_by_device_flow(flow)
        if "access_token" in result:
            self._save_cache()
            return result["access_token"]

        raise RuntimeError(f"Auth failed: {result.get('error_description', json.dumps(result))}")

    def _save_cache(self):
        if self.cache.has_state_changed:
            os.makedirs(self.cache_dir, exist_ok=True)
            with open(self.cache_file, "w") as f:
                f.write(self.cache.serialize())


class McpProxy:
    """Bridges stdio JSON-RPC to HTTP StreamableHTTP with token refresh."""

    def __init__(self, server_url: str, token_manager: TokenManager):
        self.server_url = server_url
        self.token_manager = token_manager
        self.session_id: str | None = None
        self.http = requests.Session()
        self.http.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        })

    def _get_headers(self) -> dict:
        token = self.token_manager.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        return headers

    def forward(self, message: dict) -> None:
        """Forward a JSON-RPC message to the HTTP server and write response to stdout."""
        try:
            resp = self.http.post(
                self.server_url,
                json=message,
                headers=self._get_headers(),
                timeout=120,
            )

            # Capture session ID from server
            if "mcp-session-id" in resp.headers:
                self.session_id = resp.headers["mcp-session-id"]

            content_type = resp.headers.get("content-type", "")

            if resp.status_code >= 400:
                error_resp = {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "error": {
                        "code": -32000,
                        "message": f"HTTP {resp.status_code}: {resp.text[:500]}",
                    },
                }
                self._write(error_resp)
                return

            if "text/event-stream" in content_type:
                # Parse SSE response — each "data:" line is a JSON-RPC message
                for line in resp.text.split("\n"):
                    line = line.strip()
                    if line.startswith("data:"):
                        data = line[5:].strip()
                        if data:
                            try:
                                parsed = json.loads(data)
                                self._write(parsed)
                            except json.JSONDecodeError:
                                pass
            else:
                # JSON response
                self._write(resp.json())

        except requests.exceptions.Timeout:
            self._write({
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {"code": -32000, "message": "Request timed out (120s)"},
            })
        except requests.exceptions.ConnectionError as e:
            self._write({
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {"code": -32000, "message": f"Connection error: {e}"},
            })

    def _write(self, message: dict) -> None:
        """Write a JSON-RPC message to stdout."""
        sys.stdout.write(json.dumps(message) + "\n")
        sys.stdout.flush()

    def run(self) -> None:
        """Main loop: read stdin, forward to HTTP, write to stdout."""
        log(f"Connected to {self.server_url}")
        log("Listening on stdin for JSON-RPC messages...")

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
                self.forward(message)
            except json.JSONDecodeError as e:
                log(f"Invalid JSON on stdin: {e}")
            except Exception as e:
                log(f"Error: {e}")
                # Try to send error response if we have a message ID
                try:
                    msg = json.loads(line)
                    self._write({
                        "jsonrpc": "2.0",
                        "id": msg.get("id"),
                        "error": {"code": -32603, "message": str(e)},
                    })
                except Exception:
                    pass

        log("stdin closed, shutting down.")


def main():
    parser = argparse.ArgumentParser(description="MCP stdio-to-HTTP proxy with MSAL token refresh")
    parser.add_argument("--url", required=True, help="Remote MCP server URL (e.g. https://d365.majans.com/mcp/)")
    parser.add_argument("--client-id", required=True, help="Entra app client ID for delegated auth")
    parser.add_argument("--tenant-id", default="d54794b1-f598-4c0f-a276-6039a39774ac", help="Entra tenant ID")
    parser.add_argument("--scopes", required=True, help="Space-separated OAuth scopes")
    parser.add_argument("--cache-dir", required=True, help="Directory for MSAL token cache (e.g. ~/.connector-d365)")
    args = parser.parse_args()

    scopes = args.scopes.split()

    token_manager = TokenManager(
        client_id=args.client_id,
        tenant_id=args.tenant_id,
        scopes=scopes,
        cache_dir=args.cache_dir,
    )

    # Verify we can get a token before starting
    try:
        token_manager.get_token()
        log("Token acquired successfully.")
    except Exception as e:
        log(f"FATAL: {e}")
        sys.exit(1)

    proxy = McpProxy(server_url=args.url, token_manager=token_manager)
    proxy.run()


if __name__ == "__main__":
    main()
