"""Token context and auth helpers for connector-fabric.

Auth precedence (highest first), shared by /mcp and /call-tool:
  1. X-API-Key  → agent path (SP token, full access). If header is present
                  it must match — no fallback to Bearer if it doesn't.
  2. Bearer MI JWT (when MANAGED_IDENTITY_ENABLED) → SP path, validated
                  against Azure AD JWKS for api://<sp-app-id>.
  3. Bearer vault/user token → validated against Microsoft JWKS for the
                  Power BI XMLA audience and one of VAULT_ALLOWED_APP_IDS,
                  then stored in user_token_var so Fabric API calls run
                  as that user. This path serves both local Claude Code
                  users and Anthropic Managed Agents Vaults.
  4. No auth + no API key configured → dev mode (allow).
  5. Otherwise → 401.
"""

import contextvars
import logging
import os
from dataclasses import dataclass

# Per-request user token — set by ASGI middleware, read by _request_headers()
user_token_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "user_token", default=None
)

logger = logging.getLogger("connector-fabric.auth")

# Feature flag — opt-in to Managed Identity JWT validation
MANAGED_IDENTITY_ENABLED = (
    os.getenv("MANAGED_IDENTITY_ENABLED", "false").lower() in ("true", "1", "yes")
)

# Feature flag — strict validation of vault/delegated Bearer tokens.
# Default ON: tokens issued to Fabric-MCP-User (the only delegated app we
# support today) pass validation, so existing local Claude Code users are
# unaffected. Set to "false" to fall back to legacy passthrough.
VAULT_BEARER_AUTH_ENABLED = os.getenv("VAULT_BEARER_AUTH_ENABLED", "true").lower() in (
    "true",
    "1",
    "yes",
)


@dataclass
class AuthResult:
    """Outcome of an auth check.

    allowed=True  → request may proceed. If user_token is set, downstream tool
                    code should run with that token (per-user Fabric permissions).
                    If user_token is None, run on the SP path.
    allowed=False → request rejected. error contains a short message safe for
                    inclusion in the 401 response body.
    """

    allowed: bool
    user_token: str | None = None
    error: str | None = None


def authenticate(auth_header: str, api_key_header: str, server_api_key: str) -> AuthResult:
    """Apply the connector's auth precedence rules. See module docstring."""

    # 1. X-API-Key takes precedence — must match if present.
    if api_key_header:
        if not server_api_key or api_key_header == server_api_key:
            return AuthResult(allowed=True)
        return AuthResult(allowed=False, error="Invalid X-API-Key")

    # 2 + 3. Bearer token paths.
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:]

        # 2. MI JWT first.
        if MANAGED_IDENTITY_ENABLED:
            from jwt_validator import validate_mi_token

            mi_claims = validate_mi_token(token)
            if mi_claims is not None:
                logger.info(
                    "MI auth: appid=%s",
                    mi_claims.get("appid", mi_claims.get("azp", "?")),
                )
                return AuthResult(allowed=True)

        # 3. Vault / delegated user token. Strict validation when enabled.
        if VAULT_BEARER_AUTH_ENABLED:
            from jwt_validator import validate_vault_bearer_token

            vault_claims = validate_vault_bearer_token(token)
            if vault_claims is None:
                return AuthResult(
                    allowed=False,
                    error="Bearer token failed validation (issuer/audience/appid/signature)",
                )
            logger.info(
                "Vault Bearer auth: appid=%s, oid=%s",
                vault_claims.get("appid", vault_claims.get("azp", "?")),
                vault_claims.get("oid", "?"),
            )

        # Vault validated OR validation disabled (legacy passthrough).
        return AuthResult(allowed=True, user_token=token)

    # 4. No auth at all — dev mode if no API key configured.
    if not server_api_key:
        return AuthResult(allowed=True)

    # 5. Otherwise reject.
    return AuthResult(allowed=False, error="provide Bearer token or X-API-Key")


async def _send_401(send, message: str) -> None:
    body = f'{{"error":"Unauthorized: {message}"}}'.encode()
    await send(
        {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                [b"content-type", b"application/json"],
                [b"content-length", str(len(body)).encode()],
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class McpAuthMiddleware:
    """ASGI middleware for /mcp endpoint — applies the shared auth precedence."""

    def __init__(self, app, api_key: str = ""):
        self.app = app
        self.api_key = api_key

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        auth_value = headers.get(b"authorization", b"").decode()
        api_key_value = headers.get(b"x-api-key", b"").decode()

        result = authenticate(auth_value, api_key_value, self.api_key)
        if not result.allowed:
            await _send_401(send, result.error or "unauthorized")
            return

        if result.user_token is None:
            await self.app(scope, receive, send)
            return

        tok = user_token_var.set(result.user_token)
        try:
            await self.app(scope, receive, send)
        finally:
            user_token_var.reset(tok)
