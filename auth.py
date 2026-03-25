"""Token context and auth middleware for connector-fabric.

Three auth modes on /mcp:
  - Bearer token (MI JWT) → validated via JWKS, uses SP path (no user_token_var)
  - Bearer token (user)   → stored in context var, Fabric API calls as that user
  - X-API-Key             → validated against server API key, falls back to SP token
"""

import contextvars
import logging
import os

# Per-request user token — set by ASGI middleware, read by _request_headers()
user_token_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "user_token", default=None
)

logger = logging.getLogger("connector-fabric.auth")

# Feature flag — opt-in to Managed Identity JWT validation
MANAGED_IDENTITY_ENABLED = (
    os.getenv("MANAGED_IDENTITY_ENABLED", "false").lower() in ("true", "1", "yes")
)


class McpAuthMiddleware:
    """ASGI middleware for /mcp endpoint — handles Bearer tokens and API keys.

    Bearer token (MI JWT): if MANAGED_IDENTITY_ENABLED, tries MI JWT validation
    first. Valid MI tokens with MCP.Invoke role proceed on the SP path (no
    user_token_var set — Fabric client uses SP credentials).

    Bearer token (users): extracted and stored in user_token_var for per-user
    Fabric API calls. Fabric enforces that user's workspace permissions.

    X-API-Key (agents): validated against server's API key. No user token set,
    so tool functions fall back to SP token with full access.

    No auth: rejected with 401 (unless no API key is configured — dev mode).
    """

    def __init__(self, app, api_key: str = ""):
        self.app = app
        self.api_key = api_key

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))
            auth_value = headers.get(b"authorization", b"").decode()
            api_key_value = headers.get(b"x-api-key", b"").decode()

            # Bearer token → try MI JWT first, then delegated user
            if auth_value.lower().startswith("bearer "):
                token = auth_value[7:]

                # MI JWT path: validate against Azure AD JWKS
                if MANAGED_IDENTITY_ENABLED:
                    from jwt_validator import validate_mi_token

                    mi_claims = validate_mi_token(token)
                    if mi_claims is not None:
                        # Valid MI token — proceed on SP path (no user_token_var)
                        logger.info(
                            "MI auth: appid=%s",
                            mi_claims.get("appid", mi_claims.get("azp", "?")),
                        )
                        await self.app(scope, receive, send)
                        return

                # Delegated user token — store for per-user Fabric calls
                tok = user_token_var.set(token)
                try:
                    await self.app(scope, receive, send)
                finally:
                    user_token_var.reset(tok)
                return

            # X-API-Key → agent auth (SP fallback)
            if not self.api_key or api_key_value == self.api_key:
                await self.app(scope, receive, send)
                return

            # Unauthorized
            body = b'{"error":"Unauthorized: provide Bearer token or X-API-Key"}'
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
            return

        await self.app(scope, receive, send)
