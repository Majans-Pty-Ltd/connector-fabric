"""Token context for per-request auth in connector-fabric.

When a user connects via /mcp with a Bearer token, the middleware stores it
in a context variable. Tool functions read it via _request_headers() to make
PBI API calls as that user, enforcing Fabric workspace permissions natively.

Managed Identity (MI) tokens are validated but NOT stored in user_token_var
because they are NOT Fabric access tokens — the Fabric client falls back to
SP credentials for MI-authenticated requests.
"""

import contextvars
import logging
import os

logger = logging.getLogger("connector-fabric.auth")

# Per-request user token — set by ASGI middleware, read by _request_headers()
user_token_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "user_token", default=None
)

# Feature flag — off by default for backwards compatibility
MANAGED_IDENTITY_ENABLED = os.getenv("MANAGED_IDENTITY_ENABLED", "false").lower() in (
    "true",
    "1",
    "yes",
)


class TokenExtractorASGI:
    """ASGI middleware that extracts Bearer token from Authorization header
    and stores it in the user_token_var context variable.

    When MANAGED_IDENTITY_ENABLED is true, Bearer tokens are first checked
    as MI JWTs. Valid MI tokens proceed without setting user_token_var (so
    tool functions fall back to SP credentials). Invalid MI tokens are treated
    as delegated user tokens (existing path).

    Wraps the MCP StreamableHTTP app so that tool functions can access
    the calling user's token for per-user Fabric API calls.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))
            auth_value = headers.get(b"authorization", b"").decode()
            if auth_value.lower().startswith("bearer "):
                token = auth_value[7:]

                # MI JWT check — if enabled, try to validate as MI token first
                if MANAGED_IDENTITY_ENABLED:
                    from jwt_validator import validate_mi_token

                    mi_claims = validate_mi_token(token)
                    if mi_claims is not None:
                        # Valid MI token — proceed WITHOUT setting user_token_var.
                        # Tool functions will fall back to SP credentials.
                        logger.info(
                            "MI auth on /mcp — appid=%s",
                            mi_claims.get("appid", mi_claims.get("azp", "unknown")),
                        )
                        await self.app(scope, receive, send)
                        return

                # Delegated user token — existing path
                tok = user_token_var.set(token)
                try:
                    await self.app(scope, receive, send)
                finally:
                    user_token_var.reset(tok)
                return
        await self.app(scope, receive, send)
