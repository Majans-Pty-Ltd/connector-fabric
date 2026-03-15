"""Token context for per-request auth in connector-fabric.

When a user connects via /mcp with a Bearer token, the middleware stores it
in a context variable. Tool functions read it via _request_headers() to make
PBI API calls as that user, enforcing Fabric workspace permissions natively.
"""

import contextvars

# Per-request user token — set by ASGI middleware, read by _request_headers()
user_token_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "user_token", default=None
)


class TokenExtractorASGI:
    """ASGI middleware that extracts Bearer token from Authorization header
    and stores it in the user_token_var context variable.

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
                tok = user_token_var.set(token)
                try:
                    await self.app(scope, receive, send)
                finally:
                    user_token_var.reset(tok)
                return
        await self.app(scope, receive, send)
