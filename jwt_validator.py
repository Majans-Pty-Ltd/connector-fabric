"""Managed Identity JWT validation for connector-fabric.

Validates JWTs from Azure Container Apps managed identities.
Expected audience: api://76f295bb-dc42-4419-a6a4-b74812d30ef4
Required role: MCP.Invoke
"""

import logging
import os

import jwt
from cachetools import TTLCache

logger = logging.getLogger("connector-fabric.jwt")

# Azure AD tenant and expected claims
TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
EXPECTED_AUDIENCE = "api://76f295bb-dc42-4419-a6a4-b74812d30ef4"
ISSUER_V2 = f"https://login.microsoftonline.com/{TENANT_ID}/v2.0" if TENANT_ID else ""
REQUIRED_ROLE = "MCP.Invoke"

# JWKS cache — single entry, 24h TTL
_jwks_cache: TTLCache = TTLCache(maxsize=1, ttl=86400)
_JWKS_CACHE_KEY = "jwks"


def _get_jwks_client() -> jwt.PyJWKClient | None:
    """Return a cached PyJWKClient for Azure AD JWKS endpoint.

    Caches the client for 24 hours. Returns None if tenant ID is not configured.
    """
    if not TENANT_ID:
        logger.debug("AZURE_TENANT_ID not set — MI JWT validation disabled")
        return None

    cached = _jwks_cache.get(_JWKS_CACHE_KEY)
    if cached is not None:
        return cached

    jwks_url = f"https://login.microsoftonline.com/{TENANT_ID}/discovery/v2.0/keys"
    try:
        client = jwt.PyJWKClient(jwks_url, cache_keys=True, lifespan=86400)
        _jwks_cache[_JWKS_CACHE_KEY] = client
        logger.info("Cached JWKS client for tenant %s", TENANT_ID)
        return client
    except Exception as e:
        logger.warning("Failed to create JWKS client: %s", e)
        return None


def validate_mi_token(token: str) -> dict | None:
    """Validate a Managed Identity JWT token.

    Returns the decoded claims dict if the token is a valid MI token with:
      - aud == api://76f295bb-dc42-4419-a6a4-b74812d30ef4
      - iss == Azure AD v2.0 issuer for configured tenant
      - roles claim contains MCP.Invoke

    Returns None if validation fails for any reason (wrong audience, expired,
    bad signature, missing role, etc.). Never raises — caller should fall back
    to the next auth method.
    """
    if not TENANT_ID or not ISSUER_V2:
        return None

    jwks_client = _get_jwks_client()
    if jwks_client is None:
        return None

    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
    except Exception as e:
        logger.debug("JWKS key lookup failed (not an MI token?): %s", e)
        return None

    try:
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=EXPECTED_AUDIENCE,
            issuer=ISSUER_V2,
            options={
                "verify_exp": True,
                "verify_iat": True,
                "verify_aud": True,
                "verify_iss": True,
            },
        )
    except jwt.ExpiredSignatureError:
        logger.debug("MI JWT expired")
        return None
    except jwt.InvalidAudienceError:
        # Not targeted at us — likely a delegated user token for Fabric
        logger.debug("JWT audience mismatch (not MI token)")
        return None
    except jwt.InvalidIssuerError:
        logger.debug("JWT issuer mismatch")
        return None
    except Exception as e:
        logger.debug("JWT decode failed: %s", e)
        return None

    # Check required role
    roles = claims.get("roles", [])
    if REQUIRED_ROLE not in roles:
        logger.warning(
            "MI JWT valid but missing required role %s (has: %s)",
            REQUIRED_ROLE,
            roles,
        )
        return None

    logger.info(
        "MI JWT validated — appid=%s, roles=%s",
        claims.get("appid", claims.get("azp", "unknown")),
        roles,
    )
    return claims
