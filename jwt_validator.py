"""JWT validation for connector-fabric Bearer tokens.

Two validation paths, both signature-checked against Microsoft's JWKS:

  1. Managed Identity tokens — issued to Container App MI for agent->connector
     calls. Audience is the SP app's API URI (api://76f295bb-...).
     Required role: MCP.Invoke.

  2. Vault / delegated user tokens — issued to Fabric-MCP-User Entra app
     (or other configured vault apps) for the Power BI / Fabric XMLA API.
     This path accepts tokens for both:
       - Local Claude Code users (acquired via get-user-token.py)
       - Anthropic Managed Agents Vaults (token injected as Bearer header)
     Audience must be the Power BI XMLA URL.
"""

import logging
import os

import jwt
from cachetools import TTLCache

logger = logging.getLogger("connector-fabric.jwt")

# --- Tenant / issuers ---
TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
ISSUER_V2 = f"https://login.microsoftonline.com/{TENANT_ID}/v2.0" if TENANT_ID else ""
ISSUER_V1 = f"https://sts.windows.net/{TENANT_ID}/" if TENANT_ID else ""

# --- MI token validation ---
EXPECTED_AUDIENCE = "api://76f295bb-dc42-4419-a6a4-b74812d30ef4"
REQUIRED_ROLE = "MCP.Invoke"

# --- Vault / delegated token validation ---
# Power BI / Fabric XMLA audience as it appears in tokens issued by Azure AD.
# The .default scope (https://analysis.windows.net/powerbi/api/.default) yields
# tokens with aud == "https://analysis.windows.net/powerbi/api". Accept both
# trailing-slash variants.
VAULT_EXPECTED_AUDIENCES = {
    "https://analysis.windows.net/powerbi/api",
    "https://analysis.windows.net/powerbi/api/",
}

# Allowed Entra app IDs that may issue Bearer tokens accepted by this connector.
# Comma-separated list, defaults to Fabric-MCP-User (the public-client delegated app).
# Override via env var to add additional vault-credential apps.
_DEFAULT_ALLOWED_APP_IDS = "cf4685ef-d594-4ede-961d-5c3554be3974"
VAULT_ALLOWED_APP_IDS = {
    a.strip()
    for a in os.getenv("VAULT_ALLOWED_APP_IDS", _DEFAULT_ALLOWED_APP_IDS).split(",")
    if a.strip()
}

# JWKS cache — single entry, 24h TTL
_jwks_cache: TTLCache = TTLCache(maxsize=1, ttl=86400)
_JWKS_CACHE_KEY = "jwks"


def _get_jwks_client() -> jwt.PyJWKClient | None:
    """Return a cached PyJWKClient for Azure AD JWKS endpoint.

    Caches the client for 24 hours. Returns None if tenant ID is not configured.
    Both MI and vault validation paths share this JWKS client.
    """
    if not TENANT_ID:
        logger.debug("AZURE_TENANT_ID not set — JWT validation disabled")
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

    Returns None if validation fails for any reason. Never raises — caller
    should fall back to the next auth method.
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


def validate_vault_bearer_token(token: str) -> dict | None:
    """Validate a delegated user / vault Bearer token for Fabric XMLA / PBI API.

    Accepts tokens issued by configured Entra apps (default: Fabric-MCP-User)
    targeting the Power BI / Fabric XMLA API. This is the auth path for:
      - Local Claude Code users (token from get-user-token.py)
      - Anthropic Managed Agents Vaults (token injected as Bearer header)

    Validation:
      - JWT signature against Microsoft's JWKS (Majans tenant keys)
      - iss matches Majans tenant (v1 sts.windows.net or v2 login.microsoftonline.com)
      - aud matches Power BI XMLA URL (https://analysis.windows.net/powerbi/api,
        with or without trailing slash)
      - appid (or azp) is in VAULT_ALLOWED_APP_IDS

    Returns claims dict on success (with oid, appid, upn). Returns None on
    failure — caller should reject the request with 401.
    """
    if not TENANT_ID:
        return None

    jwks_client = _get_jwks_client()
    if jwks_client is None:
        return None

    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
    except Exception as e:
        logger.debug("Vault JWT JWKS key lookup failed: %s", e)
        return None

    # Decode with audience+issuer verification — try v1 then v2 issuer to
    # support both token shapes that Azure AD emits for delegated apps.
    claims = None
    last_err: Exception | None = None
    for issuer in (ISSUER_V1, ISSUER_V2):
        if not issuer:
            continue
        try:
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                audience=list(VAULT_EXPECTED_AUDIENCES),
                issuer=issuer,
                options={
                    "verify_exp": True,
                    "verify_iat": True,
                    "verify_aud": True,
                    "verify_iss": True,
                },
            )
            break
        except jwt.InvalidIssuerError as e:
            last_err = e
            continue
        except jwt.ExpiredSignatureError:
            logger.debug("Vault Bearer token expired")
            return None
        except jwt.InvalidAudienceError:
            logger.debug(
                "Vault Bearer token audience mismatch — expected one of %s",
                VAULT_EXPECTED_AUDIENCES,
            )
            return None
        except Exception as e:
            logger.debug("Vault JWT decode failed: %s", e)
            return None

    if claims is None:
        logger.debug("Vault Bearer token issuer mismatch: %s", last_err)
        return None

    # Verify the token was issued to one of our allowed delegated apps.
    appid = claims.get("appid") or claims.get("azp") or ""
    if appid not in VAULT_ALLOWED_APP_IDS:
        logger.warning(
            "Vault Bearer token rejected — appid %s not in allowed list %s",
            appid or "<missing>",
            sorted(VAULT_ALLOWED_APP_IDS),
        )
        return None

    logger.info(
        "Vault Bearer auth — appid=%s, oid=%s, upn=%s",
        appid,
        claims.get("oid", "?"),
        claims.get("upn", claims.get("preferred_username", "?")),
    )
    return claims
