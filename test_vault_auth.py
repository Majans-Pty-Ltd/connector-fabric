"""Smoke test for vault Bearer token validation (connector-fabric).

Crafts synthetic JWTs signed with a local RSA key and verifies the validator
accepts/rejects them as expected. Mocks PyJWKClient so we don't hit Microsoft.

Run:
  AZURE_TENANT_ID=d54794b1-f598-4c0f-a276-6039a39774ac python test_vault_auth.py
"""

import sys
import time
from unittest.mock import patch

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

import auth
import jwt_validator

TENANT = "d54794b1-f598-4c0f-a276-6039a39774ac"
ALLOWED_APP = "cf4685ef-d594-4ede-961d-5c3554be3974"
OTHER_APP = "11111111-2222-3333-4444-555555555555"
FABRIC_AUD = "https://analysis.windows.net/powerbi/api"

_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PUBLIC_KEY = _PRIVATE_KEY.public_key()


class _FakeSigningKey:
    def __init__(self, key):
        self.key = key


class _FakeJWKSClient:
    def get_signing_key_from_jwt(self, token):
        return _FakeSigningKey(_PUBLIC_KEY)


def _make_token(*, aud, iss, appid, exp_offset=3600):
    now = int(time.time())
    return jwt.encode(
        {
            "iss": iss,
            "aud": aud,
            "appid": appid,
            "azp": appid,
            "oid": "user-oid-12345",
            "upn": "user@majans.com",
            "iat": now,
            "nbf": now,
            "exp": now + exp_offset,
        },
        _PRIVATE_KEY,
        algorithm="RS256",
    )


def _run_case(label, token, *, expect_pass, expect_user_token=True):
    with patch.object(jwt_validator, "_get_jwks_client", return_value=_FakeJWKSClient()):
        result = auth.authenticate(f"Bearer {token}", "", "server-api-key")

    ok = result.allowed == expect_pass
    if expect_pass and expect_user_token:
        ok = ok and result.user_token == token
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {label}: allowed={result.allowed}, error={result.error}")
    return ok


def main():
    issuer_v1 = f"https://sts.windows.net/{TENANT}/"
    issuer_v2 = f"https://login.microsoftonline.com/{TENANT}/v2.0"

    print("Test cases:")
    cases_ok = []

    cases_ok.append(_run_case(
        "valid v1 token (allowed app, Fabric aud)",
        _make_token(aud=FABRIC_AUD, iss=issuer_v1, appid=ALLOWED_APP),
        expect_pass=True,
    ))

    cases_ok.append(_run_case(
        "valid v2 token (allowed app, Fabric aud)",
        _make_token(aud=FABRIC_AUD, iss=issuer_v2, appid=ALLOWED_APP),
        expect_pass=True,
    ))

    cases_ok.append(_run_case(
        "rejected: appid not in allow-list",
        _make_token(aud=FABRIC_AUD, iss=issuer_v1, appid=OTHER_APP),
        expect_pass=False,
    ))

    cases_ok.append(_run_case(
        "rejected: wrong audience (D365 instead of Fabric)",
        _make_token(aud="https://majans.operations.dynamics.com", iss=issuer_v1, appid=ALLOWED_APP),
        expect_pass=False,
    ))

    cases_ok.append(_run_case(
        "rejected: wrong tenant",
        _make_token(
            aud=FABRIC_AUD,
            iss="https://sts.windows.net/00000000-0000-0000-0000-000000000000/",
            appid=ALLOWED_APP,
        ),
        expect_pass=False,
    ))

    cases_ok.append(_run_case(
        "rejected: expired token",
        _make_token(aud=FABRIC_AUD, iss=issuer_v1, appid=ALLOWED_APP, exp_offset=-60),
        expect_pass=False,
    ))

    valid_bearer = _make_token(aud=FABRIC_AUD, iss=issuer_v1, appid=ALLOWED_APP)
    bad_bearer = _make_token(aud=FABRIC_AUD, iss=issuer_v1, appid=OTHER_APP)
    with patch.object(jwt_validator, "_get_jwks_client", return_value=_FakeJWKSClient()):
        r1 = auth.authenticate(f"Bearer {valid_bearer}", "server-api-key", "server-api-key")
        case_g1 = r1.allowed and r1.user_token is None
        print(f"  [{'PASS' if case_g1 else 'FAIL'}] X-API-Key precedence: good key + good Bearer -> SP path: {r1}")
        cases_ok.append(case_g1)

        r2 = auth.authenticate(f"Bearer {bad_bearer}", "server-api-key", "server-api-key")
        case_g2 = r2.allowed and r2.user_token is None
        print(f"  [{'PASS' if case_g2 else 'FAIL'}] X-API-Key precedence: good key + bad Bearer -> SP path: {r2}")
        cases_ok.append(case_g2)

        r3 = auth.authenticate(f"Bearer {valid_bearer}", "wrong-key", "server-api-key")
        case_g3 = (not r3.allowed)
        print(f"  [{'PASS' if case_g3 else 'FAIL'}] X-API-Key precedence: bad key + good Bearer -> REJECT: {r3}")
        cases_ok.append(case_g3)

    r4 = auth.authenticate("", "server-api-key", "server-api-key")
    case_h = r4.allowed and r4.user_token is None
    print(f"  [{'PASS' if case_h else 'FAIL'}] X-API-Key only (regression): {r4}")
    cases_ok.append(case_h)

    passed = sum(1 for c in cases_ok if c)
    total = len(cases_ok)
    print(f"\nResult: {passed}/{total} passed")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
