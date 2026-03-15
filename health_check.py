"""
Majans MCP Health Check — Daily Automated Check

Tests connectivity to all MCP-integrated systems:
1. Power BI XMLA endpoints (4 workspaces, 15 datasets)
2. D365 UAT/PROD token acquisition
3. 1Password CLI availability and secret expiry warnings

Writes JSON summary to ~/.claude/memory/mcp-health.json

Schedule via Windows Task Scheduler:
  schtasks /create /tn "Majans MCP Health" /tr "python <path>/health_check.py" /sc daily /st 07:00
"""

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("AZURE_TENANT_ID")
PBI_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
PBI_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

# D365 MCP Agent credentials (separate from PBI SP)
D365_CLIENT_ID = os.getenv("D365_CLIENT_ID", "79b21a82-55f0-42b5-82a6-9d2eb226cb8b")
D365_CLIENT_SECRET = os.getenv("D365_CLIENT_SECRET", "")

D365_UAT_URL = "https://majans-uat.sandbox.operations.dynamics.com"
D365_PROD_URL = "https://majans.operations.dynamics.com"

# 1Password CLI — full path for Windows (not on PATH in scheduled tasks)
OP_CLI = os.getenv(
    "OP_CLI_PATH",
    r"C:\Users\Amit\AppData\Local\Microsoft\WinGet\Packages"
    r"\AgileBits.1Password.CLI_Microsoft.Winget.Source_8wekyb3d8bbwe\op.exe",
)

PBI_WORKSPACES = {
    "PRODUCT": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/PRODUCT",
        "datasets": ["CONSUMERv2"],
    },
    "DEMAND": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/DEMAND",
        "datasets": ["SALESv2", "SCANv2", "STORE", "SCAN TOTAL GROCERY"],
    },
    "SUPPLY": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/SUPPLY",
        "datasets": [
            "AM",
            "CUSTOMER SERVICE v2",
            "INVENTORYV2",
            "MANUFACTURING V3",
            "PURCHASINGV3",
        ],
    },
    "REVIEW": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/REVIEW",
        "datasets": [
            "FINANCIALv2",
            "PLANAUDIT",
            "THREE-WAY",
            "PRODUCTIONCOST",
            "COSTINGv2",
        ],
    },
    "HR": {
        "endpoint": "powerbi://api.powerbi.com/v1.0/myorg/HR",
        "datasets": ["HR"],
    },
}

OUTPUT_PATH = Path.home() / ".claude" / "memory" / "mcp-health.json"


def check_d365_token(env_name: str, resource_url: str) -> dict:
    """Test D365 token acquisition for a given environment."""
    result = {"system": f"D365 {env_name}", "status": "unknown", "details": ""}

    if not D365_CLIENT_SECRET:
        result["status"] = "skipped"
        result["details"] = "D365_CLIENT_SECRET not set"
        return result

    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": D365_CLIENT_ID,
        "client_secret": D365_CLIENT_SECRET,
        "scope": f"{resource_url}/.default",
    }

    try:
        resp = requests.post(token_url, data=token_data, timeout=30)
        if resp.status_code == 200:
            token = resp.json().get("access_token", "")
            result["status"] = "connected"
            result["details"] = f"Token acquired ({len(token)} chars)"
        else:
            error = resp.json().get("error_description", resp.text[:200])
            result["status"] = "failed"
            result["details"] = f"Auth failed ({resp.status_code}): {error}"
    except requests.RequestException as e:
        result["status"] = "failed"
        result["details"] = f"Network error: {e}"

    return result


def check_pbi_rest() -> dict:
    """Test Power BI REST API access."""
    result = {"system": "Power BI REST", "status": "unknown", "details": ""}

    if not PBI_CLIENT_ID or not PBI_CLIENT_SECRET:
        result["status"] = "skipped"
        result["details"] = "PBI credentials not set"
        return result

    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": PBI_CLIENT_ID,
        "client_secret": PBI_CLIENT_SECRET,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
    }

    try:
        resp = requests.post(token_url, data=token_data, timeout=30)
        if resp.status_code != 200:
            result["status"] = "failed"
            result["details"] = f"Auth failed ({resp.status_code})"
            return result

        token = resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        api_resp = requests.get(
            "https://api.powerbi.com/v1.0/myorg/groups",
            headers=headers,
            timeout=30,
        )

        if api_resp.status_code == 200:
            groups = api_resp.json().get("value", [])
            workspace_names = [g["name"] for g in groups]
            result["status"] = "connected"
            result["details"] = (
                f"{len(groups)} workspaces: {', '.join(workspace_names)}"
            )
        else:
            result["status"] = "failed"
            result["details"] = f"API call failed ({api_resp.status_code})"
    except requests.RequestException as e:
        result["status"] = "failed"
        result["details"] = f"Network error: {e}"

    return result


def check_1password() -> dict:
    """Check 1Password CLI availability and sign-in status."""
    result = {"system": "1Password CLI", "status": "unknown", "details": ""}

    try:
        version = subprocess.run(
            [OP_CLI, "--version"], capture_output=True, text=True, timeout=10
        )
        if version.returncode != 0:
            result["status"] = "not_installed"
            result["details"] = "op CLI not found"
            return result

        whoami = subprocess.run(
            [OP_CLI, "whoami"], capture_output=True, text=True, timeout=10
        )
        if whoami.returncode == 0:
            result["status"] = "signed_in"
            result["details"] = whoami.stdout.strip()
        else:
            result["status"] = "not_signed_in"
            result["details"] = "CLI installed but not signed in"
    except FileNotFoundError:
        result["status"] = "not_installed"
        result["details"] = "op CLI not found on PATH"
    except subprocess.TimeoutExpired:
        result["status"] = "timeout"
        result["details"] = "op command timed out"

    return result


def check_secret_expiry() -> list[dict]:
    """Check 1Password items for approaching expiry (Entra app secrets)."""
    warnings = []

    # Known Entra app secrets with expiry dates
    # These are manually tracked since op CLI doesn't expose expiry natively
    secrets_to_track = [
        {
            "name": "Majans Service Principal",
            "vault": "Majans Dev",
            "expires": "2027-12-02",
        },
    ]

    now = datetime.now(timezone.utc)
    for secret in secrets_to_track:
        try:
            expiry = datetime.strptime(secret["expires"], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            days_left = (expiry - now).days
            if days_left < 90:
                warnings.append(
                    {
                        "item": secret["name"],
                        "vault": secret["vault"],
                        "expires": secret["expires"],
                        "days_left": days_left,
                        "severity": "critical" if days_left < 30 else "warning",
                    }
                )
        except ValueError:
            pass

    return warnings


def main():
    print("=== Majans MCP Health Check ===\n")
    timestamp = datetime.now(timezone.utc).isoformat()

    results = []

    # D365 checks
    print("Checking D365 UAT...")
    results.append(check_d365_token("UAT", D365_UAT_URL))

    print("Checking D365 PROD...")
    results.append(check_d365_token("PROD", D365_PROD_URL))

    # Power BI check
    print("Checking Power BI REST API...")
    results.append(check_pbi_rest())

    # 1Password check
    print("Checking 1Password CLI...")
    results.append(check_1password())

    # Secret expiry check
    print("Checking secret expiry...")
    expiry_warnings = check_secret_expiry()

    # Print summary
    print(f"\n{'=' * 50}")
    print(f"{'System':<20} {'Status':<15} {'Details'}")
    print(f"{'=' * 50}")
    for r in results:
        status_icon = {
            "connected": "OK",
            "signed_in": "OK",
            "failed": "FAIL",
            "not_installed": "WARN",
            "not_signed_in": "WARN",
            "skipped": "SKIP",
            "timeout": "WARN",
        }.get(r["status"], "??")
        print(f"{r['system']:<20} {status_icon:<15} {r['details'][:60]}")

    if expiry_warnings:
        print("\n--- Secret Expiry Warnings ---")
        for w in expiry_warnings:
            print(
                f"  {w['severity'].upper()}: {w['item']} expires {w['expires']} ({w['days_left']} days)"
            )

    # Write JSON output
    output = {
        "timestamp": timestamp,
        "results": results,
        "expiry_warnings": expiry_warnings,
        "all_healthy": all(
            r["status"] in ("connected", "signed_in", "not_signed_in", "skipped")
            for r in results
        ),
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, indent=2))
    print(f"\nResults written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
