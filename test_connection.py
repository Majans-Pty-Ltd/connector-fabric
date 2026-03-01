"""
Power BI XMLA Endpoint Connection Test

Tests connectivity in two stages:
1. REST API auth test (verifies Service Principal credentials)
2. XMLA endpoint test via ADOMD.NET (verifies full XMLA connectivity)
"""

import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

# Add ADOMD.NET DLL path before pyadomd is imported
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ADOMD_DLL_PATH = os.path.join(SCRIPT_DIR, "adomd_package", "lib", "net45")
if os.path.isdir(ADOMD_DLL_PATH):
    sys.path.insert(0, ADOMD_DLL_PATH)
    os.environ["PATH"] = ADOMD_DLL_PATH + os.pathsep + os.environ.get("PATH", "")

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
XMLA_ENDPOINT = os.getenv("PBI_XMLA_ENDPOINT")
DATASET_NAME = os.getenv("PBI_DATASET_NAME")


def check_env():
    """Verify all required environment variables are set."""
    missing = []
    for var in ["AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET",
                "PBI_XMLA_ENDPOINT", "PBI_DATASET_NAME"]:
        if not os.getenv(var):
            missing.append(var)
    if missing:
        print(f"FAIL: Missing environment variables: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in your values.")
        return False
    print("OK: All environment variables set")
    return True


def test_auth():
    """Test Service Principal authentication against Azure AD."""
    print("\n--- Stage 1: Authentication Test ---")
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
    }

    try:
        resp = requests.post(token_url, data=token_data, timeout=30)
        if resp.status_code == 200:
            token = resp.json().get("access_token")
            print(f"OK: Got access token ({len(token)} chars)")
            return token
        else:
            error = resp.json().get("error_description", resp.text)
            print(f"FAIL: Auth failed ({resp.status_code}): {error}")
            return None
    except requests.RequestException as e:
        print(f"FAIL: Network error: {e}")
        return None


def test_rest_api(token):
    """Test Power BI REST API access with the token."""
    print("\n--- Stage 2: Power BI REST API Test ---")
    headers = {"Authorization": f"Bearer {token}"}

    # List workspaces the SP has access to
    resp = requests.get(
        "https://api.powerbi.com/v1.0/myorg/groups",
        headers=headers,
        timeout=30,
    )
    if resp.status_code == 200:
        groups = resp.json().get("value", [])
        print(f"OK: Service Principal can access {len(groups)} workspace(s):")
        for g in groups:
            print(f"  - {g['name']} (ID: {g['id']})")
        return True
    else:
        print(f"FAIL: REST API call failed ({resp.status_code}): {resp.text}")
        return False


def test_xmla_connection(token):
    """Test XMLA endpoint connectivity via ADOMD.NET (pyadomd)."""
    print("\n--- Stage 3: XMLA Endpoint Test ---")
    try:
        import clr
        dll_path = os.path.join(ADOMD_DLL_PATH, "Microsoft.AnalysisServices.AdomdClient.dll")
        if os.path.isfile(dll_path):
            clr.AddReference(dll_path)
            print(f"OK: Loaded ADOMD.NET DLL from {dll_path}")
        from pyadomd import Pyadomd
    except ImportError as e:
        print(f"FAIL: Import error: {e}")
        return False
    except Exception as e:
        print(f"FAIL: Could not load ADOMD.NET: {e}")
        return False

    conn_str = (
        f"Provider=MSOLAP;"
        f"Data Source={XMLA_ENDPOINT};"
        f"Initial Catalog={DATASET_NAME};"
        f"User ID=app:{CLIENT_ID}@{TENANT_ID};"
        f"Password={CLIENT_SECRET};"
        f"Persist Security Info=True;"
        f"Impersonation Level=Impersonate;"
    )

    try:
        conn = Pyadomd(conn_str)
        conn.open()
        print("OK: XMLA connection opened successfully!")

        # Run a simple DAX query to confirm data access
        query = "EVALUATE ROW(\"Test\", 1)"
        cursor1 = conn.cursor()
        cursor1.execute(query)
        rows = cursor1.fetchall()
        if rows:
            print(f"OK: DAX query returned {len(rows)} row(s): {rows}")
        cursor1.close()

        # List tables in the model (new cursor to avoid reader conflict)
        query_tables = """
        SELECT [TABLE_NAME]
        FROM $SYSTEM.DBSCHEMA_TABLES
        WHERE [TABLE_TYPE] = 'TABLE'
        """
        cursor2 = conn.cursor()
        cursor2.execute(query_tables)
        tables = cursor2.fetchall()
        print(f"OK: Dataset contains {len(tables)} table(s):")
        for t in tables:
            print(f"  - {t[0]}")
        cursor2.close()

        conn.close()
        print("\nOK: XMLA connection test PASSED")
        return True

    except Exception as e:
        print(f"FAIL: XMLA connection error: {e}")
        if "MSOLAP" in str(e):
            print("Install the MSOLAP provider from:")
            print("https://learn.microsoft.com/en-us/analysis-services/client-libraries")
        return False


def main():
    print("=== Power BI XMLA Connection Test ===\n")

    if not check_env():
        sys.exit(1)

    token = test_auth()
    if not token:
        sys.exit(1)

    test_rest_api(token)

    test_xmla_connection(token)


if __name__ == "__main__":
    main()
