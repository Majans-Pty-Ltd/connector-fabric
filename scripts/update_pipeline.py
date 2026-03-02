"""
Update the PLAN GOLD STAGING TO LIVE COPY pipeline to use
Graph API (Service Principal) for email instead of Office 365 user OAuth.

Approach:
1. Create a Fabric "Web" connection with SP credentials for graph.microsoft.com
2. Update the pipeline to use a single WebActivity with that connection
   (Fabric handles token management via the connection — no manual token step)
"""

import os
import json
import base64
import sys
import requests
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
WS_ID = "a679e435-dc2a-40a3-88b2-c31179064d70"
PIPELINE_ID = "358af190-6f0b-45c5-b60c-afb7783728fc"
SENDER = "mars@majans.com"

RECIPIENTS = [
    "amit@majans.com",
    "davidr@majans.com",
    "simon@majans.com",
    "phoebe@majans.com",
]

# --- Column mappings (unchanged from original) ---
COLUMN_MAPPINGS = [
    {
        "source": {"name": c, "type": t, "physicalType": p},
        "sink": {"name": c, "physicalType": p},
    }
    for c, t, p in [
        ("MODELID", "String", "string"),
        ("ITEM", "String", "string"),
        ("DATE", "Date", "date"),
        ("CUSTOMER", "String", "string"),
        ("INVOICEACCOUNT", "String", "string"),
        ("SELLINDATE", "Date", "date"),
        ("UNITS", "Double", "double"),
        ("QUANTITY", "Double", "double"),
        ("KG", "Double", "double"),
        ("PALLETS", "Double", "double"),
        ("GSV", "Double", "double"),
        ("ACCOUNT", "String", "string"),
        ("Value", "Double", "double"),
        ("DEPARTMENT", "String", "string"),
        ("ACCOUNTKEY", "String", "string"),
        ("SELLINWEEK", "Date", "date"),
        ("SNAPSHOTDATE", "Date", "date"),
    ]
]

# --- Lakehouse linked service (same workspace, same artifact) ---
LAKEHOUSE_SERVICE = {
    "name": "MajansLakehouse",
    "properties": {
        "annotations": [],
        "type": "Lakehouse",
        "typeProperties": {
            "workspaceId": WS_ID,
            "artifactId": "e4e3c5e8-6513-483f-982a-7aad46cb6dd8",
            "rootFolder": "Tables",
        },
    },
}


def get_fabric_token():
    """Get token for Fabric API."""
    r = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        },
    )
    r.raise_for_status()
    return r.json()["access_token"]


def create_graph_connection():
    """Create a WebForPipeline connection with SP credentials for Graph API.

    Uses the pipeline-specific 'WebForPipeline' connection type which:
    - Has a baseUrl + audience parameter
    - Supports ServicePrincipal credentials
    - Fabric manages the SP token lifecycle automatically
    """
    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    display_name = "Graph API - Majans SP"
    body = {
        "connectivityType": "ShareableCloud",
        "displayName": display_name,
        "connectionDetails": {
            "type": "WebForPipeline",
            "creationMethod": "WebForPipeline.Contents",
            "parameters": [
                {
                    "dataType": "Text",
                    "name": "baseUrl",
                    "value": "https://graph.microsoft.com",
                },
                {
                    "dataType": "Text",
                    "name": "audience",
                    "value": "https://graph.microsoft.com",
                },
            ],
        },
        "privacyLevel": "Organizational",
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": False,
            "credentials": {
                "credentialType": "ServicePrincipal",
                "tenantId": TENANT_ID,
                "servicePrincipalClientId": CLIENT_ID,
                "servicePrincipalSecret": CLIENT_SECRET,
            },
        },
    }

    r = requests.post(
        "https://api.fabric.microsoft.com/v1/connections",
        headers=headers,
        json=body,
    )

    if r.status_code == 201:
        conn = r.json()
        print(f"OK: Created '{display_name}' (ID: {conn['id']})")
        return conn["id"]
    elif "DuplicateConnectionName" in r.text:
        r2 = requests.get(
            "https://api.fabric.microsoft.com/v1/connections",
            headers=headers,
        )
        if r2.status_code == 200:
            for c in r2.json().get("value", []):
                if c.get("displayName") == display_name:
                    print(f"OK: Found existing '{display_name}' (ID: {c['id']})")
                    return c["id"]
    else:
        print(f"Failed to create '{display_name}': {r.status_code}")
        print(r.text[:500])
    return None


def build_pipeline_content(connection_id):
    """Build the pipeline definition with a single WebActivity.

    The WebForPipeline connection handles SP authentication automatically,
    so we only need one Web activity (no manual token step).
    """
    email_body = {
        "message": {
            "subject": "Forecast Staging has been copied to Forecast Current Successfully",
            "body": {
                "contentType": "HTML",
                "content": (
                    "<p>The PLAN GOLD STAGING table has been successfully "
                    "copied to PLAN GOLD.</p><p>Thank you!</p>"
                ),
            },
            "toRecipients": [{"emailAddress": {"address": r}} for r in RECIPIENTS],
        }
    }

    return {
        "properties": {
            "activities": [
                # Activity 1: Copy (unchanged from original)
                {
                    "name": "PLAN GOLD STAGING TO LIVE",
                    "type": "Copy",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.12:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False,
                    },
                    "typeProperties": {
                        "source": {
                            "type": "LakehouseTableSource",
                            "datasetSettings": {
                                "annotations": [],
                                "linkedService": LAKEHOUSE_SERVICE,
                                "type": "LakehouseTable",
                                "schema": [],
                                "typeProperties": {"table": "PLAN_GOLD_STAGING"},
                            },
                        },
                        "sink": {
                            "type": "LakehouseTableSink",
                            "tableActionOption": "OverwriteSchema",
                            "partitionOption": "None",
                            "datasetSettings": {
                                "annotations": [],
                                "linkedService": LAKEHOUSE_SERVICE,
                                "type": "LakehouseTable",
                                "schema": [],
                                "typeProperties": {"table": "PLAN_GOLD"},
                            },
                        },
                        "enableStaging": False,
                        "translator": {
                            "type": "TabularTranslator",
                            "mappings": COLUMN_MAPPINGS,
                            "typeConversion": True,
                            "typeConversionSettings": {
                                "allowDataTruncation": True,
                                "treatBooleanAsNumber": False,
                            },
                        },
                    },
                },
                # Activity 2: Send email via Graph API
                # Connection handles SP auth + token automatically
                {
                    "name": "Send Success Email",
                    "type": "WebActivity",
                    "dependsOn": [
                        {
                            "activity": "PLAN GOLD STAGING TO LIVE",
                            "dependencyConditions": ["Succeeded"],
                        }
                    ],
                    "policy": {
                        "timeout": "0.00:10:00",
                        "retry": 1,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False,
                    },
                    "typeProperties": {
                        "relativeUrl": f"/v1.0/users/{SENDER}/sendMail",
                        "method": "POST",
                        "headers": {
                            "Content-Type": "application/json",
                        },
                        "body": json.dumps(email_body),
                    },
                    "externalReferences": {
                        "connection": connection_id,
                    },
                },
            ]
        }
    }


def update_pipeline(connection_id):
    """Update the pipeline definition in Fabric."""
    pipeline_content = build_pipeline_content(connection_id)

    payload_bytes = json.dumps(pipeline_content, indent=2).encode("utf-8")
    payload_b64 = base64.b64encode(payload_bytes).decode("utf-8")

    update_body = {
        "definition": {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": payload_b64,
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }

    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/items/{PIPELINE_ID}/updateDefinition"
    r = requests.post(url, headers=headers, json=update_body)

    print(f"Update status: {r.status_code}")
    if r.status_code in (200, 202):
        print("OK: Pipeline updated successfully!")
    else:
        print(f"Error: {r.text[:500]}")
    return r.status_code in (200, 202)


def trigger_run():
    """Trigger a pipeline run."""
    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WS_ID}/items/{PIPELINE_ID}/jobs/instances?jobType=Pipeline"
    r = requests.post(url, headers=headers)

    if r.status_code == 202:
        location = r.headers.get("Location", "")
        print(f"OK: Pipeline triggered! Monitor: {location}")
        return location
    else:
        print(f"Trigger failed: {r.status_code} - {r.text[:300]}")
        return None


def monitor_run(monitor_url, max_checks=15, interval=30):
    """Monitor a pipeline run until completion."""
    import time

    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}

    for i in range(max_checks):
        r = requests.get(monitor_url, headers=headers)
        if r.status_code == 200:
            data = r.json()
            status = data.get("status", "?")
            end = data.get("endTimeUtc", "running...")
            print(f"[Check {i + 1}] Status: {status} | Ended: {end}")
            if status in ("Completed", "Failed", "Cancelled"):
                if data.get("failureReason"):
                    print(
                        f"  Failure: {data['failureReason'].get('message', 'unknown')}"
                    )
                return status
        else:
            print(f"[Check {i + 1}] HTTP {r.status_code}")
        time.sleep(interval)

    print("Timed out waiting for pipeline")
    return None


if __name__ == "__main__":
    if "--deploy" in sys.argv:
        # Step 1: Create the WebForPipeline connection with SP auth
        print("=== Step 1: Create Graph API Connection (WebForPipeline + SP) ===")
        conn_id = create_graph_connection()
        if not conn_id:
            print("ABORT: Could not create connection")
            sys.exit(1)

        # Step 2: Update the pipeline
        print(f"\n=== Step 2: Update Pipeline (connection: {conn_id}) ===")
        if not update_pipeline(conn_id):
            print("ABORT: Pipeline update failed")
            sys.exit(1)

        # Step 3: Trigger a run
        if "--run" in sys.argv:
            print("\n=== Step 3: Trigger Pipeline Run ===")
            monitor_url = trigger_run()
            if monitor_url:
                print("\n=== Monitoring ===")
                monitor_run(monitor_url)
    else:
        print("Usage:")
        print(
            "  python update_pipeline.py --deploy        # Create connection + update pipeline"
        )
        print(
            "  python update_pipeline.py --deploy --run  # + trigger and monitor a run"
        )
