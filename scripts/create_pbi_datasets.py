"""Create the 3 push/streaming datasets via the Power BI REST API.
Needs a token: export PBI_ACCESS_TOKEN=$(az account get-access-token --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv)
(or set PBI_TENANT_ID / PBI_CLIENT_ID / PBI_CLIENT_SECRET). Optional PBI_WORKSPACE_ID.
Prints the PBI_DATASET_* lines to paste into .env.   python -m scripts.create_pbi_datasets"""
import os, sys
import requests
from common import powerbi

I, D, S, T = "Int64", "Double", "String", "DateTime"
SCHEMAS = {
    "METRICS": ("Factory Live - Metrics", {"ts": T, "violation_count": I, "duplicate_count": I, "sequence_count": I,
                                           "gap_count": I, "throughput_per_min": I, "events_total": I}),
    "SLA": ("Factory Live - SLA", {"ts": T, "open_breaches": I, "blocked_open": I, "late_open": I,
                                   "total_breaches": I, "deliveries_total": I}),
    "TEAMS": ("Factory Live - Teams", {"ts": T, "team": S, "open_blockers": I, "avg_eta_slippage_min": D,
                                       "max_eta_slippage_min": D, "health": S, "health_score": I}),
}

token = powerbi.get_token()
if not token:
    sys.exit("No Power BI token. See docstring.")
group = os.getenv("PBI_WORKSPACE_ID")
base = powerbi.API + (f"/groups/{group}" if group else "")
for key, (name, cols) in SCHEMAS.items():
    body = {"name": name, "defaultMode": "PushStreaming",
            "tables": [{"name": powerbi.TABLE, "columns": [{"name": c, "dataType": t} for c, t in cols.items()]}]}
    r = requests.post(f"{base}/datasets?defaultRetentionPolicy=basicFIFO", json=body,
                      headers={"Authorization": f"Bearer {token}"}, timeout=30)
    if r.status_code >= 300:
        sys.exit(f"{name}: {r.status_code} {r.text}")
    print(f"PBI_DATASET_{key}={r.json()['id']}")
