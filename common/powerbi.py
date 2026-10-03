"""Power BI push helper. Throttled, never raises, never blocks consumers for long.

Per stream (metrics | sla | teams) configure ONE of:
  PBI_URL_<STREAM>      full push URL from a UI-created 'API' streaming dataset (no auth needed)
  PBI_DATASET_<STREAM>  dataset id created via scripts/create_pbi_datasets.py (needs Azure AD token)
"""
import logging, os, time
import requests
import config

log = logging.getLogger("powerbi")
_tok = {"v": None, "exp": 0}
API = "https://api.powerbi.com/v1.0/myorg"
TABLE = "RealTimeData"


def get_token():
    if os.getenv("PBI_ACCESS_TOKEN"):
        return os.getenv("PBI_ACCESS_TOKEN")
    if _tok["v"] and time.time() < _tok["exp"] - 60:
        return _tok["v"]
    tenant, cid, secret = (os.getenv(k) for k in ("PBI_TENANT_ID", "PBI_CLIENT_ID", "PBI_CLIENT_SECRET"))
    if not (tenant and cid and secret):
        return None
    r = requests.post(f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token", data={
        "grant_type": "client_credentials", "client_id": cid, "client_secret": secret,
        "scope": "https://analysis.windows.net/powerbi/.default"}, timeout=10)
    r.raise_for_status()
    j = r.json()
    _tok.update(v=j["access_token"], exp=time.time() + int(j["expires_in"]))
    return _tok["v"]


class Pusher:
    def __init__(self, stream, interval=None):
        key = stream.upper()
        self.stream = stream
        self.url = os.getenv(f"PBI_URL_{key}") or None
        self.dataset = os.getenv(f"PBI_DATASET_{key}") or None
        self.group = os.getenv("PBI_WORKSPACE_ID") or None
        self.interval = config.PBI_PUSH_INTERVAL if interval is None else interval
        self.enabled = bool(self.url or self.dataset)
        self.last = 0.0
        if not self.enabled:
            log.info("Power BI push disabled for '%s' (set PBI_URL_%s or PBI_DATASET_%s)", stream, key, key)

    def due(self):
        return self.enabled and time.time() - self.last >= self.interval

    def push(self, rows):
        if not self.enabled:
            return False
        self.last = time.time()
        try:
            if self.url:
                r = requests.post(self.url, json=rows, timeout=5)
            else:
                base = API + (f"/groups/{self.group}" if self.group else "")
                r = requests.post(f"{base}/datasets/{self.dataset}/tables/{TABLE}/rows",
                                  json={"rows": rows}, timeout=5,
                                  headers={"Authorization": f"Bearer {get_token()}"})
            r.raise_for_status()
            return True
        except Exception as e:
            log.warning("push to Power BI '%s' failed: %s", self.stream, e)
            return False
