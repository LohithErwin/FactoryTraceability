"""Inject one deterministic, known anomaly for a live demo.
python -m producers.demo_inject {dup|skip|gap|late|blocked|redteam|all}"""
import sys, time, uuid
from datetime import timedelta
import config
from common import kafka_utils as ku

p = ku.make_producer()
S = config.STATIONS


def st(sn, idx, offset_s=0):
    s = S[idx]
    return {"serial_number": sn, "station_id": s["station_id"], "event_type": s["event_type"],
            "timestamp": (ku.utc_now() + timedelta(seconds=offset_s)).isoformat(), "team": s["team"], "status": "OK"}


def sup(did, status, late_min):
    return {"delivery_id": did, "supplier": "Acme Components", "serial_number": "LOT-DEMO", "station_id": "DOCK-01",
            "event_type": "SUPPLIER_DELIVERY", "timestamp": ku.utc_now().isoformat(), "team": "Logistics",
            "status": status, "expected_ts": (ku.utc_now() - timedelta(minutes=late_min)).isoformat()}


def dup():
    sn = "DEMO-DUP-" + uuid.uuid4().hex[:4]
    for _ in range(2):
        ku.send(p, config.TOPIC_STATION, st(sn, 0), key=sn)


def skip():
    sn = "DEMO-SKIP-" + uuid.uuid4().hex[:4]
    ku.send(p, config.TOPIC_STATION, st(sn, 0), key=sn)
    ku.send(p, config.TOPIC_STATION, st(sn, 2), key=sn)


def gap():
    sn = "DEMO-GAP-" + uuid.uuid4().hex[:4]
    ku.send(p, config.TOPIC_STATION, st(sn, 0), key=sn)
    ku.send(p, config.TOPIC_STATION, st(sn, 1, config.MAX_GAP_SECONDS + 120), key=sn)


def late():
    d = "DLV-DEMO-LATE-" + uuid.uuid4().hex[:3]
    ku.send(p, config.TOPIC_SUPPLIER, sup(d, "IN_TRANSIT", 90), key=d)


def blocked():
    d = "DLV-DEMO-BLK-" + uuid.uuid4().hex[:3]
    ku.send(p, config.TOPIC_SUPPLIER, sup(d, "BLOCKED", 45), key=d)


def redteam():
    planned = ku.utc_now() + timedelta(hours=8)
    for i in (1, 2):
        ku.send(p, config.TOPIC_TEAM, {
            "serial_number": "TASK-PAC-1", "station_id": "N/A", "event_type": "TEAM_STATUS",
            "timestamp": ku.utc_now().isoformat(), "team": "Packaging", "status": "BLOCKED",
            "blocker_id": f"BLK-DEMO-{uuid.uuid4().hex[:3]}", "eta_planned": planned.isoformat(),
            "eta_current": (planned + timedelta(minutes=180)).isoformat()}, key="Packaging")


SC = {"dup": dup, "skip": skip, "gap": gap, "late": late, "blocked": blocked, "redteam": redteam}
if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    if arg not in SC and arg != "all":
        sys.exit(__doc__)
    for name, fn in SC.items():
        if arg in (name, "all"):
            fn()
            print("injected", name)
            time.sleep(0.3)
    p.flush()
