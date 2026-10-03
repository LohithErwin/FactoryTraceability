"""Simulates supplier deliveries: on-time, late (in transit, overdue) and blocked.
python -m producers.supplier_producer [--interval 6]"""
import argparse, random, time
from datetime import timedelta
import config
from common import kafka_utils as ku

SUPPLIER = "Acme Components"


def event(did, status, exp):
    return {"delivery_id": did, "supplier": SUPPLIER, "serial_number": f"LOT-{did[4:]}", "station_id": "DOCK-01",
            "event_type": "SUPPLIER_DELIVERY", "timestamp": ku.utc_now().isoformat(), "team": "Logistics",
            "status": status, "expected_ts": exp.isoformat()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=6.0, help="seconds between new deliveries")
    a = ap.parse_args()
    p = ku.make_producer()
    pending, n, next_new = [], 0, 0.0
    try:
        while True:
            t = time.time()
            if t >= next_new:
                n += 1
                did = f"DLV-{int(t) % 100000:05d}{n:03d}"
                r = random.random()
                if r < 0.6:      # on time
                    exp, first, wait = ku.utc_now() - timedelta(minutes=random.uniform(-10, 8)), "IN_TRANSIT", (4, 10)
                elif r < 0.85:   # late and still in transit
                    exp, first, wait = ku.utc_now() - timedelta(minutes=random.uniform(20, 120)), "IN_TRANSIT", (15, 40)
                else:            # blocked (e.g. customs)
                    exp, first, wait = ku.utc_now() - timedelta(minutes=random.uniform(5, 60)), "BLOCKED", (20, 60)
                ku.send(p, config.TOPIC_SUPPLIER, event(did, first, exp), key=did)
                if not (first == "BLOCKED" and random.random() < 0.3):   # 30% of blocked never clear
                    pending.append((t + random.uniform(*wait), did, exp))
                next_new = t + a.interval
            for item in [x for x in pending if x[0] <= t]:
                pending.remove(item)
                ku.send(p, config.TOPIC_SUPPLIER, event(item[1], "DELIVERED", item[2]), key=item[1])
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        p.flush()


if __name__ == "__main__":
    main()
