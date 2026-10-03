"""Simulates units moving ST-01 -> ST-02 -> ST-03 with injected anomalies.
python -m producers.station_producer [--rate 2] [--anomaly-rate 0.12] [--max-events N]"""
import argparse, random, time
import config
from common import kafka_utils as ku


def make_event(sn, idx):
    s = config.STATIONS[idx]
    status = ("FAIL" if random.random() < 0.08 else "PASS") if s["event_type"] == "QC" else "OK"
    return {"serial_number": sn, "station_id": s["station_id"], "event_type": s["event_type"],
            "timestamp": ku.utc_now().isoformat(), "team": s["team"], "status": status}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=2.0, help="events per second")
    ap.add_argument("--anomaly-rate", type=float, default=0.12)
    ap.add_argument("--max-events", type=int, default=0)
    a = ap.parse_args()
    p = ku.make_producer()
    units, n, sent, run = [], 0, 0, int(time.time()) % 100000
    ar = a.anomaly_rate
    try:
        while not (a.max_events and sent >= a.max_events):
            t = time.time()
            ready = [u for u in units if u["ready_at"] <= t]
            if not ready or random.random() < 0.4:
                n += 1
                u = {"sn": f"SN{run:05d}{n:04d}", "idx": 0, "ready_at": t, "last": None}
                units.append(u)
                ready.append(u)
            u = random.choice(ready)
            r, idx = random.random(), u["idx"]
            if r < ar / 3 and u["last"]:                      # duplicate serial at same station
                ev = dict(u["last"], timestamp=ku.utc_now().isoformat())
                ku.send(p, config.TOPIC_STATION, ev, key=u["sn"])
                sent += 1
                u["ready_at"] = t + 1
            else:
                if r < 2 * ar / 3 and idx < len(config.STATIONS) - 1:   # skipped station
                    idx += 1
                ev = make_event(u["sn"], idx)
                ku.send(p, config.TOPIC_STATION, ev, key=u["sn"])
                sent += 1
                u["last"], u["idx"] = ev, idx + 1
                stall = random.random() < ar / 3                # stall -> time-gap violation
                u["ready_at"] = t + (config.MAX_GAP_SECONDS + 10 if stall else random.uniform(1, 4))
                if u["idx"] >= len(config.STATIONS):
                    units.remove(u)
            time.sleep(1.0 / a.rate)
    except KeyboardInterrupt:
        pass
    finally:
        p.flush()


if __name__ == "__main__":
    main()
