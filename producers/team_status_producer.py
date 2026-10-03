"""Simulates per-team status updates: progress, ETA slippage, blockers opening/resolving.
python -m producers.team_status_producer [--rate 1]"""
import argparse, random, time
from datetime import timedelta
import config
from common import kafka_utils as ku


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=1.0, help="events per second")
    a = ap.parse_args()
    p = ku.make_producer()
    planned = ku.utc_now() + timedelta(hours=8)
    tasks = {t: [{"id": f"TASK-{t[:3].upper()}-{i}", "slip": 0.0} for i in (1, 2, 3)] for t in config.TEAMS}
    blockers = {t: [] for t in config.TEAMS}     # list of (blocker_id, task)
    bn = 0
    try:
        while True:
            team = random.choice(config.TEAMS)
            task, r = random.choice(tasks[team]), random.random()
            bid = None
            if r < 0.10 and len(blockers[team]) < 3:
                bn += 1
                bid, status = f"BLK-{bn:04d}", "BLOCKED"
                blockers[team].append((bid, task))
                task["slip"] += random.uniform(15, 60)
            elif r < 0.30 and blockers[team]:
                bid, btask = blockers[team].pop(random.randrange(len(blockers[team])))
                status = "RESOLVED"
                btask["slip"] = max(0.0, btask["slip"] - random.uniform(10, 40))
                task = btask
            else:
                task["slip"] = max(0.0, task["slip"] + random.uniform(-10, 12))
                status = "AT_RISK" if task["slip"] > config.SLIP_AMBER_MIN else "ON_TRACK"
            ku.send(p, config.TOPIC_TEAM, {
                "serial_number": task["id"], "station_id": "N/A", "event_type": "TEAM_STATUS",
                "timestamp": ku.utc_now().isoformat(), "team": team, "status": status, "blocker_id": bid,
                "eta_planned": planned.isoformat(),
                "eta_current": (planned + timedelta(minutes=task["slip"])).isoformat()}, key=team)
            time.sleep(1.0 / a.rate)
    except KeyboardInterrupt:
        pass
    finally:
        p.flush()


if __name__ == "__main__":
    main()
