"""station-events -> raw_events + violations (+ live push 'metrics')."""
from datetime import datetime
import config
from common import pg, kafka_utils as ku, powerbi

pusher = powerbi.Pusher("metrics")


def parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def flag(e, vtype, detail):
    pg.execute("INSERT INTO violations(serial_number,station_id,violation_type,detail,event_ts) VALUES (%s,%s,%s,%s,%s)",
               (e["serial_number"], e["station_id"], vtype, detail, e["timestamp"]))
    ku.log.info("VIOLATION %s %s %s", vtype, e["serial_number"], detail)


def handle(e):
    sn, st, ts = e["serial_number"], e["station_id"], parse(e["timestamp"])
    prev = pg.query("SELECT station_id, event_ts FROM station_events WHERE serial_number=%s ORDER BY id", (sn,))
    pg.execute("INSERT INTO station_events(serial_number,station_id,event_type,team,status,event_ts) VALUES (%s,%s,%s,%s,%s,%s)",
               (sn, st, e["event_type"], e.get("team"), e.get("status"), ts))
    idx = config.STATION_INDEX.get(st)
    if idx is None:
        return flag(e, "UNKNOWN_STATION", f"station {st} not on line")
    if any(p["station_id"] == st for p in prev):
        return flag(e, "DUPLICATE_SERIAL", f"{sn} already processed at {st}")
    last_idx = config.STATION_INDEX.get(prev[-1]["station_id"], -1) if prev else -1
    if idx != last_idx + 1:
        nxt = config.STATIONS[last_idx + 1]["station_id"] if last_idx + 1 < len(config.STATIONS) else "END"
        flag(e, "OUT_OF_SEQUENCE", f"expected {nxt}, got {st}")
    if prev:
        gap = (ts - prev[-1]["event_ts"]).total_seconds()
        if gap < 0:
            flag(e, "TIME_GAP", f"timestamp {-gap:.0f}s earlier than previous station")
        elif gap > config.MAX_GAP_SECONDS:
            flag(e, "TIME_GAP", f"{gap:.0f}s between stations (max {config.MAX_GAP_SECONDS:.0f}s)")


def snapshot():
    r = pg.query("""SELECT
        (SELECT count(*) FROM violations) AS violation_count,
        (SELECT count(*) FROM violations WHERE violation_type='DUPLICATE_SERIAL') AS duplicate_count,
        (SELECT count(*) FROM violations WHERE violation_type='OUT_OF_SEQUENCE') AS sequence_count,
        (SELECT count(*) FROM violations WHERE violation_type='TIME_GAP') AS gap_count,
        (SELECT count(*) FROM station_events WHERE ingested_at > now() - interval '60 seconds') AS throughput_per_min,
        (SELECT count(*) FROM station_events) AS events_total""")[0]
    return [{"ts": ku.utc_now().isoformat(), **{k: int(v) for k, v in r.items()}}]


def tick():
    if pusher.due():
        pusher.push(snapshot())


if __name__ == "__main__":
    ku.run_consumer(config.TOPIC_STATION, "integrity-checker", handle, tick)
