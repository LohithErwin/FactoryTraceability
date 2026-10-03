"""supplier-events -> supplier_events + sla_breaches (+ live push 'sla')."""
from datetime import datetime
import config
from common import pg, kafka_utils as ku, powerbi

pusher = powerbi.Pusher("sla")


def parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def open_breach(e, btype, delay):
    pg.execute("""INSERT INTO sla_breaches(delivery_id,supplier,breach_type,delay_minutes,status,expected_ts)
                  VALUES (%s,%s,%s,%s,'OPEN',%s)
                  ON CONFLICT (delivery_id,breach_type) DO UPDATE SET delay_minutes=EXCLUDED.delay_minutes
                  WHERE sla_breaches.status='OPEN'""",
               (e["delivery_id"], e.get("supplier"), btype, round(delay, 1), e["expected_ts"]))
    ku.log.info("SLA %s %s delay=%.0fmin", btype, e["delivery_id"], delay)


def handle(e):
    did, status = e["delivery_id"], e["status"]
    ts, exp = parse(e["timestamp"]), parse(e["expected_ts"])
    pg.execute("""INSERT INTO supplier_events(delivery_id,supplier,serial_number,station_id,event_type,team,status,event_ts,expected_ts)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
               (did, e.get("supplier"), e.get("serial_number"), e.get("station_id"), e["event_type"],
                e.get("team"), status, ts, exp))
    late = (ts - exp).total_seconds() / 60
    if status == "BLOCKED":
        open_breach(e, "BLOCKED", max(late, 0))
    elif status == "DELIVERED":
        n = pg.execute("""UPDATE sla_breaches SET status='RESOLVED', resolved_at=%s, delay_minutes=%s
                          WHERE delivery_id=%s AND status='OPEN'""", (ts, round(max(late, 0), 1), did))
        if n == 0 and late > config.SLA_THRESHOLD_MIN:   # delivered late without prior open breach
            pg.execute("""INSERT INTO sla_breaches(delivery_id,supplier,breach_type,delay_minutes,status,expected_ts,resolved_at)
                          VALUES (%s,%s,'LATE',%s,'RESOLVED',%s,%s) ON CONFLICT DO NOTHING""",
                       (did, e.get("supplier"), round(late, 1), exp, ts))
    elif late > config.SLA_THRESHOLD_MIN:                # still in transit and overdue
        open_breach(e, "LATE", late)


def snapshot():
    r = pg.query("""SELECT
        count(*) FILTER (WHERE status='OPEN') AS open_breaches,
        count(*) FILTER (WHERE status='OPEN' AND breach_type='BLOCKED') AS blocked_open,
        count(*) FILTER (WHERE status='OPEN' AND breach_type='LATE') AS late_open,
        count(*) AS total_breaches,
        (SELECT count(DISTINCT delivery_id) FROM supplier_events) AS deliveries_total
        FROM sla_breaches""")[0]
    return [{"ts": ku.utc_now().isoformat(), **{k: int(v) for k, v in r.items()}}]


def tick():
    if pusher.due():
        pusher.push(snapshot())


if __name__ == "__main__":
    ku.run_consumer(config.TOPIC_SUPPLIER, "sla-monitor", handle, tick)
