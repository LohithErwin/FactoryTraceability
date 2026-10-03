"""team-status -> team_status_events + team_status_rollup/history (+ live push 'teams')."""
from datetime import datetime
import config
from common import pg, kafka_utils as ku, powerbi

pusher = powerbi.Pusher("teams")
SCORE = {"GREEN": 0, "AMBER": 1, "RED": 2}


def parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None


def health(open_blockers, max_slip):
    if open_blockers >= config.RED_BLOCKERS or max_slip >= config.SLIP_RED_MIN:
        return "RED"
    if open_blockers >= 1 or max_slip >= config.SLIP_AMBER_MIN:
        return "AMBER"
    return "GREEN"


def handle(e):
    team = e["team"]
    pg.execute("""INSERT INTO team_status_events(serial_number,station_id,event_type,team,status,blocker_id,eta_planned,eta_current,event_ts)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
               (e["serial_number"], e.get("station_id"), e["event_type"], team, e["status"], e.get("blocker_id"),
                parse(e.get("eta_planned")), parse(e.get("eta_current")), parse(e["timestamp"])))
    blockers = pg.query("""SELECT count(*) AS n FROM (
            SELECT DISTINCT ON (blocker_id) status FROM team_status_events
            WHERE team=%s AND blocker_id IS NOT NULL ORDER BY blocker_id, event_ts DESC, id DESC) t
            WHERE status='BLOCKED'""", (team,))[0]["n"]
    slip = pg.query("""SELECT COALESCE(avg(s),0) AS avg, COALESCE(max(s),0) AS mx FROM (
            SELECT DISTINCT ON (serial_number) EXTRACT(EPOCH FROM (eta_current-eta_planned))/60 AS s
            FROM team_status_events WHERE team=%s AND eta_current IS NOT NULL
            ORDER BY serial_number, event_ts DESC, id DESC) t""", (team,))[0]
    avg, mx = round(float(slip["avg"]), 1), round(float(slip["mx"]), 1)
    h = health(blockers, mx)
    pg.execute("""INSERT INTO team_status_rollup(team,open_blockers,avg_eta_slippage_min,max_eta_slippage_min,health,updated_at)
                  VALUES (%s,%s,%s,%s,%s,now())
                  ON CONFLICT (team) DO UPDATE SET open_blockers=EXCLUDED.open_blockers,
                    avg_eta_slippage_min=EXCLUDED.avg_eta_slippage_min,
                    max_eta_slippage_min=EXCLUDED.max_eta_slippage_min,
                    health=EXCLUDED.health, updated_at=now()""", (team, blockers, avg, mx, h))
    pg.execute("""INSERT INTO team_status_history(team,open_blockers,avg_eta_slippage_min,max_eta_slippage_min,health)
                  VALUES (%s,%s,%s,%s,%s)""", (team, blockers, avg, mx, h))


def tick():
    if pusher.due():
        rows = pg.query("SELECT team, open_blockers, avg_eta_slippage_min, max_eta_slippage_min, health FROM team_status_rollup ORDER BY team")
        now = ku.utc_now().isoformat()
        pusher.push([{"ts": now, "team": r["team"], "open_blockers": int(r["open_blockers"]),
                      "avg_eta_slippage_min": float(r["avg_eta_slippage_min"]),
                      "max_eta_slippage_min": float(r["max_eta_slippage_min"]),
                      "health": r["health"], "health_score": SCORE[r["health"]]} for r in rows])


if __name__ == "__main__":
    ku.run_consumer(config.TOPIC_TEAM, "status-aggregator", handle, tick)
