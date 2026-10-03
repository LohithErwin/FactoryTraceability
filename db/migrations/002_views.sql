-- Views used by the historical Power BI report
CREATE VIEW v_throughput_minute AS
SELECT date_trunc('minute', event_ts) AS minute, station_id, event_type, count(*) AS events,
       count(*) FILTER (WHERE status = 'FAIL') AS fails
FROM station_events GROUP BY 1, 2, 3;

CREATE VIEW v_violations_trend AS
SELECT date_trunc('minute', detected_at) AS minute, violation_type, station_id, count(*) AS violations
FROM violations GROUP BY 1, 2, 3;

CREATE VIEW v_sla_breaches AS
SELECT delivery_id, supplier, breach_type, status, delay_minutes, expected_ts, detected_at, resolved_at,
       EXTRACT(EPOCH FROM (COALESCE(resolved_at, now()) - detected_at)) / 60 AS open_minutes
FROM sla_breaches;

CREATE VIEW v_team_health_trend AS
SELECT date_trunc('minute', snapshot_ts) AS minute, team,
       max(open_blockers) AS open_blockers,
       round(avg(avg_eta_slippage_min), 1) AS avg_eta_slippage_min,
       max(max_eta_slippage_min) AS max_eta_slippage_min,
       max(CASE health WHEN 'RED' THEN 2 WHEN 'AMBER' THEN 1 ELSE 0 END) AS health_score
FROM team_status_history GROUP BY 1, 2;
