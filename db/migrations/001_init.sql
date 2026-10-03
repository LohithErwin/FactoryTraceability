CREATE TABLE station_events (
  id BIGSERIAL PRIMARY KEY,
  serial_number TEXT NOT NULL,
  station_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  team TEXT,
  status TEXT,
  event_ts TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON station_events(serial_number);
CREATE INDEX ON station_events(event_ts);

CREATE TABLE supplier_events (
  id BIGSERIAL PRIMARY KEY,
  delivery_id TEXT NOT NULL,
  supplier TEXT,
  serial_number TEXT,
  station_id TEXT,
  event_type TEXT NOT NULL,
  team TEXT,
  status TEXT NOT NULL,
  event_ts TIMESTAMPTZ NOT NULL,
  expected_ts TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON supplier_events(delivery_id);

CREATE TABLE team_status_events (
  id BIGSERIAL PRIMARY KEY,
  serial_number TEXT NOT NULL,           -- task id
  station_id TEXT,
  event_type TEXT NOT NULL,
  team TEXT NOT NULL,
  status TEXT NOT NULL,                  -- ON_TRACK | AT_RISK | BLOCKED | RESOLVED
  blocker_id TEXT,
  eta_planned TIMESTAMPTZ,
  eta_current TIMESTAMPTZ,
  event_ts TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON team_status_events(team);

CREATE TABLE violations (
  id BIGSERIAL PRIMARY KEY,
  serial_number TEXT NOT NULL,
  station_id TEXT,
  violation_type TEXT NOT NULL,          -- DUPLICATE_SERIAL | OUT_OF_SEQUENCE | TIME_GAP | UNKNOWN_STATION
  detail TEXT,
  event_ts TIMESTAMPTZ,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON violations(detected_at);

CREATE TABLE sla_breaches (
  id BIGSERIAL PRIMARY KEY,
  delivery_id TEXT NOT NULL,
  supplier TEXT,
  breach_type TEXT NOT NULL,             -- LATE | BLOCKED
  delay_minutes NUMERIC(10,1),
  status TEXT NOT NULL DEFAULT 'OPEN',   -- OPEN | RESOLVED
  expected_ts TIMESTAMPTZ,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  UNIQUE (delivery_id, breach_type)
);

CREATE TABLE team_status_rollup (
  team TEXT PRIMARY KEY,
  open_blockers INT NOT NULL,
  avg_eta_slippage_min NUMERIC(10,1) NOT NULL,
  max_eta_slippage_min NUMERIC(10,1) NOT NULL,
  health TEXT NOT NULL,                  -- GREEN | AMBER | RED
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE team_status_history (       -- snapshots for trend analysis
  id BIGSERIAL PRIMARY KEY,
  team TEXT NOT NULL,
  open_blockers INT NOT NULL,
  avg_eta_slippage_min NUMERIC(10,1) NOT NULL,
  max_eta_slippage_min NUMERIC(10,1) NOT NULL,
  health TEXT NOT NULL,
  snapshot_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON team_status_history(snapshot_ts);
