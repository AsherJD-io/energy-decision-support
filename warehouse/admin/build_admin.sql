SET search_path TO public;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id UUID PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    country_code TEXT NOT NULL,
    bidding_zone TEXT NOT NULL,
    requested_start_utc TIMESTAMPTZ NOT NULL,
    requested_end_utc TIMESTAMPTZ NOT NULL,
    started_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at_utc TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_seen INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    min_event_time_utc TIMESTAMPTZ,
    max_event_time_utc TIMESTAMPTZ,
    resolution_detected TEXT,
    source_timeseries_count INTEGER,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name_zone_started
ON pipeline_runs (pipeline_name, country_code, bidding_zone, started_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_success_latest
ON pipeline_runs (pipeline_name, country_code, bidding_zone, started_at_utc DESC)
WHERE status = 'success';

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_started
ON pipeline_runs (status, started_at_utc DESC);

CREATE TABLE IF NOT EXISTS pipeline_state (
    pipeline_name TEXT NOT NULL,
    country_code TEXT NOT NULL,
    bidding_zone TEXT NOT NULL,
    last_ingested_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_name, country_code, bidding_zone)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_state_updated_at
ON pipeline_state (updated_at_utc DESC);
