SET search_path TO public;

CREATE TABLE IF NOT EXISTS energy_load_raw (
    time_utc TIMESTAMP NOT NULL,
    country_code TEXT NOT NULL,
    bidding_zone TEXT NOT NULL,
    load_mw DOUBLE PRECISION NOT NULL,
    source TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (time_utc, country_code, bidding_zone)
);

CREATE INDEX IF NOT EXISTS idx_energy_load_raw_zone_time
ON energy_load_raw (country_code, bidding_zone, time_utc);

CREATE INDEX IF NOT EXISTS idx_energy_load_raw_ingested_at
ON energy_load_raw (ingested_at);
