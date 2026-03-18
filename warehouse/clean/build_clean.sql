SET search_path TO public;

CREATE TABLE IF NOT EXISTS energy_load_clean (
    time_utc TIMESTAMP NOT NULL,
    country_code TEXT NOT NULL,
    bidding_zone TEXT NOT NULL,
    load_mw DOUBLE PRECISION NOT NULL,
    source TEXT,
    ingested_at TIMESTAMP,
    PRIMARY KEY (time_utc, country_code, bidding_zone)
);

CREATE INDEX IF NOT EXISTS idx_energy_load_clean_time
ON energy_load_clean (time_utc);

CREATE INDEX IF NOT EXISTS idx_energy_load_clean_zone_time
ON energy_load_clean (country_code, bidding_zone, time_utc);
