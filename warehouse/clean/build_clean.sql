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

CREATE INDEX IF NOT EXISTS idx_energy_load_clean_zone_time
ON energy_load_clean (country_code, bidding_zone, time_utc);

CREATE INDEX IF NOT EXISTS idx_energy_load_clean_ingested_at
ON energy_load_clean (ingested_at);

INSERT INTO energy_load_clean (
    time_utc,
    country_code,
    bidding_zone,
    load_mw,
    source,
    ingested_at
)
SELECT
    r.time_utc,
    r.country_code,
    r.bidding_zone,
    r.load_mw,
    COALESCE(r.source, 'ENTSO-E') AS source,
    r.ingested_at
FROM energy_load_raw r
WHERE r.load_mw IS NOT NULL
  AND r.load_mw >= 0
ON CONFLICT (time_utc, country_code, bidding_zone) DO UPDATE
SET
    load_mw = EXCLUDED.load_mw,
    source = EXCLUDED.source,
    ingested_at = EXCLUDED.ingested_at;
