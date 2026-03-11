CREATE TABLE IF NOT EXISTS energy_load_raw (
    time_utc TIMESTAMP NOT NULL,
    country_code TEXT NOT NULL,
    bidding_zone TEXT NOT NULL,
    load_mw NUMERIC NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (time_utc, country_code, bidding_zone)
);
