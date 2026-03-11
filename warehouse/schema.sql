-- =========================================================
-- Energy Decision Support System (DSS)
-- Warehouse Schema
-- =========================================================

SET search_path TO public;

-- =========================================================
-- 1. RAW LAYER
-- =========================================================

CREATE TABLE IF NOT EXISTS energy_load_raw (
    time_utc TIMESTAMP NOT NULL,
    country_code TEXT NOT NULL,
    bidding_zone TEXT NOT NULL,
    load_mw DOUBLE PRECISION NOT NULL,
    source TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_energy_load_raw_time
ON energy_load_raw (time_utc);

CREATE INDEX IF NOT EXISTS idx_energy_load_raw_zone_time
ON energy_load_raw (country_code, bidding_zone, time_utc);

-- =========================================================
-- 2. CLEAN LAYER
-- =========================================================

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

-- =========================================================
-- 3. DATA QUALITY LAYER
-- =========================================================

DROP VIEW IF EXISTS dq_time_gaps;
CREATE VIEW dq_time_gaps AS
WITH ordered_load AS (
    SELECT
        country_code,
        bidding_zone,
        time_utc,
        LAG(time_utc) OVER (
            PARTITION BY country_code, bidding_zone
            ORDER BY time_utc
        ) AS previous_time
    FROM energy_load_clean
)
SELECT
    country_code,
    bidding_zone,
    previous_time,
    time_utc,
    time_utc - previous_time AS gap_interval
FROM ordered_load
WHERE previous_time IS NOT NULL
  AND time_utc - previous_time > INTERVAL '1 hour'
ORDER BY
    country_code,
    bidding_zone,
    time_utc;

DROP VIEW IF EXISTS dq_missing_hours;
CREATE VIEW dq_missing_hours AS
WITH series_bounds AS (
    SELECT
        country_code,
        bidding_zone,
        MIN(time_utc) AS min_time,
        MAX(time_utc) AS max_time
    FROM energy_load_clean
    GROUP BY
        country_code,
        bidding_zone
),
expected_hours AS (
    SELECT
        sb.country_code,
        sb.bidding_zone,
        generate_series(
            sb.min_time,
            sb.max_time,
            INTERVAL '1 hour'
        ) AS expected_time
    FROM series_bounds sb
)
SELECT
    e.country_code,
    e.bidding_zone,
    e.expected_time AS missing_time
FROM expected_hours e
LEFT JOIN energy_load_clean c
    ON e.country_code = c.country_code
   AND e.bidding_zone = c.bidding_zone
   AND e.expected_time = c.time_utc
WHERE c.time_utc IS NULL
ORDER BY
    e.country_code,
    e.bidding_zone,
    e.expected_time;

DROP VIEW IF EXISTS dq_invalid_loads;
CREATE VIEW dq_invalid_loads AS
SELECT
    country_code,
    bidding_zone,
    time_utc,
    load_mw
FROM energy_load_clean
WHERE load_mw IS NULL
   OR load_mw < 0
ORDER BY
    country_code,
    bidding_zone,
    time_utc;

DROP VIEW IF EXISTS dq_pipeline_status;
CREATE VIEW dq_pipeline_status AS
WITH raw_stats AS (
    SELECT COUNT(*) AS raw_rows
    FROM energy_load_raw
),
clean_stats AS (
    SELECT COUNT(*) AS clean_rows
    FROM energy_load_clean
),
missing_stats AS (
    SELECT COUNT(*) AS missing_hours
    FROM dq_missing_hours
),
gap_stats AS (
    SELECT COUNT(*) AS time_gaps
    FROM dq_time_gaps
),
invalid_stats AS (
    SELECT COUNT(*) AS invalid_loads
    FROM dq_invalid_loads
)
SELECT
    r.raw_rows,
    c.clean_rows,
    m.missing_hours,
    g.time_gaps,
    i.invalid_loads,
    CASE
        WHEN c.clean_rows = 0 THEN 'FAILED'
        WHEN i.invalid_loads > 0 THEN 'FAILED'
        WHEN m.missing_hours > 24 THEN 'WARNING'
        WHEN g.time_gaps > 24 THEN 'WARNING'
        ELSE 'HEALTHY'
    END AS pipeline_status
FROM raw_stats r
CROSS JOIN clean_stats c
CROSS JOIN missing_stats m
CROSS JOIN gap_stats g
CROSS JOIN invalid_stats i;

-- =========================================================
-- 4. ANALYTICS LAYER
-- =========================================================

DROP VIEW IF EXISTS daily_load_summary;
CREATE VIEW daily_load_summary AS
SELECT
    country_code,
    bidding_zone,
    DATE(time_utc) AS load_date,
    AVG(load_mw) AS avg_load_mw,
    MAX(load_mw) AS peak_load_mw,
    MIN(load_mw) AS min_load_mw,
    COUNT(*) AS hourly_points
FROM energy_load_clean
GROUP BY
    country_code,
    bidding_zone,
    DATE(time_utc)
ORDER BY
    load_date;

DROP VIEW IF EXISTS hourly_load_anomalies;
CREATE VIEW hourly_load_anomalies AS
WITH daily_baseline AS (
    SELECT
        country_code,
        bidding_zone,
        DATE(time_utc) AS load_date,
        AVG(load_mw) AS daily_avg_load_mw
    FROM energy_load_clean
    GROUP BY
        country_code,
        bidding_zone,
        DATE(time_utc)
)
SELECT
    r.country_code,
    r.bidding_zone,
    r.time_utc,
    DATE(r.time_utc) AS load_date,
    r.load_mw,
    d.daily_avg_load_mw,
    ROUND(
        (((r.load_mw - d.daily_avg_load_mw) / d.daily_avg_load_mw) * 100.0)::numeric,
        2
    ) AS pct_deviation,
    CASE
        WHEN r.load_mw > d.daily_avg_load_mw * 1.10 THEN 'HIGH_ANOMALY'
        WHEN r.load_mw < d.daily_avg_load_mw * 0.90 THEN 'LOW_ANOMALY'
    END AS anomaly_type
FROM energy_load_clean r
JOIN daily_baseline d
    ON r.country_code = d.country_code
   AND r.bidding_zone = d.bidding_zone
   AND DATE(r.time_utc) = d.load_date
WHERE r.load_mw > d.daily_avg_load_mw * 1.10
   OR r.load_mw < d.daily_avg_load_mw * 0.90
ORDER BY
    r.time_utc;

DROP VIEW IF EXISTS daily_load_curve_profile;
CREATE VIEW daily_load_curve_profile AS
SELECT
    country_code,
    bidding_zone,
    EXTRACT(HOUR FROM time_utc) AS hour_of_day,
    AVG(load_mw) AS avg_hourly_load_mw,
    MIN(load_mw) AS min_hourly_load_mw,
    MAX(load_mw) AS max_hourly_load_mw,
    COUNT(*) AS observation_count
FROM energy_load_clean
GROUP BY
    country_code,
    bidding_zone,
    EXTRACT(HOUR FROM time_utc)
ORDER BY
    hour_of_day;

-- =========================================================
-- 5. MART LAYER
-- =========================================================

DROP VIEW IF EXISTS mart_energy_system_metrics;
CREATE VIEW mart_energy_system_metrics AS
WITH base_stats AS (
    SELECT
        AVG(load_mw) AS avg_hourly_load_mw,
        MAX(load_mw) AS max_hourly_load_mw,
        MIN(load_mw) AS min_hourly_load_mw
    FROM energy_load_clean
),
peak_hour AS (
    SELECT
        time_utc AS peak_time_utc,
        load_mw AS peak_load_mw
    FROM energy_load_clean
    ORDER BY load_mw DESC
    LIMIT 1
),
daily_stats AS (
    SELECT
        DATE(time_utc) AS load_date,
        AVG(load_mw) AS avg_daily_load_mw,
        MAX(load_mw) AS peak_daily_load_mw
    FROM energy_load_clean
    GROUP BY DATE(time_utc)
),
daily_extremes AS (
    SELECT
        MAX(avg_daily_load_mw) AS max_avg_daily_load_mw,
        MIN(avg_daily_load_mw) AS min_avg_daily_load_mw
    FROM daily_stats
),
seasonal_peaks AS (
    SELECT
        MAX(CASE
            WHEN EXTRACT(MONTH FROM time_utc) IN (12, 1, 2) THEN load_mw
        END) AS winter_peak_load_mw,
        MAX(CASE
            WHEN EXTRACT(MONTH FROM time_utc) IN (6, 7, 8) THEN load_mw
        END) AS summer_peak_load_mw
    FROM energy_load_clean
),
load_curve_shape AS (
    SELECT
        MAX(CASE WHEN hour_of_day = 0 THEN avg_load END) AS avg_midnight_load_mw,
        MAX(CASE WHEN hour_of_day = 6 THEN avg_load END) AS avg_morning_load_mw,
        MAX(CASE WHEN hour_of_day = 18 THEN avg_load END) AS avg_evening_load_mw
    FROM (
        SELECT
            EXTRACT(HOUR FROM time_utc) AS hour_of_day,
            AVG(load_mw) AS avg_load
        FROM energy_load_clean
        GROUP BY EXTRACT(HOUR FROM time_utc)
    ) h
)
SELECT
    b.avg_hourly_load_mw,
    b.max_hourly_load_mw,
    b.min_hourly_load_mw,
    p.peak_time_utc,
    p.peak_load_mw,
    d.max_avg_daily_load_mw,
    d.min_avg_daily_load_mw,
    s.winter_peak_load_mw,
    s.summer_peak_load_mw,
    l.avg_midnight_load_mw,
    l.avg_morning_load_mw,
    l.avg_evening_load_mw
FROM base_stats b
CROSS JOIN peak_hour p
CROSS JOIN daily_extremes d
CROSS JOIN seasonal_peaks s
CROSS JOIN load_curve_shape l;

DROP VIEW IF EXISTS mart_energy_system_metrics_long;
CREATE VIEW mart_energy_system_metrics_long AS
SELECT 'avg_hourly_load_mw' AS metric, avg_hourly_load_mw::TEXT AS value
FROM mart_energy_system_metrics
UNION ALL
SELECT 'max_hourly_load_mw', max_hourly_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'min_hourly_load_mw', min_hourly_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'peak_time_utc', peak_time_utc::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'peak_load_mw', peak_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'max_avg_daily_load_mw', max_avg_daily_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'min_avg_daily_load_mw', min_avg_daily_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'winter_peak_load_mw', winter_peak_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'summer_peak_load_mw', summer_peak_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'avg_midnight_load_mw', avg_midnight_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'avg_morning_load_mw', avg_morning_load_mw::TEXT
FROM mart_energy_system_metrics
UNION ALL
SELECT 'avg_evening_load_mw', avg_evening_load_mw::TEXT
FROM mart_energy_system_metrics;
