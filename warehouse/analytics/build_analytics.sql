SET search_path TO public;

DROP VIEW IF EXISTS hourly_load_anomalies;
DROP VIEW IF EXISTS daily_load_curve_profile;
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
