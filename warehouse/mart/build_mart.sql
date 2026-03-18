SET search_path TO public;

DROP VIEW IF EXISTS mart_energy_system_metrics_long;
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
