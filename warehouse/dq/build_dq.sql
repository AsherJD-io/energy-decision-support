SET search_path TO public;

DROP VIEW IF EXISTS dq_pipeline_status;
DROP VIEW IF EXISTS dq_invalid_loads;
DROP VIEW IF EXISTS dq_missing_hours;
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

CREATE OR REPLACE VIEW dq_reconciliation_summary AS
WITH expected AS (
    SELECT
        MIN(time_utc) AS min_ts,
        MAX(time_utc) AS max_ts
    FROM energy_load_clean
),
expected_hours AS (
    SELECT
        (EXTRACT(EPOCH FROM (max_ts - min_ts)) / 3600 + 1)::INT AS expected_hours
    FROM expected
),
actual AS (
    SELECT
        COUNT(DISTINCT time_utc) AS actual_hours
    FROM energy_load_clean
),
missing AS (
    SELECT COUNT(*) AS missing_hours
    FROM dq_missing_hours
),
duplicates AS (
    SELECT COUNT(*) AS duplicate_rows
    FROM (
        SELECT time_utc, country_code, bidding_zone, COUNT(*)
        FROM energy_load_raw
        GROUP BY time_utc, country_code, bidding_zone
        HAVING COUNT(*) > 1
    ) t
),
latest_run AS (
    SELECT
        run_id,
        started_at_utc,
        finished_at_utc
    FROM pipeline_runs
    WHERE status = 'success'
    ORDER BY started_at_utc DESC
    LIMIT 1
)
SELECT
    e.expected_hours,
    a.actual_hours,
    m.missing_hours,
    d.duplicate_rows,
    l.run_id AS last_run_id,
    l.started_at_utc AS last_run_started_at,
    l.finished_at_utc AS last_run_finished_at
FROM expected_hours e
CROSS JOIN actual a
CROSS JOIN missing m
CROSS JOIN duplicates d
LEFT JOIN latest_run l ON TRUE;

CREATE OR REPLACE VIEW dq_assertions AS
SELECT
    'missing_hours_check' AS assertion_name,
    CASE
        WHEN missing_hours = 20 THEN 'PASS'
        ELSE 'FAIL'
    END AS status,
    missing_hours AS observed_value,
    'expected_missing_hours = 20' AS rule
FROM dq_reconciliation_summary

UNION ALL

SELECT
    'duplicate_rows_check',
    CASE
        WHEN duplicate_rows = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    duplicate_rows,
    'duplicate_rows = 0'
FROM dq_reconciliation_summary

UNION ALL

SELECT
    'data_coverage_check',
    CASE
        WHEN actual_hours >= expected_hours - 20 THEN 'PASS'
        ELSE 'FAIL'
    END,
    actual_hours,
    'actual_hours close to expected_hours'
FROM dq_reconciliation_summary;
