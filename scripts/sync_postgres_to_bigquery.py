import json
import os
import tempfile
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor
from google.cloud import bigquery

PG_HOST = os.getenv("PG_HOST", "34.38.45.106")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "energy_dss")
PG_USER = os.getenv("PG_USER", "energy")
PG_PASSWORD = os.getenv("PG_PASSWORD", "energy123")

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "energy-dss-1773915785")
BQ_DATASET = os.getenv("BQ_DATASET", "energy_dss")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-west1")

OBJECT_QUERIES = {
    "energy_load_raw": """
        SELECT
            time_utc,
            country_code,
            bidding_zone,
            load_mw,
            source,
            ingested_at
        FROM energy_load_raw
        ORDER BY time_utc
    """,
    "energy_load_clean": """
        SELECT
            time_utc,
            country_code,
            bidding_zone,
            load_mw,
            source,
            ingested_at
        FROM energy_load_clean
        ORDER BY time_utc
    """,
    "pipeline_runs": """
        SELECT
            run_id::text AS run_id,
            pipeline_name,
            source_name,
            country_code,
            bidding_zone,
            requested_start_utc,
            requested_end_utc,
            started_at_utc,
            finished_at_utc,
            status,
            rows_seen,
            rows_inserted,
            min_event_time_utc,
            max_event_time_utc,
            resolution_detected,
            source_timeseries_count,
            error_message
        FROM pipeline_runs
        ORDER BY started_at_utc
    """,
    "pipeline_state": """
        SELECT
            pipeline_name,
            country_code,
            bidding_zone,
            last_ingested_at_utc,
            updated_at_utc
        FROM pipeline_state
        ORDER BY updated_at_utc
    """,
    "dq_time_gaps": """
        SELECT
            country_code,
            bidding_zone,
            previous_time,
            time_utc,
            gap_interval::text AS gap_interval
        FROM dq_time_gaps
        ORDER BY time_utc
    """,
    "dq_missing_hours": """
        SELECT
            country_code,
            bidding_zone,
            missing_time
        FROM dq_missing_hours
        ORDER BY missing_time
    """,
    "dq_invalid_loads": """
        SELECT
            country_code,
            bidding_zone,
            time_utc,
            load_mw
        FROM dq_invalid_loads
        ORDER BY time_utc
    """,
    "dq_pipeline_status": """
        SELECT
            raw_rows,
            clean_rows,
            missing_hours,
            time_gaps,
            invalid_loads,
            pipeline_status
        FROM dq_pipeline_status
    """,
    "dq_reconciliation_summary": """
        SELECT
            expected_hours,
            actual_hours,
            missing_hours,
            duplicate_rows,
            last_run_id::text AS last_run_id,
            last_run_started_at,
            last_run_finished_at
        FROM dq_reconciliation_summary
    """,
    "dq_assertions": """
        SELECT
            assertion_name,
            status,
            observed_value,
            rule
        FROM dq_assertions
        ORDER BY assertion_name
    """,
    "daily_load_summary": """
        SELECT
            country_code,
            bidding_zone,
            load_date,
            avg_load_mw,
            peak_load_mw,
            min_load_mw,
            hourly_points
        FROM daily_load_summary
        ORDER BY load_date
    """,
    "hourly_load_anomalies": """
        SELECT
            country_code,
            bidding_zone,
            time_utc,
            load_date,
            load_mw,
            daily_avg_load_mw,
            pct_deviation,
            anomaly_type
        FROM hourly_load_anomalies
        ORDER BY time_utc
    """,
    "daily_load_curve_profile": """
        SELECT
            country_code,
            bidding_zone,
            hour_of_day,
            avg_hourly_load_mw,
            min_hourly_load_mw,
            max_hourly_load_mw,
            observation_count
        FROM daily_load_curve_profile
        ORDER BY hour_of_day
    """,
    "mart_energy_system_metrics": """
        SELECT
            avg_hourly_load_mw,
            max_hourly_load_mw,
            min_hourly_load_mw,
            peak_time_utc,
            peak_load_mw,
            max_avg_daily_load_mw,
            min_avg_daily_load_mw,
            winter_peak_load_mw,
            summer_peak_load_mw,
            avg_midnight_load_mw,
            avg_morning_load_mw,
            avg_evening_load_mw
        FROM mart_energy_system_metrics
    """,
    "mart_energy_system_metrics_long": """
        SELECT
            metric,
            value
        FROM mart_energy_system_metrics_long
        ORDER BY metric
    """,
}

TABLE_SCHEMAS = {
    "energy_load_raw": [
        bigquery.SchemaField("time_utc", "TIMESTAMP"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("load_mw", "FLOAT"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ],
    "energy_load_clean": [
        bigquery.SchemaField("time_utc", "TIMESTAMP"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("load_mw", "FLOAT"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ],
    "pipeline_runs": [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("pipeline_name", "STRING"),
        bigquery.SchemaField("source_name", "STRING"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("requested_start_utc", "TIMESTAMP"),
        bigquery.SchemaField("requested_end_utc", "TIMESTAMP"),
        bigquery.SchemaField("started_at_utc", "TIMESTAMP"),
        bigquery.SchemaField("finished_at_utc", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("rows_seen", "INT64"),
        bigquery.SchemaField("rows_inserted", "INT64"),
        bigquery.SchemaField("min_event_time_utc", "TIMESTAMP"),
        bigquery.SchemaField("max_event_time_utc", "TIMESTAMP"),
        bigquery.SchemaField("resolution_detected", "STRING"),
        bigquery.SchemaField("source_timeseries_count", "INT64"),
        bigquery.SchemaField("error_message", "STRING"),
    ],
    "pipeline_state": [
        bigquery.SchemaField("pipeline_name", "STRING"),
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("last_ingested_at_utc", "TIMESTAMP"),
        bigquery.SchemaField("updated_at_utc", "TIMESTAMP"),
    ],
    "dq_time_gaps": [
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("previous_time", "TIMESTAMP"),
        bigquery.SchemaField("time_utc", "TIMESTAMP"),
        bigquery.SchemaField("gap_interval", "STRING"),
    ],
    "dq_missing_hours": [
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("missing_time", "TIMESTAMP"),
    ],
    "dq_invalid_loads": [
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("time_utc", "TIMESTAMP"),
        bigquery.SchemaField("load_mw", "FLOAT"),
    ],
    "dq_pipeline_status": [
        bigquery.SchemaField("raw_rows", "INT64"),
        bigquery.SchemaField("clean_rows", "INT64"),
        bigquery.SchemaField("missing_hours", "INT64"),
        bigquery.SchemaField("time_gaps", "INT64"),
        bigquery.SchemaField("invalid_loads", "INT64"),
        bigquery.SchemaField("pipeline_status", "STRING"),
    ],
    "dq_reconciliation_summary": [
        bigquery.SchemaField("expected_hours", "INT64"),
        bigquery.SchemaField("actual_hours", "INT64"),
        bigquery.SchemaField("missing_hours", "INT64"),
        bigquery.SchemaField("duplicate_rows", "INT64"),
        bigquery.SchemaField("last_run_id", "STRING"),
        bigquery.SchemaField("last_run_started_at", "TIMESTAMP"),
        bigquery.SchemaField("last_run_finished_at", "TIMESTAMP"),
    ],
    "dq_assertions": [
        bigquery.SchemaField("assertion_name", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("observed_value", "INT64"),
        bigquery.SchemaField("rule", "STRING"),
    ],
    "daily_load_summary": [
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("load_date", "DATE"),
        bigquery.SchemaField("avg_load_mw", "FLOAT"),
        bigquery.SchemaField("peak_load_mw", "FLOAT"),
        bigquery.SchemaField("min_load_mw", "FLOAT"),
        bigquery.SchemaField("hourly_points", "INT64"),
    ],
    "hourly_load_anomalies": [
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("time_utc", "TIMESTAMP"),
        bigquery.SchemaField("load_date", "DATE"),
        bigquery.SchemaField("load_mw", "FLOAT"),
        bigquery.SchemaField("daily_avg_load_mw", "FLOAT"),
        bigquery.SchemaField("pct_deviation", "FLOAT"),
        bigquery.SchemaField("anomaly_type", "STRING"),
    ],
    "daily_load_curve_profile": [
        bigquery.SchemaField("country_code", "STRING"),
        bigquery.SchemaField("bidding_zone", "STRING"),
        bigquery.SchemaField("hour_of_day", "FLOAT"),
        bigquery.SchemaField("avg_hourly_load_mw", "FLOAT"),
        bigquery.SchemaField("min_hourly_load_mw", "FLOAT"),
        bigquery.SchemaField("max_hourly_load_mw", "FLOAT"),
        bigquery.SchemaField("observation_count", "INT64"),
    ],
    "mart_energy_system_metrics": [
        bigquery.SchemaField("avg_hourly_load_mw", "FLOAT"),
        bigquery.SchemaField("max_hourly_load_mw", "FLOAT"),
        bigquery.SchemaField("min_hourly_load_mw", "FLOAT"),
        bigquery.SchemaField("peak_time_utc", "TIMESTAMP"),
        bigquery.SchemaField("peak_load_mw", "FLOAT"),
        bigquery.SchemaField("max_avg_daily_load_mw", "FLOAT"),
        bigquery.SchemaField("min_avg_daily_load_mw", "FLOAT"),
        bigquery.SchemaField("winter_peak_load_mw", "FLOAT"),
        bigquery.SchemaField("summer_peak_load_mw", "FLOAT"),
        bigquery.SchemaField("avg_midnight_load_mw", "FLOAT"),
        bigquery.SchemaField("avg_morning_load_mw", "FLOAT"),
        bigquery.SchemaField("avg_evening_load_mw", "FLOAT"),
    ],
    "mart_energy_system_metrics_long": [
        bigquery.SchemaField("metric", "STRING"),
        bigquery.SchemaField("value", "STRING"),
    ],
}


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def normalize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def fetch_rows(query):
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()

    return [{k: normalize_value(v) for k, v in row.items()} for row in rows]


def ensure_dataset(client):
    dataset_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = BQ_LOCATION
    client.create_dataset(dataset, exists_ok=True)


def ensure_table_schema(client, table_name):
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    schema = TABLE_SCHEMAS[table_name]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    truncate_sql = f"TRUNCATE TABLE `{table_id}`"
    client.query(truncate_sql).result()


def load_rows_to_bigquery(client, table_name, rows):
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    if not rows:
        ensure_table_schema(client, table_name)
        print(f"Loaded 0 rows into {table_id}")
        return

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        temp_path = tmp.name
        for row in rows:
            tmp.write(json.dumps(row))
            tmp.write("\n")

    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMAS[table_name],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(temp_path, "rb") as f:
        job = client.load_table_from_file(
            f,
            table_id,
            job_config=job_config,
        )

    job.result()
    os.remove(temp_path)

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_id}")


def main():
    print("Connecting to PostgreSQL source...")
    print(f"PG source: {PG_HOST}:{PG_PORT}/{PG_DB}")

    client = bigquery.Client(project=BQ_PROJECT_ID)
    ensure_dataset(client)

    print(f"BigQuery target: {BQ_PROJECT_ID}.{BQ_DATASET} ({BQ_LOCATION})")

    for object_name, query in OBJECT_QUERIES.items():
        print(f"\nExtracting {object_name} from PostgreSQL...")
        rows = fetch_rows(query)
        print(f"Fetched {len(rows)} rows from {object_name}")

        print(f"Loading {object_name} into BigQuery...")
        load_rows_to_bigquery(client, object_name, rows)

    print("\nPostgreSQL to BigQuery sync complete.")


if __name__ == "__main__":
    main()
