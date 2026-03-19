import os
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )


def ensure_pipeline_state_table():
    create_sql = """
        CREATE TABLE IF NOT EXISTS pipeline_state (
            pipeline_name TEXT NOT NULL,
            country_code TEXT NOT NULL,
            bidding_zone TEXT NOT NULL,
            last_ingested_at_utc TIMESTAMPTZ NOT NULL,
            updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (pipeline_name, country_code, bidding_zone)
        )
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()


def get_last_ingested_at(pipeline_name: str, country_code: str, bidding_zone: str):
    query = """
        SELECT last_ingested_at_utc
        FROM pipeline_state
        WHERE pipeline_name = %s
          AND country_code = %s
          AND bidding_zone = %s
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (pipeline_name, country_code, bidding_zone))
            row = cur.fetchone()

    return row[0] if row else None


def upsert_pipeline_state(
    pipeline_name: str,
    country_code: str,
    bidding_zone: str,
    last_ingested_at_utc: datetime,
):
    query = """
        INSERT INTO pipeline_state (
            pipeline_name,
            country_code,
            bidding_zone,
            last_ingested_at_utc,
            updated_at_utc
        )
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (pipeline_name, country_code, bidding_zone)
        DO UPDATE SET
            last_ingested_at_utc = EXCLUDED.last_ingested_at_utc,
            updated_at_utc = NOW()
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    pipeline_name,
                    country_code,
                    bidding_zone,
                    last_ingested_at_utc,
                ),
            )
        conn.commit()


def insert_load_rows(rows, target_table: str):
    """
    rows: list of tuples
    (time_utc, country_code, bidding_zone, load_mw)
    """

    if not rows:
        return 0

    insert_sql = f"""
        INSERT INTO {target_table} (
            time_utc,
            country_code,
            bidding_zone,
            load_mw
        )
        VALUES %s
        ON CONFLICT (time_utc, country_code, bidding_zone) DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=1000)
            inserted_count = cur.rowcount
        conn.commit()

    return inserted_count
