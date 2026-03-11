import os
import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )


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
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (time_utc, country_code, bidding_zone) DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()

    return len(rows)
