import argparse
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
import xml.etree.ElementTree as ET

import requests

from db import (
    ensure_pipeline_state_table,
    get_last_ingested_at,
    insert_load_rows,
    upsert_pipeline_state,
)

try:
    import psycopg2
except ImportError:
    psycopg2 = None

ENTSOE_BASE_URL = "https://web-api.tp.entsoe.eu/api"
PIPELINE_NAME = "energy_dss_batch_ingestion"
SOURCE_NAME = "entsoe_actual_total_load"

NS = {
    "ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"
}


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest ENTSO-E load data into PostgreSQL")
    parser.add_argument("--country-code", required=True)
    parser.add_argument("--bidding-zone", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--target-table", required=True)
    return parser.parse_args()


def validate_env():
    token = os.getenv("ENTSOE_API_TOKEN")
    if not token:
        raise RuntimeError("ENTSOE_API_TOKEN not set")
    return token


def get_pg_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed, cannot write pipeline audit metadata")

    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )


def insert_pipeline_run(
    conn,
    run_id,
    pipeline_name,
    source_name,
    country_code,
    bidding_zone,
    requested_start_utc,
    requested_end_utc,
    started_at_utc,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (
                run_id,
                pipeline_name,
                source_name,
                country_code,
                bidding_zone,
                requested_start_utc,
                requested_end_utc,
                started_at_utc,
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'running')
            """,
            (
                run_id,
                pipeline_name,
                source_name,
                country_code,
                bidding_zone,
                requested_start_utc,
                requested_end_utc,
                started_at_utc,
            ),
        )
    conn.commit()


def update_pipeline_run_success(
    conn,
    run_id,
    finished_at_utc,
    rows_seen,
    rows_inserted,
    min_event_time_utc,
    max_event_time_utc,
    resolution_detected,
    source_timeseries_count,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET finished_at_utc = %s,
                status = 'success',
                rows_seen = %s,
                rows_inserted = %s,
                min_event_time_utc = %s,
                max_event_time_utc = %s,
                resolution_detected = %s,
                source_timeseries_count = %s
            WHERE run_id = %s
            """,
            (
                finished_at_utc,
                rows_seen,
                rows_inserted,
                min_event_time_utc,
                max_event_time_utc,
                resolution_detected,
                source_timeseries_count,
                run_id,
            ),
        )
    conn.commit()


def update_pipeline_run_failed(
    conn,
    run_id,
    finished_at_utc,
    rows_seen,
    rows_inserted,
    min_event_time_utc,
    max_event_time_utc,
    resolution_detected,
    source_timeseries_count,
    error_message,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET finished_at_utc = %s,
                status = 'failed',
                rows_seen = %s,
                rows_inserted = %s,
                min_event_time_utc = %s,
                max_event_time_utc = %s,
                resolution_detected = %s,
                source_timeseries_count = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (
                finished_at_utc,
                rows_seen,
                rows_inserted,
                min_event_time_utc,
                max_event_time_utc,
                resolution_detected,
                source_timeseries_count,
                error_message[:2000] if error_message else None,
                run_id,
            ),
        )
    conn.commit()


def parse_requested_window(start_date_str, end_date_str):
    start_day = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_day = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    requested_start_utc = datetime.combine(
        start_day,
        time(0, 0),
        tzinfo=timezone.utc,
    )
    requested_end_utc = datetime.combine(
        end_day,
        time(23, 0),
        tzinfo=timezone.utc,
    )

    if requested_end_utc < requested_start_utc:
        raise ValueError("end-date must be greater than or equal to start-date")

    return requested_start_utc, requested_end_utc


def resolve_effective_window(country_code, bidding_zone, start_date_str, end_date_str):
    requested_start_utc, requested_end_utc = parse_requested_window(start_date_str, end_date_str)

    last_ingested_at_utc = get_last_ingested_at(
        PIPELINE_NAME,
        country_code,
        bidding_zone,
    )

    if last_ingested_at_utc is None:
        effective_start_utc = requested_start_utc
    else:
        effective_start_utc = last_ingested_at_utc + timedelta(hours=1)
        if effective_start_utc < requested_start_utc:
            effective_start_utc = requested_start_utc

    return last_ingested_at_utc, effective_start_utc, requested_end_utc


def fetch_entsoe_xml(token, bidding_zone, start_dt_utc, end_dt_utc):
    params = {
        "securityToken": token,
        "documentType": "A65",
        "processType": "A16",
        "outBiddingZone_Domain": bidding_zone,
        "periodStart": start_dt_utc.strftime("%Y%m%d%H%M"),
        "periodEnd": (end_dt_utc + timedelta(hours=1)).strftime("%Y%m%d%H%M"),
    }

    response = requests.get(ENTSOE_BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.text


def parse_resolution(resolution):
    if resolution == "PT60M":
        return timedelta(hours=1)
    if resolution == "PT15M":
        return timedelta(minutes=15)
    raise ValueError(f"Unsupported resolution {resolution}")


def parse_xml(xml_text, country_code, bidding_zone):
    root = ET.fromstring(xml_text)

    rows = []
    resolutions_seen = set()

    timeseries = root.findall(".//ns:TimeSeries", NS)
    source_timeseries_count = len(timeseries)

    for ts in timeseries:
        periods = ts.findall(".//ns:Period", NS)

        for period in periods:
            start_text = period.findtext("ns:timeInterval/ns:start", namespaces=NS)
            resolution = period.findtext("ns:resolution", namespaces=NS)

            if not start_text or not resolution:
                continue

            resolutions_seen.add(resolution)

            start_dt = datetime.strptime(start_text, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            delta = parse_resolution(resolution)

            points = period.findall("ns:Point", NS)

            for p in points:
                position_text = p.findtext("ns:position", namespaces=NS)
                quantity_text = p.findtext("ns:quantity", namespaces=NS)

                if not position_text or not quantity_text:
                    continue

                position = int(position_text)
                quantity = float(quantity_text)

                ts_time = start_dt + (position - 1) * delta

                rows.append(
                    (
                        ts_time,
                        country_code,
                        bidding_zone,
                        quantity,
                    )
                )

    resolution_detected = ",".join(sorted(resolutions_seen)) if resolutions_seen else None

    return rows, source_timeseries_count, resolution_detected


def quarter_end_date(current_date: date):
    if current_date.month <= 3:
        return date(current_date.year, 3, 31)
    if current_date.month <= 6:
        return date(current_date.year, 6, 30)
    if current_date.month <= 9:
        return date(current_date.year, 9, 30)
    return date(current_date.year, 12, 31)


def quarter_windows(start_dt_utc, end_dt_utc):
    current = start_dt_utc

    while current <= end_dt_utc:
        q_end_date = quarter_end_date(current.date())
        q_end_dt = datetime.combine(
            q_end_date,
            time(23, 0),
            tzinfo=timezone.utc,
        )

        if q_end_dt > end_dt_utc:
            q_end_dt = end_dt_utc

        yield current, q_end_dt
        current = q_end_dt + timedelta(hours=1)


def main():
    args = parse_args()
    token = validate_env()

    ensure_pipeline_state_table()

    last_ingested_at_utc, effective_start_utc, effective_end_utc = resolve_effective_window(
        country_code=args.country_code,
        bidding_zone=args.bidding_zone,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
    )

    run_id = str(uuid.uuid4())
    started_at_utc = datetime.now(timezone.utc)

    rows_seen = 0
    rows_inserted = 0
    min_event_time_utc = None
    max_event_time_utc = None
    resolutions_seen = set()
    source_timeseries_count = 0

    audit_conn = get_pg_connection()

    insert_pipeline_run(
        conn=audit_conn,
        run_id=run_id,
        pipeline_name=PIPELINE_NAME,
        source_name=SOURCE_NAME,
        country_code=args.country_code,
        bidding_zone=args.bidding_zone,
        requested_start_utc=effective_start_utc,
        requested_end_utc=effective_end_utc,
        started_at_utc=started_at_utc,
    )

    print(f"Resolved state for {args.country_code} / {args.bidding_zone}")
    print(f"Last ingested at UTC: {last_ingested_at_utc}")
    print(f"Effective request window UTC: {effective_start_utc} -> {effective_end_utc}")

    try:
        if effective_start_utc > effective_end_utc:
            print("No new window to ingest. Pipeline state is already ahead of requested range.")

            update_pipeline_run_success(
                conn=audit_conn,
                run_id=run_id,
                finished_at_utc=datetime.now(timezone.utc),
                rows_seen=0,
                rows_inserted=0,
                min_event_time_utc=None,
                max_event_time_utc=None,
                resolution_detected=None,
                source_timeseries_count=0,
            )
            return

        for chunk_start_utc, chunk_end_utc in quarter_windows(effective_start_utc, effective_end_utc):
            print(f"Fetching ENTSO-E data for chunk {chunk_start_utc} -> {chunk_end_utc}...")

            xml = fetch_entsoe_xml(
                token=token,
                bidding_zone=args.bidding_zone,
                start_dt_utc=chunk_start_utc,
                end_dt_utc=chunk_end_utc,
            )

            print(f"Parsing XML for chunk {chunk_start_utc} -> {chunk_end_utc}...")

            chunk_rows, chunk_timeseries_count, chunk_resolution_detected = parse_xml(
                xml,
                args.country_code,
                args.bidding_zone,
            )

            chunk_rows_seen = len(chunk_rows)
            rows_seen += chunk_rows_seen
            source_timeseries_count += chunk_timeseries_count

            if chunk_resolution_detected:
                for item in chunk_resolution_detected.split(","):
                    if item:
                        resolutions_seen.add(item)

            if chunk_rows:
                chunk_event_times = [row[0] for row in chunk_rows]
                chunk_min = min(chunk_event_times)
                chunk_max = max(chunk_event_times)

                if min_event_time_utc is None or chunk_min < min_event_time_utc:
                    min_event_time_utc = chunk_min

                if max_event_time_utc is None or chunk_max > max_event_time_utc:
                    max_event_time_utc = chunk_max

            print(f"Parsed {chunk_rows_seen} rows for chunk {chunk_start_utc} -> {chunk_end_utc}")

            chunk_inserted = insert_load_rows(chunk_rows, args.target_table)
            rows_inserted += chunk_inserted

            print(f"Inserted {chunk_inserted} rows for chunk {chunk_start_utc} -> {chunk_end_utc}")

        resolution_detected = ",".join(sorted(resolutions_seen)) if resolutions_seen else None

        if max_event_time_utc is not None:
            upsert_pipeline_state(
                pipeline_name=PIPELINE_NAME,
                country_code=args.country_code,
                bidding_zone=args.bidding_zone,
                last_ingested_at_utc=max_event_time_utc,
            )
            print(f"Updated pipeline_state to {max_event_time_utc}")

        update_pipeline_run_success(
            conn=audit_conn,
            run_id=run_id,
            finished_at_utc=datetime.now(timezone.utc),
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            min_event_time_utc=min_event_time_utc,
            max_event_time_utc=max_event_time_utc,
            resolution_detected=resolution_detected,
            source_timeseries_count=source_timeseries_count,
        )

        print(f"Run complete. Total parsed rows: {rows_seen}")
        print(f"Run complete. Total inserted rows: {rows_inserted}")

    except Exception as e:
        resolution_detected = ",".join(sorted(resolutions_seen)) if resolutions_seen else None

        update_pipeline_run_failed(
            conn=audit_conn,
            run_id=run_id,
            finished_at_utc=datetime.now(timezone.utc),
            rows_seen=rows_seen,
            rows_inserted=rows_inserted,
            min_event_time_utc=min_event_time_utc,
            max_event_time_utc=max_event_time_utc,
            resolution_detected=resolution_detected,
            source_timeseries_count=source_timeseries_count,
            error_message=str(e),
        )
        raise
    finally:
        audit_conn.close()


if __name__ == "__main__":
    main()
