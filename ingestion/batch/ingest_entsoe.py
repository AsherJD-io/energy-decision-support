import argparse
import os
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

import requests
from db import insert_load_rows

ENTSOE_BASE_URL = "https://web-api.tp.entsoe.eu/api"

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


def fetch_entsoe_xml(token, bidding_zone, start_date, end_date):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

    params = {
        "securityToken": token,
        "documentType": "A65",
        "processType": "A16",
        "outBiddingZone_Domain": bidding_zone,
        "periodStart": start_dt.strftime("%Y%m%d%H%M"),
        "periodEnd": end_dt.strftime("%Y%m%d%H%M"),
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

    timeseries = root.findall(".//ns:TimeSeries", NS)

    for ts in timeseries:
        period = ts.find(".//ns:Period", NS)

        start_text = period.findtext("ns:timeInterval/ns:start", namespaces=NS)
        resolution = period.findtext("ns:resolution", namespaces=NS)

        start_dt = datetime.strptime(start_text, "%Y-%m-%dT%H:%MZ")
        delta = parse_resolution(resolution)

        points = period.findall("ns:Point", NS)

        for p in points:
            position = int(p.findtext("ns:position", namespaces=NS))
            quantity = float(p.findtext("ns:quantity", namespaces=NS))

            ts_time = start_dt + (position - 1) * delta

            rows.append(
                (
                    ts_time,
                    country_code,
                    bidding_zone,
                    quantity,
                )
            )

    return rows


def main():
    args = parse_args()
    token = validate_env()

    print("Fetching ENTSO-E data...")

    xml = fetch_entsoe_xml(
        token,
        args.bidding_zone,
        args.start_date,
        args.end_date,
    )

    print("Parsing XML...")

    rows = parse_xml(
        xml,
        args.country_code,
        args.bidding_zone,
    )

    print(f"Parsed {len(rows)} rows")

    inserted = insert_load_rows(rows, args.target_table)

    print(f"Inserted {inserted} rows")


if __name__ == "__main__":
    main()
