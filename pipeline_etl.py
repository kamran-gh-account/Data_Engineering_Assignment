from __future__ import annotations
import argparse
import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
import requests
import urllib3
from zoneinfo import ZoneInfo


SUN_API_URL = "https://api.sunrise-sunset.org/json"
IST_TZ = ZoneInfo("Asia/Kolkata")
STREAM_NAME = "sunrise_sunset"


def parse_args() -> argparse.Namespace:
    """Parse command-line options for date range, location, and output paths."""
    parser = argparse.ArgumentParser(description="Extract sunrise/sunset data and load into SQLite")
    parser.add_argument("--start-date", default="2020-01-01", help="Start date in YYYY-MM-DD")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="End date in YYYY-MM-DD")
    parser.add_argument(
        "--location",
        default="19.0760,72.8777",
        help="Location as 'lat,lng' (example: '19.0760,72.8777')",
    )
    parser.add_argument("--db-path", default="data/sunrise_sunset.db", help="SQLite database path")
    parser.add_argument(
        "--messages-path",
        default="data/singer_messages.jsonl",
        help="Singer-format messages output path",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL certificate verification (use only in local/dev environments)",
    )
    return parser.parse_args()


def parse_location(location: str) -> tuple[float, float]:
    """Split location text into latitude and longitude floats."""
    try:
        lat_str, lng_str = [part.strip() for part in location.split(",", 1)]
        return float(lat_str), float(lng_str)
    except Exception as exc:
        raise ValueError("--location must be in 'lat,lng' format") from exc


def date_range(start: date, end: date) -> Iterable[date]:
    """Yield each date from start to end (inclusive)."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def parse_utc_timestamp(ts: str) -> datetime:
    """Convert API UTC timestamp text into a timezone-aware UTC datetime."""
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def fetch_one_day(day: date, latitude: float, longitude: float, verify_ssl: bool = True) -> dict:
    """Fetch one day's sunrise/sunset data and normalize it with IST conversions."""
    response = requests.get(
        SUN_API_URL,
        params={
            "lat": latitude,
            "lng": longitude,
            "date": day.isoformat(),
            "formatted": 0,
        },
        timeout=20,
        verify=verify_ssl,
    )
    response.raise_for_status()
    data = response.json()

    if data.get("status") != "OK":
        raise RuntimeError(f"Sunrise-Sunset API returned non-OK status for {day}: {data}")

    results = data["results"]
    sunrise_utc = parse_utc_timestamp(results["sunrise"])
    sunset_utc = parse_utc_timestamp(results["sunset"])
    sunrise_ist = sunrise_utc.astimezone(IST_TZ)
    sunset_ist = sunset_utc.astimezone(IST_TZ)

    return {
        "date": day.isoformat(),
        "latitude": latitude,
        "longitude": longitude,
        "sunrise_utc": sunrise_utc.isoformat(),
        "sunset_utc": sunset_utc.isoformat(),
        "sunrise_ist": sunrise_ist.isoformat(),
        "sunset_ist": sunset_ist.isoformat(),
        "day_length_seconds": int((sunset_utc - sunrise_utc).total_seconds()),
        "fetched_at_ist": datetime.now(IST_TZ).isoformat(),
    }


def write_singer_messages(records: list[dict], output_path: Path) -> None:
    """Write SCHEMA and RECORD Singer messages to a JSONL file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    schema_message = {
        "type": "SCHEMA",
        "stream": STREAM_NAME,
        "schema": {
            "type": "object",
            "properties": {
                "date": {"type": "string"},
                "latitude": {"type": "number"},
                "longitude": {"type": "number"},
                "sunrise_utc": {"type": "string"},
                "sunset_utc": {"type": "string"},
                "sunrise_ist": {"type": "string"},
                "sunset_ist": {"type": "string"},
                "day_length_seconds": {"type": "integer"},
                "fetched_at_ist": {"type": "string"},
            },
        },
        "key_properties": ["date"],
    }

    with output_path.open("w", encoding="utf-8") as fp:
        fp.write(json.dumps(schema_message) + "\n")
        for row in records:
            fp.write(
                json.dumps(
                    {
                        "type": "RECORD",
                        "stream": STREAM_NAME,
                        "record": row,
                    }
                )
                + "\n"
            )


def load_messages_to_sqlite(messages_path: Path, db_path: Path) -> int:
    """Read Singer RECORD messages and upsert them into the SQLite table."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sunrise_sunset (
            date TEXT PRIMARY KEY,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            sunrise_utc TEXT NOT NULL,
            sunset_utc TEXT NOT NULL,
            sunrise_ist TEXT NOT NULL,
            sunset_ist TEXT NOT NULL,
            day_length_seconds INTEGER NOT NULL,
            fetched_at_ist TEXT NOT NULL
        )
        """
    )

    upsert_sql = """
        INSERT INTO sunrise_sunset (
            date, latitude, longitude, sunrise_utc, sunset_utc,
            sunrise_ist, sunset_ist, day_length_seconds, fetched_at_ist
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            latitude=excluded.latitude,
            longitude=excluded.longitude,
            sunrise_utc=excluded.sunrise_utc,
            sunset_utc=excluded.sunset_utc,
            sunrise_ist=excluded.sunrise_ist,
            sunset_ist=excluded.sunset_ist,
            day_length_seconds=excluded.day_length_seconds,
            fetched_at_ist=excluded.fetched_at_ist
    """

    inserted = 0
    with messages_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            message = json.loads(line)
            if message.get("type") != "RECORD":
                continue
            record = message["record"]
            conn.execute(
                upsert_sql,
                (
                    record["date"],
                    record["latitude"],
                    record["longitude"],
                    record["sunrise_utc"],
                    record["sunset_utc"],
                    record["sunrise_ist"],
                    record["sunset_ist"],
                    record["day_length_seconds"],
                    record["fetched_at_ist"],
                ),
            )
            inserted += 1

    conn.commit()
    conn.close()
    return inserted


def main() -> None:
    """Run the full ETL flow: parse args, extract, serialize Singer messages, and load SQLite."""
    args = parse_args()

    if args.insecure:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    start = datetime.fromisoformat(args.start_date).date()
    end = datetime.fromisoformat(args.end_date).date()
    if start > end:
        raise ValueError("--start-date must be before or equal to --end-date")

    latitude, longitude = parse_location(args.location)
    print(f"Using location: {latitude}, {longitude}")

    total_days = (end - start).days + 1
    records: list[dict] = []
    for index, day in enumerate(date_range(start, end), start=1):
        records.append(fetch_one_day(day, latitude, longitude, verify_ssl=not args.insecure))
        if index % 100 == 0 or index == total_days:
            print(f"Progress: {index}/{total_days} days fetched")

    messages_path = Path(args.messages_path)
    db_path = Path(args.db_path)

    write_singer_messages(records, messages_path)
    loaded_count = load_messages_to_sqlite(messages_path, db_path)

    print(f"Fetched days: {len(records)}")
    print(f"Singer messages written: {messages_path}")
    print(f"Rows loaded to SQLite: {loaded_count}")
    print(f"SQLite DB: {db_path}")


if __name__ == "__main__":
    main()
