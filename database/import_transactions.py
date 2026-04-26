from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path


TRANSACTION_FILE_RE = re.compile(r"Transactions(\d{4})\.csv$", re.IGNORECASE)
DATE_RE = re.compile(
    r"^(?P<weekday>[A-Za-z]{3}) (?P<month>[A-Za-z]{3}) (?P<day>\d{1,2}), "
    r"(?P<year>\d{4}), (?P<hour>\d{1,2}):(?P<minute>\d{2})(?P<ampm>AM|PM)$"
)
MONTHS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
TEAM_NAME_ALIASES = {
    "Athletic de Bapao": "Athletic Club Bapao",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Fantrax transaction history CSV files into SQLite."
    )
    parser.add_argument(
        "--input-dir",
        default="downloaddata",
        help="Folder containing Transactions*.csv files",
    )
    parser.add_argument(
        "--db-path",
        default="database/fantrax_v2.db",
        help="Path to the SQLite database file",
    )
    parser.add_argument(
        "--schema-path",
        default="database/schema.sql",
        help="Path to the SQLite schema file",
    )
    return parser.parse_args()


def parse_optional_int(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = value.strip().replace(",", "")
    if cleaned in {"", "-"}:
        return None
    return int(float(cleaned))


def parse_optional_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip().replace(",", "")
    if cleaned in {"", "-"}:
        return None
    return float(cleaned)


def parse_transaction_datetime(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None

    match = DATE_RE.match(cleaned)
    if not match:
        return None

    month = MONTHS[match.group("month")]
    day = int(match.group("day"))
    year = int(match.group("year"))
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    ampm = match.group("ampm")

    if ampm == "AM" and hour == 12:
        hour = 0
    elif ampm == "PM" and hour != 12:
        hour += 12

    return datetime(year, month, day, hour, minute).isoformat(timespec="minutes")


def transaction_files(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("Transactions*.csv"))


def season_slug_from_filename(path: Path) -> str:
    match = TRANSACTION_FILE_RE.match(path.name)
    if not match:
        raise ValueError(f"Could not derive season from filename: {path.name}")

    token = match.group(1)
    start_short = int(token[:2])
    end_short = int(token[2:])
    start_year = 2000 + start_short
    end_year = 2000 + end_short
    return f"{start_year}-{end_year}"


def load_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    connection.executescript(schema_path.read_text(encoding="utf-8"))


def ensure_season(connection: sqlite3.Connection, season_slug: str) -> None:
    start_year, end_year = season_slug.split("-")
    connection.execute(
        """
        INSERT INTO season (season_slug, start_year, end_year)
        VALUES (?, ?, ?)
        ON CONFLICT (season_slug) DO UPDATE SET
            start_year = excluded.start_year,
            end_year = excluded.end_year
        """,
        (season_slug, int(start_year), int(end_year)),
    )


def ensure_gameweek(connection: sqlite3.Connection, season_slug: str, gameweek: int) -> None:
    connection.execute(
        """
        INSERT INTO gameweek (season_slug, gameweek, is_playoff)
        VALUES (?, ?, 0)
        ON CONFLICT (season_slug, gameweek) DO NOTHING
        """,
        (season_slug, gameweek),
    )


def row_to_dict(row: list[str]) -> dict[str, str]:
    if len(row) < 9:
        raise ValueError(f"Unexpected transaction row length {len(row)}: {row!r}")

    return {
        "player_name": row[0].strip(),
        "premier_league_team_code": row[1].strip(),
        "position": row[2].strip(),
        "transaction_type": row[3].strip(),
        "fantrax_team_name": TEAM_NAME_ALIASES.get(row[4].strip(), row[4].strip()),
        "bid_amount": row[5].strip(),
        "priority_value": row[6].strip(),
        "transaction_datetime_text": row[7].strip(),
        "gameweek": row[8].strip(),
    }


def import_transaction_file(
    connection: sqlite3.Connection,
    season_slug: str,
    csv_path: Path,
) -> int:
    ensure_season(connection, season_slug)
    imported_rows = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"CSV has no header: {csv_path}")

        for row in reader:
            if not row or not any(cell.strip() for cell in row):
                continue

            parsed = row_to_dict(row)
            player_name = parsed["player_name"]
            team_code = parsed["premier_league_team_code"] or None
            gameweek = parse_optional_int(parsed["gameweek"])
            if gameweek is None:
                raise ValueError(f"Missing gameweek in row: {row!r}")

            ensure_gameweek(connection, season_slug, gameweek)

            if team_code:
                connection.execute(
                    """
                    INSERT INTO premier_league_team (team_code)
                    VALUES (?)
                    ON CONFLICT (team_code) DO NOTHING
                    """,
                    (team_code,),
                )

            connection.execute(
                """
                INSERT INTO transaction_event (
                    season_slug,
                    gameweek,
                    player_name,
                    premier_league_team_code,
                    position,
                    transaction_type,
                    fantrax_team_name,
                    bid_amount,
                    priority_value,
                    transaction_datetime_text,
                    transaction_datetime_iso,
                    raw_row_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (
                    season_slug,
                    gameweek,
                    player_name,
                    transaction_type,
                    fantrax_team_name,
                    transaction_datetime_text
                ) DO UPDATE SET
                    premier_league_team_code = excluded.premier_league_team_code,
                    position = excluded.position,
                    bid_amount = excluded.bid_amount,
                    priority_value = excluded.priority_value,
                    transaction_datetime_iso = excluded.transaction_datetime_iso,
                    raw_row_json = excluded.raw_row_json
                """,
                (
                    season_slug,
                    gameweek,
                    player_name,
                    team_code,
                    parsed["position"] or None,
                    parsed["transaction_type"],
                    parsed["fantrax_team_name"] or None,
                    parse_optional_float(parsed["bid_amount"]),
                    parse_optional_int(parsed["priority_value"]),
                    parsed["transaction_datetime_text"] or None,
                    parse_transaction_datetime(parsed["transaction_datetime_text"]),
                    json.dumps(parsed, ensure_ascii=False),
                ),
            )
            imported_rows += 1

    return imported_rows


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    db_path = Path(args.db_path)
    schema_path = Path(args.schema_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    csv_paths = transaction_files(input_dir)
    if not csv_paths:
        raise FileNotFoundError(f"No Transactions*.csv files found in {input_dir}")

    imported_files = 0
    imported_rows = 0
    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON;")
        load_schema(connection, schema_path)
        for csv_path in csv_paths:
            season_slug = season_slug_from_filename(csv_path)
            imported_rows += import_transaction_file(connection, season_slug, csv_path)
            imported_files += 1
        connection.commit()

    print(
        f"Imported {imported_rows} transaction rows from {imported_files} files into {db_path}"
    )


if __name__ == "__main__":
    main()
