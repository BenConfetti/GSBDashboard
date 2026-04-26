from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from pathlib import Path
from typing import Iterable


FIXED_COLUMNS = {
    "ID",
    "Player",
    "Team",
    "Position",
    "RkOv",
    "Status",
    "Opponent",
    "Score",
    "Ros",
    "+/-",
    "%D",
    "ADP",
    "GP",
}

GAMEWEEK_FILE_RE = re.compile(r"playerdata_gw(\d+)\.csv$", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Fantrax player gameweek CSV files into SQLite."
    )
    parser.add_argument(
        "--input-root",
        default="downloaddata/player_gameweeks",
        help="Root folder containing season subfolders with playerdata_gwXX.csv files",
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
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1]
    return float(cleaned)


def season_folders(input_root: Path) -> list[Path]:
    return sorted(path for path in input_root.iterdir() if path.is_dir())


def gameweek_from_filename(path: Path) -> int:
    match = GAMEWEEK_FILE_RE.search(path.name)
    if not match:
        raise ValueError(f"Could not derive gameweek from filename: {path.name}")
    return int(match.group(1))


def csv_files(season_dir: Path) -> list[Path]:
    return sorted(season_dir.glob("playerdata_gw*.csv"))


def stat_columns(fieldnames: Iterable[str]) -> list[str]:
    return [name for name in fieldnames if name not in FIXED_COLUMNS]


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


def import_gameweek_csv(connection: sqlite3.Connection, season_slug: str, csv_path: Path) -> None:
    gameweek = gameweek_from_filename(csv_path)
    ensure_season(connection, season_slug)
    ensure_gameweek(connection, season_slug, gameweek)

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {csv_path}")

        stats = stat_columns(reader.fieldnames)
        for stat_key in stats:
            connection.execute(
                """
                INSERT INTO stat_category (stat_key, first_seen_season_slug)
                VALUES (?, ?)
                ON CONFLICT (stat_key) DO NOTHING
                """,
                (stat_key, season_slug),
            )

        for row in reader:
            player_id = row["ID"].strip()
            player_name = row["Player"].strip()
            team_code = row["Team"].strip() or None

            connection.execute(
                """
                INSERT INTO player (player_id, player_name)
                VALUES (?, ?)
                ON CONFLICT (player_id) DO UPDATE SET
                    player_name = excluded.player_name
                """,
                (player_id, player_name),
            )

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
                INSERT INTO player_gameweek (
                    player_id,
                    season_slug,
                    gameweek,
                    premier_league_team_code,
                    position,
                    rank_overall,
                    score,
                    games_played,
                    status_snapshot,
                    opponent_snapshot,
                    ros,
                    plus_minus,
                    raw_row_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (player_id, season_slug, gameweek) DO UPDATE SET
                    premier_league_team_code = excluded.premier_league_team_code,
                    position = excluded.position,
                    rank_overall = excluded.rank_overall,
                    score = excluded.score,
                    games_played = excluded.games_played,
                    status_snapshot = excluded.status_snapshot,
                    opponent_snapshot = excluded.opponent_snapshot,
                    ros = excluded.ros,
                    plus_minus = excluded.plus_minus,
                    raw_row_json = excluded.raw_row_json
                """,
                (
                    player_id,
                    season_slug,
                    gameweek,
                    team_code,
                    row.get("Position", "").strip() or None,
                    parse_optional_int(row.get("RkOv")),
                    parse_optional_float(row.get("Score")),
                    parse_optional_int(row.get("GP")),
                    row.get("Status", "").strip() or None,
                    row.get("Opponent", "").strip() or None,
                    row.get("Ros", "").strip() or None,
                    row.get("+/-", "").strip() or None,
                    json.dumps(row, ensure_ascii=False),
                ),
            )

            for stat_key in stats:
                raw_value = (row.get(stat_key) or "").strip()
                connection.execute(
                    """
                    INSERT INTO player_gameweek_stat (
                        player_id,
                        season_slug,
                        gameweek,
                        stat_key,
                        stat_value,
                        raw_value
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (player_id, season_slug, gameweek, stat_key) DO UPDATE SET
                        stat_value = excluded.stat_value,
                        raw_value = excluded.raw_value
                    """,
                    (
                        player_id,
                        season_slug,
                        gameweek,
                        stat_key,
                        parse_optional_float(raw_value),
                        raw_value,
                    ),
                )


def main() -> None:
    args = parse_args()
    input_root = Path(args.input_root)
    db_path = Path(args.db_path)
    schema_path = Path(args.schema_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    seasons = season_folders(input_root)
    if not seasons:
        raise FileNotFoundError(f"No season folders found in {input_root}")

    imported_files = 0
    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON;")
        load_schema(connection, schema_path)
        for season_dir in seasons:
            season_slug = season_dir.name
            for csv_path in csv_files(season_dir):
                import_gameweek_csv(connection, season_slug, csv_path)
                imported_files += 1
        connection.commit()

    print(f"Imported {imported_files} player gameweek CSV files into {db_path}")


if __name__ == "__main__":
    main()
