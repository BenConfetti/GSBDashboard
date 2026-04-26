from __future__ import annotations

import argparse
import csv
import json
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Fantrax player season CSV files into SQLite."
    )
    parser.add_argument(
        "--input-dir",
        default="downloaddata",
        help="Directory containing players*.csv files",
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


def season_slug_from_filename(path: Path) -> str:
    suffix = path.stem.replace("players", "")
    if not suffix.isdigit():
        raise ValueError(f"Could not derive season from filename: {path.name}")

    if len(suffix) == 4:
        start_suffix = suffix[:2]
        end_suffix = suffix[2:]
    elif len(suffix) == 3:
        start_suffix = suffix[:2]
        end_suffix = suffix[1:]
    else:
        raise ValueError(f"Could not derive season from filename: {path.name}")

    start_year = 2000 + int(start_suffix)
    end_year = 2000 + int(end_suffix)
    return f"{start_year}-{end_year}"


def season_years(season_slug: str) -> tuple[int, int]:
    start_year, end_year = season_slug.split("-")
    return int(start_year), int(end_year)


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


def csv_files(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("players*.csv"))


def stat_columns(fieldnames: Iterable[str]) -> list[str]:
    return [name for name in fieldnames if name not in FIXED_COLUMNS]


def load_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    connection.executescript(schema_path.read_text(encoding="utf-8"))


def import_csv(connection: sqlite3.Connection, csv_path: Path) -> None:
    season_slug = season_slug_from_filename(csv_path)
    start_year, end_year = season_years(season_slug)

    connection.execute(
        """
        INSERT INTO season (season_slug, start_year, end_year)
        VALUES (?, ?, ?)
        ON CONFLICT (season_slug) DO UPDATE SET
            start_year = excluded.start_year,
            end_year = excluded.end_year
        """,
        (season_slug, start_year, end_year),
    )

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
                INSERT INTO player_season (
                    player_id,
                    season_slug,
                    premier_league_team_code,
                    position,
                    rank_overall,
                    score,
                    games_played,
                    adp,
                    raw_row_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (player_id, season_slug) DO UPDATE SET
                    premier_league_team_code = excluded.premier_league_team_code,
                    position = excluded.position,
                    rank_overall = excluded.rank_overall,
                    score = excluded.score,
                    games_played = excluded.games_played,
                    adp = excluded.adp,
                    raw_row_json = excluded.raw_row_json
                """,
                (
                    player_id,
                    season_slug,
                    team_code,
                    row.get("Position", "").strip() or None,
                    parse_optional_int(row.get("RkOv")),
                    parse_optional_float(row.get("Score")),
                    parse_optional_int(row.get("GP")),
                    parse_optional_float(row.get("ADP")),
                    json.dumps(row, ensure_ascii=False),
                ),
            )

            for stat_key in stats:
                raw_value = (row.get(stat_key) or "").strip()
                connection.execute(
                    """
                    INSERT INTO player_season_stat (
                        player_id,
                        season_slug,
                        stat_key,
                        stat_value,
                        raw_value
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (player_id, season_slug, stat_key) DO UPDATE SET
                        stat_value = excluded.stat_value,
                        raw_value = excluded.raw_value
                    """,
                    (
                        player_id,
                        season_slug,
                        stat_key,
                        parse_optional_float(raw_value),
                        raw_value,
                    ),
                )


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    db_path = Path(args.db_path)
    schema_path = Path(args.schema_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    files = csv_files(input_dir)
    if not files:
        raise FileNotFoundError(f"No players*.csv files found in {input_dir}")

    with sqlite3.connect(db_path) as connection:
        load_schema(connection, schema_path)
        for csv_path in files:
            import_csv(connection, csv_path)
        connection.commit()

    print(f"Imported {len(files)} CSV files into {db_path}")


if __name__ == "__main__":
    main()
