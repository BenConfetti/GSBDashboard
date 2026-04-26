from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


TABLES_TO_COPY = [
    "draft_event",
    "gameweek",
    "matchup",
    "matchup_stat_result",
    "player",
    "player_gameweek",
    "player_gameweek_stat",
    "player_season",
    "season",
    "trade_event",
    "transaction_event",
]

VIEWS_TO_COPY = [
    "v_all_time_team_standings",
    "v_all_time_team_standings_regular_season",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a smaller deployment SQLite database for the Streamlit app.")
    parser.add_argument(
        "--source-db",
        default=str(Path("database") / "fantrax_v2.db"),
        help="Path to the full source SQLite database.",
    )
    parser.add_argument(
        "--target-db",
        default=str(Path("database") / "fantrax_deploy.db"),
        help="Path to the smaller deployment SQLite database to create.",
    )
    return parser.parse_args()


def fetch_create_sql(connection: sqlite3.Connection, object_type: str, name: str) -> str:
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = ? AND name = ?",
        (object_type, name),
    ).fetchone()
    if row is None or not row[0]:
        raise RuntimeError(f"Could not find CREATE SQL for {object_type} {name}")
    return row[0]


def object_exists(connection: sqlite3.Connection, object_type: str, name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?",
        (object_type, name),
    ).fetchone()
    return row is not None


def copy_table_schema(source: sqlite3.Connection, target: sqlite3.Connection, table_name: str) -> None:
    create_sql = fetch_create_sql(source, "table", table_name)
    target.execute(create_sql)


def copy_table_rows(source: sqlite3.Connection, target: sqlite3.Connection, table_name: str) -> None:
    rows = source.execute(f'SELECT * FROM "{table_name}"')
    column_names = [description[0] for description in rows.description]
    placeholders = ", ".join(["?"] * len(column_names))
    quoted_columns = ", ".join(f'"{name}"' for name in column_names)
    insert_sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'
    target.executemany(insert_sql, rows)


def copy_view_schema(source: sqlite3.Connection, target: sqlite3.Connection, view_name: str) -> None:
    create_sql = fetch_create_sql(source, "view", view_name)
    target.execute(create_sql)


def build_deploy_db(source_path: Path, target_path: Path) -> None:
    if not source_path.exists():
        raise FileNotFoundError(f"Source database not found: {source_path}")

    if target_path.exists():
        target_path.unlink()

    source = sqlite3.connect(source_path)
    target = sqlite3.connect(target_path)

    try:
        target.execute("PRAGMA journal_mode = OFF")
        target.execute("PRAGMA synchronous = OFF")
        target.execute("PRAGMA temp_store = MEMORY")

        for table_name in TABLES_TO_COPY:
            if not object_exists(source, "table", table_name):
                raise RuntimeError(f"Required table missing in source DB: {table_name}")
            copy_table_schema(source, target, table_name)

        for table_name in TABLES_TO_COPY:
            copy_table_rows(source, target, table_name)
            target.commit()

        for view_name in VIEWS_TO_COPY:
            if object_exists(source, "view", view_name):
                copy_view_schema(source, target, view_name)

        target.execute("VACUUM")
        target.commit()
    finally:
        source.close()
        target.close()


def main() -> None:
    args = parse_args()
    source_path = Path(args.source_db)
    target_path = Path(args.target_db)
    build_deploy_db(source_path, target_path)
    size_mb = target_path.stat().st_size / (1024 * 1024)
    print(f"Built {target_path} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    main()
