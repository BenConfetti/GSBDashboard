from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path


LOWER_IS_BETTER_STATS = {"DPt2"}
GAMEWEEK_RE = re.compile(r"^Gameweek\s+(\d+)\s*$")
PLAYOFF_RE = re.compile(r"^Gameweek:\s*Playoffs\s+(\d+)\s*$", re.IGNORECASE)
PLAYOFF_ROUND_RE = re.compile(r"^Playoffs\s*-\s*Round\s+(\d+)\s*$", re.IGNORECASE)
DATE_RANGE_RE = re.compile(r"^\(.+\)$")
COMMISSIONER_EDIT_RE = re.compile(r"\s*\([+-]?\d+(?:\.\d+)?\)")
TEAM_NAME_ALIASES = {
    "Athletic de Bapao": "Athletic Club Bapao",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import pasted Fantrax matchup text into SQLite."
    )
    parser.add_argument(
        "--input-file",
        required=True,
        help="Text file with pasted matchup blocks",
    )
    parser.add_argument(
        "--season-slug",
        required=True,
        help="Season slug, for example 2025-2026",
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


def load_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    connection.executescript(schema_path.read_text(encoding="utf-8"))


def parse_optional_float(value: str) -> float | None:
    cleaned = strip_commissioner_edits(value).strip().replace(",", "")
    if cleaned in {"", "-"}:
        return None
    return float(cleaned)


def strip_commissioner_edits(value: str) -> str:
    return COMMISSIONER_EDIT_RE.sub("", value)


def normalize_team_name(value: str) -> str:
    cleaned = value.strip()
    return TEAM_NAME_ALIASES.get(cleaned, cleaned)


def stat_points_for_win(connection: sqlite3.Connection, stat_key: str) -> float:
    row = connection.execute(
        "SELECT points_for_win FROM stat_weight WHERE stat_key = ?",
        (stat_key,),
    ).fetchone()
    if row is None:
        return 1.0
    return float(row[0])


def ensure_stat_category(connection: sqlite3.Connection, season_slug: str, stat_key: str) -> None:
    connection.execute(
        """
        INSERT INTO stat_category (stat_key, first_seen_season_slug)
        VALUES (?, ?)
        ON CONFLICT (stat_key) DO NOTHING
        """,
        (stat_key, season_slug),
    )


def is_block_start(line: str) -> bool:
    return bool(
        GAMEWEEK_RE.match(line)
        or PLAYOFF_RE.match(line)
        or PLAYOFF_ROUND_RE.match(line)
        or DATE_RANGE_RE.match(line)
    )


def split_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current: list[str] = []

    for line in lines:
        starts_new_block = False
        if current:
            if GAMEWEEK_RE.match(line) or PLAYOFF_RE.match(line) or PLAYOFF_ROUND_RE.match(line):
                starts_new_block = True
            elif DATE_RANGE_RE.match(line):
                previous_is_header_only = len(current) == 1 and (
                    GAMEWEEK_RE.match(current[0])
                    or PLAYOFF_RE.match(current[0])
                    or PLAYOFF_ROUND_RE.match(current[0])
                )
                starts_new_block = not previous_is_header_only

        if starts_new_block:
            blocks.append(current)
            current = [line]
        else:
            current.append(line)

    if current:
        blocks.append(current)

    return [
        block
        for block in blocks
        if block and is_block_start(block[0])
    ]


def is_numeric_row(line: str) -> bool:
    if not line:
        return False
    first = line[0]
    return first.isdigit() or first == "-"


def is_dash_separator_row(line: str) -> bool:
    tokens = re.split(r"\s+", line.strip())
    return bool(tokens) and all(token == "-" for token in tokens)


def parse_table_header(line: str) -> list[str]:
    columns = re.split(r"\s+", line.strip())
    if not columns or columns[0] != "Team":
        raise ValueError(f"Unexpected table header: {line}")
    return columns


def parse_numeric_row(line: str, expected_count: int) -> list[str]:
    normalized_line = strip_commissioner_edits(line)
    values = re.split(r"\s+", normalized_line.strip())
    if is_dash_separator_row(line):
        return ["-"] * expected_count
    if len(values) != expected_count:
        raise ValueError(
            f"Expected {expected_count} values in row, got {len(values)}: {line}"
        )
    return values


def winner_side(stat_key: str, team_a_value: float | None, team_b_value: float | None) -> str:
    if team_a_value is None and team_b_value is None:
        return "TIE"
    if team_a_value is None:
        return "B"
    if team_b_value is None:
        return "A"

    if stat_key in LOWER_IS_BETTER_STATS:
        if team_a_value < team_b_value:
            return "A"
        if team_b_value < team_a_value:
            return "B"
        return "TIE"

    if team_a_value > team_b_value:
        return "A"
    if team_b_value > team_a_value:
        return "B"
    return "TIE"


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


def max_regular_gameweek(connection: sqlite3.Connection, season_slug: str) -> int:
    row = connection.execute(
        """
        SELECT COALESCE(MAX(gameweek), 0)
        FROM gameweek
        WHERE season_slug = ? AND is_playoff = 0
        """,
        (season_slug,),
    ).fetchone()
    if row is None:
        return 0
    return int(row[0])


def playoff_gameweek_number(
    connection: sqlite3.Connection,
    season_slug: str,
    round_number: int,
    playoff_round_gameweeks: dict[int, int],
) -> int:
    if round_number not in playoff_round_gameweeks:
        playoff_round_gameweeks[round_number] = (
            max_regular_gameweek(connection, season_slug) + round_number
        )
    return playoff_round_gameweeks[round_number]


def clear_gameweek(connection: sqlite3.Connection, season_slug: str, gameweek: int) -> None:
    connection.execute(
        """
        DELETE FROM matchup
        WHERE season_slug = ? AND gameweek = ?
        """,
        (season_slug, gameweek),
    )
    connection.execute(
        """
        DELETE FROM gameweek
        WHERE season_slug = ? AND gameweek = ?
        """,
        (season_slug, gameweek),
    )


def import_block(
    connection: sqlite3.Connection,
    season_slug: str,
    block: list[str],
    previous_gameweek: int | None,
    previous_is_playoff: int,
    known_date_ranges: dict[str, tuple[int, int, str | None]],
    playoff_round_gameweeks: dict[int, int],
    cleared_gameweeks: set[tuple[str, int]],
) -> tuple[int, int]:
    regular_match = GAMEWEEK_RE.match(block[0])
    playoff_match = PLAYOFF_RE.match(block[0])
    playoff_round_match = PLAYOFF_ROUND_RE.match(block[0])
    if playoff_match:
        round_number = int(playoff_match.group(1))
        gameweek = playoff_gameweek_number(
            connection,
            season_slug,
            round_number,
            playoff_round_gameweeks,
        )
        is_playoff = 1
        stage_label = f"Playoffs {round_number}"
        date_range_label = block[1].strip("()")
        header = parse_table_header(block[2])
        data_lines = block[3:]
    elif playoff_round_match:
        round_number = int(playoff_round_match.group(1))
        gameweek = playoff_gameweek_number(
            connection,
            season_slug,
            round_number,
            playoff_round_gameweeks,
        )
        is_playoff = 1
        stage_label = f"Playoffs {round_number}"
        date_range_label = block[1].strip("()")
        header = parse_table_header(block[2])
        data_lines = block[3:]
    elif regular_match:
        gameweek = int(regular_match.group(1))
        is_playoff = 0
        stage_label = None
        date_range_label = block[1].strip("()")
        header = parse_table_header(block[2])
        data_lines = block[3:]
    elif DATE_RANGE_RE.match(block[0]):
        date_range_label = block[0].strip("()")
        known = known_date_ranges.get(date_range_label)
        if known is None:
            raise ValueError(
                "Cannot infer gameweek number for block starting with date line. "
                f"No known mapping found for: {block[0]}"
            )
        gameweek, is_playoff, stage_label = known
        header = parse_table_header(block[1])
        data_lines = block[2:]
    else:
        raise ValueError(f"Block does not start with recognizable header: {block[0]}")

    value_columns = header[1:]
    stat_columns = value_columns[4:]

    for stat_key in stat_columns:
        ensure_stat_category(connection, season_slug, stat_key)

    names: list[str] = []
    values: list[list[str]] = []

    for line in data_lines:
        stripped = line.strip()
        if not stripped:
            continue
        if is_numeric_row(stripped):
            values.append(parse_numeric_row(stripped, len(value_columns)))
        else:
            names.append(normalize_team_name(stripped))

    if len(names) != len(values):
        raise ValueError(
            f"Gameweek {gameweek}: team rows ({len(names)}) do not match value rows ({len(values)})."
        )
    if len(names) % 2 != 0:
        raise ValueError(f"Gameweek {gameweek}: expected an even number of team rows.")

    gameweek_key = (season_slug, gameweek)
    if gameweek_key not in cleared_gameweeks:
        clear_gameweek(connection, season_slug, gameweek)
        cleared_gameweeks.add(gameweek_key)

    connection.execute(
        """
        INSERT INTO gameweek (season_slug, gameweek, date_range_label, is_playoff, stage_label)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (season_slug, gameweek) DO UPDATE SET
            date_range_label = COALESCE(gameweek.date_range_label, excluded.date_range_label),
            is_playoff = excluded.is_playoff,
            stage_label = COALESCE(gameweek.stage_label, excluded.stage_label)
        """,
        (season_slug, gameweek, date_range_label, is_playoff, stage_label),
    )
    known_date_ranges[date_range_label] = (gameweek, is_playoff, stage_label)

    for index in range(0, len(names), 2):
        team_a_name = names[index]
        team_b_name = names[index + 1]
        row_a = values[index]
        row_b = values[index + 1]

        cursor = connection.execute(
            """
            INSERT INTO matchup (
                season_slug,
                gameweek,
                is_playoff,
                team_a_name,
                team_b_name,
                team_a_points,
                team_b_points,
                team_a_wins,
                team_a_losses,
                team_a_ties,
                team_b_wins,
                team_b_losses,
                team_b_ties,
                source_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                season_slug,
                gameweek,
                is_playoff,
                team_a_name,
                team_b_name,
                parse_optional_float(row_a[3]) or 0.0,
                parse_optional_float(row_b[3]) or 0.0,
                parse_optional_float(row_a[0]),
                parse_optional_float(row_a[1]),
                parse_optional_float(row_a[2]),
                parse_optional_float(row_b[0]),
                parse_optional_float(row_b[1]),
                parse_optional_float(row_b[2]),
                f"{team_a_name}\n{team_b_name}\n{' '.join(row_a)}\n{' '.join(row_b)}",
            ),
        )
        matchup_id = int(cursor.lastrowid)

        for stat_index, stat_key in enumerate(stat_columns, start=4):
            team_a_value = parse_optional_float(row_a[stat_index])
            team_b_value = parse_optional_float(row_b[stat_index])
            connection.execute(
                """
                INSERT INTO matchup_stat_result (
                    matchup_id,
                    stat_key,
                    team_a_value,
                    team_b_value,
                    winner_side,
                    points_for_win
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    matchup_id,
                    stat_key,
                    team_a_value,
                    team_b_value,
                    winner_side(stat_key, team_a_value, team_b_value),
                    stat_points_for_win(connection, stat_key),
                ),
            )

    return gameweek, is_playoff


def main() -> None:
    args = parse_args()
    input_file = Path(args.input_file)
    db_path = Path(args.db_path)
    schema_path = Path(args.schema_path)

    content = input_file.read_text(encoding="utf-8")
    lines = [line.rstrip() for line in content.splitlines() if line.strip()]
    blocks = split_blocks(lines)
    if not blocks:
        raise ValueError(f"No gameweek blocks found in {input_file}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON;")
        load_schema(connection, schema_path)
        ensure_season(connection, args.season_slug)
        previous_gameweek: int | None = None
        previous_is_playoff = 0
        known_date_ranges: dict[str, tuple[int, int, str | None]] = {}
        playoff_round_gameweeks: dict[int, int] = {}
        cleared_gameweeks: set[tuple[str, int]] = set()
        for block in blocks:
            previous_gameweek, previous_is_playoff = import_block(
                connection,
                args.season_slug,
                block,
                previous_gameweek,
                previous_is_playoff,
                known_date_ranges,
                playoff_round_gameweeks,
                cleared_gameweeks,
            )
        connection.commit()

    print(f"Imported {len(blocks)} gameweeks from {input_file} into {db_path}")


if __name__ == "__main__":
    main()
