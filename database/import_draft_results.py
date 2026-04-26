from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from pathlib import Path


DRAFT_FILE_RE = re.compile(r"(?:DraftResults|Draft)(\d{4})\.(?:csv|txt)$", re.IGNORECASE)
PICK_RE = re.compile(r"^(?P<round>\d+)-(?P<pick>\d+)$")
TEAM_NAME_ALIASES = {
    "Athletic de Bapao": "Athletic Club Bapao",
    "MÃ¶venpickje FC": "Mövenpickje FC",
    "MÃƒÂ¶venpickje FC": "Mövenpickje FC",
    "MÃƒÆ’Ã‚Â¶venpickje FC": "Mövenpickje FC",
    "LÃ¡szlÃ³ HofnÃ¡r": "László Hofnár",
    "LÃƒÂ¡szlÃƒÂ³ HofnÃƒÂ¡r": "László Hofnár",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Fantrax draft result files into SQLite."
    )
    parser.add_argument(
        "--input-dir",
        default="downloaddata",
        help="Folder containing Draft*.csv/.txt files",
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


def draft_files(input_dir: Path) -> list[Path]:
    selected: dict[str, Path] = {}
    for path in sorted(input_dir.iterdir()):
        if not path.is_file() or not DRAFT_FILE_RE.match(path.name):
            continue
        season_slug = season_slug_from_filename(path)
        existing = selected.get(season_slug)
        if existing is None:
            selected[season_slug] = path
            continue
        if existing.suffix.lower() != ".csv" and path.suffix.lower() == ".csv":
            selected[season_slug] = path
    return [selected[key] for key in sorted(selected)]


def season_slug_from_filename(path: Path) -> str:
    match = DRAFT_FILE_RE.match(path.name)
    if not match:
        raise ValueError(f"Could not derive season from filename: {path.name}")
    token = match.group(1)
    start_short = int(token[:2])
    end_short = int(token[2:])
    return f"{2000 + start_short}-{2000 + end_short}"


def load_schema(connection: sqlite3.Connection, schema_path: Path) -> None:
    connection.executescript(schema_path.read_text(encoding="utf-8"))


def ensure_draft_event_player_id_column(connection: sqlite3.Connection) -> None:
    table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'draft_event'"
    ).fetchone()
    if not table_exists:
        return

    columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(draft_event)").fetchall()
    }
    if "player_id" not in columns:
        connection.execute("ALTER TABLE draft_event ADD COLUMN player_id TEXT")


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


def normalize_team_name(value: str) -> str:
    cleaned = value.strip()
    return TEAM_NAME_ALIASES.get(cleaned, cleaned)


def load_tokens(path: Path) -> list[str]:
    if path.suffix.lower() == ".txt":
        return [line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]

    tokens: list[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            for cell in row:
                cleaned = cell.strip()
                if cleaned:
                    tokens.append(cleaned)
    return tokens


def parse_csv_rows(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required_columns = {"Round", "Pick", "Ov Pick", "Pos", "Player", "Team", "Fantasy Team"}
        if reader.fieldnames is None or not required_columns.issubset(set(reader.fieldnames)):
            return []

        for row in reader:
            if not row:
                continue
            player_name = (row.get("Player") or "").strip()
            fantasy_team = normalize_team_name((row.get("Fantasy Team") or "").strip())
            round_text = (row.get("Round") or "").strip()
            pick_text = (row.get("Pick") or "").strip()
            overall_text = (row.get("Ov Pick") or "").strip()
            if not player_name or not fantasy_team or not round_text or not pick_text or not overall_text:
                continue

            try:
                round_number = int(round_text)
                pick_in_round = int(pick_text)
                overall_pick_number = int(overall_text)
            except ValueError:
                continue

            player_id = (row.get("Player ID") or "").strip() or None
            rows.append(
                {
                    "player_id": player_id,
                    "round_number": round_number,
                    "pick_in_round": pick_in_round,
                    "overall_pick": overall_pick_number,
                    "fantrax_team_name": fantasy_team,
                    "player_name": player_name,
                    "position": ((row.get("Pos") or "").strip() or None),
                    "premier_league_team_code": ((row.get("Team") or "").strip() or None),
                    "raw_player_team_text": (row.get("Team") or "").strip(),
                    "raw_row_json": json.dumps(row, ensure_ascii=False),
                }
            )
    return rows


def team_for_pick(teams: list[str], round_number: int, pick_in_round: int) -> str:
    if round_number % 2 == 1:
        return teams[pick_in_round - 1]
    return teams[len(teams) - pick_in_round]


def overall_pick(teams_count: int, round_number: int, pick_in_round: int) -> int:
    return (round_number - 1) * teams_count + pick_in_round


def parse_player_team_code(raw_value: str) -> str | None:
    cleaned = raw_value.strip()
    if not cleaned:
        return None
    if cleaned.startswith("-"):
        cleaned = cleaned[1:].strip()
    return cleaned or None


def looks_like_position(value: str) -> bool:
    cleaned = value.strip().upper()
    if not cleaned:
        return False
    return bool(re.fullmatch(r"[DMFG](?:,[DMFG])?", cleaned))


def looks_like_team_code_line(value: str) -> bool:
    cleaned = value.strip()
    if not cleaned:
        return False
    return cleaned.startswith("-")


def import_draft_file(connection: sqlite3.Connection, season_slug: str, path: Path) -> int:
    ensure_season(connection, season_slug)
    structured_rows = parse_csv_rows(path) if path.suffix.lower() == ".csv" else []
    if structured_rows:
        imported_rows = 0
        for row_payload in structured_rows:
            pl_team_code = row_payload["premier_league_team_code"]
            player_id = row_payload["player_id"]
            if pl_team_code:
                connection.execute(
                    """
                    INSERT INTO premier_league_team (team_code)
                    VALUES (?)
                    ON CONFLICT (team_code) DO NOTHING
                    """,
                    (pl_team_code,),
                )
            if player_id:
                connection.execute(
                    """
                    INSERT INTO player (player_id, player_name)
                    VALUES (?, ?)
                    ON CONFLICT (player_id) DO UPDATE SET
                        player_name = excluded.player_name
                    """,
                    (player_id, row_payload["player_name"]),
                )

            connection.execute(
                """
                INSERT INTO draft_event (
                    season_slug,
                    round_number,
                    pick_in_round,
                    overall_pick,
                    fantrax_team_name,
                    player_id,
                    player_name,
                    position,
                    premier_league_team_code,
                    raw_player_team_text,
                    raw_row_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (season_slug, overall_pick) DO UPDATE SET
                    round_number = excluded.round_number,
                    pick_in_round = excluded.pick_in_round,
                    fantrax_team_name = excluded.fantrax_team_name,
                    player_id = excluded.player_id,
                    player_name = excluded.player_name,
                    position = excluded.position,
                    premier_league_team_code = excluded.premier_league_team_code,
                    raw_player_team_text = excluded.raw_player_team_text,
                    raw_row_json = excluded.raw_row_json
                """,
                (
                    season_slug,
                    row_payload["round_number"],
                    row_payload["pick_in_round"],
                    row_payload["overall_pick"],
                    row_payload["fantrax_team_name"],
                    row_payload["player_id"],
                    row_payload["player_name"],
                    row_payload["position"],
                    row_payload["premier_league_team_code"],
                    row_payload["raw_player_team_text"],
                    row_payload["raw_row_json"],
                ),
            )
            imported_rows += 1
        return imported_rows

    tokens = load_tokens(path)
    if not tokens:
        return 0

    first_pick_index = next((idx for idx, token in enumerate(tokens) if PICK_RE.match(token)), None)
    if first_pick_index is None:
        raise ValueError(f"No draft picks found in file: {path}")

    teams = [normalize_team_name(token) for token in tokens[:first_pick_index]]
    if len(teams) < 2:
        raise ValueError(f"Not enough teams found before first pick in file: {path}")

    imported_rows = 0
    index = first_pick_index
    while index < len(tokens):
        pick_token = tokens[index]
        match = PICK_RE.match(pick_token)
        if not match:
            raise ValueError(f"Expected draft pick token at index {index}, got {pick_token!r}")

        round_number = int(match.group("round"))
        pick_in_round = int(match.group("pick"))
        if pick_in_round < 1 or pick_in_round > len(teams):
            raise ValueError(
                f"Pick {pick_token} exceeds team count {len(teams)} in {path.name}"
            )

        default_team_name = team_for_pick(teams, round_number, pick_in_round)
        cursor = index + 1
        override_team_name: str | None = None

        if cursor >= len(tokens):
            break

        if not PICK_RE.match(tokens[cursor]) and not looks_like_position(tokens[cursor]):
            possible_team = normalize_team_name(tokens[cursor])
            if possible_team in teams:
                override_team_name = possible_team
                cursor += 1

        if cursor >= len(tokens):
            break

        if PICK_RE.match(tokens[cursor]):
            index = cursor
            continue

        player_name = tokens[cursor].strip()
        cursor += 1

        if cursor >= len(tokens) or not looks_like_position(tokens[cursor]):
            index = cursor
            continue
        position = tokens[cursor].strip()
        cursor += 1

        if cursor >= len(tokens) or not looks_like_team_code_line(tokens[cursor]):
            index = cursor
            continue
        raw_player_team_text = tokens[cursor].strip()
        cursor += 1

        fantrax_team_name = override_team_name or default_team_name
        pl_team_code = parse_player_team_code(raw_player_team_text)
        if pl_team_code:
            connection.execute(
                """
                INSERT INTO premier_league_team (team_code)
                VALUES (?)
                ON CONFLICT (team_code) DO NOTHING
                """,
                (pl_team_code,),
            )

        row_payload = {
            "player_id": None,
            "round_number": round_number,
            "pick_in_round": pick_in_round,
            "overall_pick": overall_pick(len(teams), round_number, pick_in_round),
            "fantrax_team_name": fantrax_team_name,
            "player_name": player_name,
            "position": position or None,
            "premier_league_team_code": pl_team_code,
            "raw_player_team_text": raw_player_team_text,
        }

        connection.execute(
            """
            INSERT INTO draft_event (
                season_slug,
                round_number,
                pick_in_round,
                overall_pick,
                fantrax_team_name,
                player_id,
                player_name,
                position,
                premier_league_team_code,
                raw_player_team_text,
                raw_row_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (season_slug, overall_pick) DO UPDATE SET
                round_number = excluded.round_number,
                pick_in_round = excluded.pick_in_round,
                fantrax_team_name = excluded.fantrax_team_name,
                player_id = excluded.player_id,
                player_name = excluded.player_name,
                position = excluded.position,
                premier_league_team_code = excluded.premier_league_team_code,
                raw_player_team_text = excluded.raw_player_team_text,
                raw_row_json = excluded.raw_row_json
            """,
            (
                season_slug,
                row_payload["round_number"],
                row_payload["pick_in_round"],
                row_payload["overall_pick"],
                row_payload["fantrax_team_name"],
                row_payload["player_id"],
                row_payload["player_name"],
                row_payload["position"],
                row_payload["premier_league_team_code"],
                row_payload["raw_player_team_text"],
                json.dumps(row_payload, ensure_ascii=False),
            ),
        )
        imported_rows += 1
        index = cursor

    return imported_rows


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    db_path = Path(args.db_path)
    schema_path = Path(args.schema_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        load_schema(connection, schema_path)
        ensure_draft_event_player_id_column(connection)

        files = draft_files(input_dir)
        if not files:
            print(f"No Draft*.csv/.txt files found in {input_dir}")
            return

        total_rows = 0
        for path in files:
            season_slug = season_slug_from_filename(path)
            imported = import_draft_file(connection, season_slug, path)
            total_rows += imported
            print(f"Imported {imported} draft picks from {path.name} into {season_slug}")

        connection.commit()
        print(f"Done. Imported {total_rows} draft picks into {db_path}")


if __name__ == "__main__":
    main()
