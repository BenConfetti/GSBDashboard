from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path


DROP_TYPES = {"DROP"}
ACQUIRE_TYPES = {"CLAIM"}
STATUS_TEAM_MAP = {
    "ACB": "Athletic Club Bapao",
    "BAR": "BarcelOna '53",
    "KLI": "Klimmania",
    "LHO": "László Hofnár",
    "MOV": "Mövenpickje FC",
    "NIM": "Nimma Koempels",
    "NOAD": "NOAD Athletic",
    "OSW": "One Season Wonderers",
    "PMP": "Poor Man's Pirlo",
    "RED": "Redford United",
    "SJB": "Sjeeterboys",
    "WW": "Wilton Wankers",
    "W_W": "Wilton Wankers",
    "FA": "FA",
}


@dataclass
class OwnershipEvent:
    gameweek: int
    event_type: str
    previous_owner: str | None
    new_owner: str | None
    transaction_datetime_iso: str | None
    transaction_datetime_text: str | None
    sort_id: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historical Fantrax ownership per player gameweek from transactions."
    )
    parser.add_argument(
        "--db-path",
        default="database/fantrax_v2.db",
        help="Path to the SQLite database file",
    )
    return parser.parse_args()


def ensure_column(connection: sqlite3.Connection) -> None:
    columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(player_gameweek)").fetchall()
    }
    if "fantrax_team_name" not in columns:
        connection.execute("ALTER TABLE player_gameweek ADD COLUMN fantrax_team_name TEXT")


def event_result(event: OwnershipEvent) -> str | None:
    event_type = event.event_type.upper().strip()
    if event_type in DROP_TYPES:
        return event.new_owner or "FA"
    if event_type in ACQUIRE_TYPES:
        return event.new_owner
    if event_type == "TRADE":
        return event.new_owner
    return event.new_owner


def normalize_status_owner(status_snapshot: str | None) -> str | None:
    if status_snapshot is None:
        return None
    cleaned = status_snapshot.strip()
    if not cleaned:
        return None
    return STATUS_TEAM_MAP.get(cleaned, cleaned)


def load_events(
    connection: sqlite3.Connection,
) -> dict[tuple[str, str], list[OwnershipEvent]]:
    events: dict[tuple[str, str], list[OwnershipEvent]] = {}
    transaction_rows = connection.execute(
        """
        SELECT
            season_slug,
            player_name,
            gameweek,
            transaction_type,
            fantrax_team_name,
            transaction_datetime_iso,
            transaction_datetime_text,
            transaction_event_id
        FROM transaction_event
        """
    ).fetchall()

    for row in transaction_rows:
        key = (row[0], row[1])
        events.setdefault(key, []).append(
            OwnershipEvent(
                gameweek=row[2],
                event_type=row[3],
                previous_owner=row[4] if row[3].upper().strip() in DROP_TYPES else "FA",
                new_owner="FA" if row[3].upper().strip() in DROP_TYPES else row[4],
                transaction_datetime_iso=row[5],
                transaction_datetime_text=row[6],
                sort_id=row[7],
            )
        )

    trade_table_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'trade_event'"
    ).fetchone()

    if trade_table_exists:
        trade_rows = connection.execute(
            """
            SELECT
                season_slug,
                player_name,
                gameweek,
                from_fantrax_team_name,
                to_fantrax_team_name,
                transaction_datetime_iso,
                transaction_datetime_text,
                trade_event_id
            FROM trade_event
            """
        ).fetchall()

        for row in trade_rows:
            key = (row[0], row[1])
            events.setdefault(key, []).append(
                OwnershipEvent(
                    gameweek=row[2],
                    event_type="TRADE",
                    previous_owner=row[3],
                    new_owner=row[4],
                    transaction_datetime_iso=row[5],
                    transaction_datetime_text=row[6],
                    sort_id=row[7],
                )
            )

    for key in events:
        events[key].sort(
            key=lambda event: (
                event.gameweek,
                event.transaction_datetime_iso or event.transaction_datetime_text or "",
                event.sort_id,
            )
        )

    return events


def load_player_gameweeks(
    connection: sqlite3.Connection,
) -> dict[tuple[str, str], list[tuple[int, str, str | None]]]:
    rows = connection.execute(
        """
        SELECT
            pg.season_slug,
            p.player_name,
            pg.gameweek,
            pg.player_id,
            pg.status_snapshot
        FROM player_gameweek pg
        JOIN player p
            ON p.player_id = pg.player_id
        ORDER BY
            pg.season_slug,
            p.player_name,
            pg.gameweek,
            pg.player_id
        """
    ).fetchall()

    result: dict[tuple[str, str], list[tuple[int, str, str | None]]] = {}
    for season_slug, player_name, gameweek, player_id, status_snapshot in rows:
        result.setdefault((season_slug, player_name), []).append(
            (gameweek, player_id, status_snapshot)
        )
    return result


def infer_initial_owner(events: list[OwnershipEvent]) -> str | None:
    if not events:
        return None

    first_event = events[0]
    first_type = first_event.event_type.upper().strip()
    if first_type in DROP_TYPES:
        return first_event.previous_owner
    if first_type == "TRADE":
        return first_event.previous_owner
    return "FA"


def backfill_ownership(connection: sqlite3.Connection) -> int:
    events_by_player = load_events(connection)
    gameweeks_by_player = load_player_gameweeks(connection)
    updated_rows = 0

    for key, gameweeks in gameweeks_by_player.items():
        events = events_by_player.get(key)
        if not events:
            for gameweek, player_id, status_snapshot in gameweeks:
                fallback_owner = normalize_status_owner(status_snapshot)
                connection.execute(
                    """
                    UPDATE player_gameweek
                    SET fantrax_team_name = ?
                    WHERE player_id = ?
                      AND season_slug = ?
                      AND gameweek = ?
                    """,
                    (fallback_owner, player_id, key[0], gameweek),
                )
                updated_rows += 1
            continue

        current_owner = infer_initial_owner(events)
        event_index = 0

        for gameweek, player_id, status_snapshot in gameweeks:
            while event_index < len(events) and events[event_index].gameweek <= gameweek:
                current_owner = event_result(events[event_index])
                event_index += 1

            resolved_owner = current_owner or normalize_status_owner(status_snapshot)

            connection.execute(
                """
                UPDATE player_gameweek
                SET fantrax_team_name = ?
                WHERE player_id = ?
                  AND season_slug = ?
                  AND gameweek = ?
                """,
                (resolved_owner, player_id, key[0], gameweek),
            )
            updated_rows += 1

    return updated_rows


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)

    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON;")
        ensure_column(connection)
        updated_rows = backfill_ownership(connection)
        connection.commit()

    print(f"Backfilled fantrax_team_name for {updated_rows} player_gameweek rows in {db_path}")


if __name__ == "__main__":
    main()
