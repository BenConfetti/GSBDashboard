from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class TeamRecord:
    team_id: str
    name: str
    short_name: str | None
    logo_url: str | None


@dataclass
class StandingRecord:
    season_slug: str
    period: int
    team_id: str
    rank: int
    team_name: str
    wins: float
    losses: float
    ties: float
    points: float
    win_pct: float
    games_back: float
    category_points_for: float
    category_points_against: float
    streak_raw: str


@dataclass
class MatchupRecord:
    season_slug: str
    period: int
    matchup_id: str
    team_id: str
    team_name: str
    wins: float
    losses: float
    ties: float
    category_points: float
    categories: dict[str, str]
    is_highlighted_team: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize saved Fantrax standings responses.")
    parser.add_argument("--input", required=True, help="Folder with raw standings JSON")
    parser.add_argument("--output", required=True, help="Folder for normalized output")
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def safe_float(value: str) -> float:
    if value == "-":
        return 0.0
    return float(value)


def extract_response_data(document: dict[str, Any]) -> dict[str, Any]:
    return document["response"]["responses"][0]["data"]


def normalize_teams(data: dict[str, Any]) -> list[TeamRecord]:
    teams = []
    for team_id, team in data.get("fantasyTeamInfo", {}).items():
        teams.append(
            TeamRecord(
                team_id=team_id,
                name=team["name"],
                short_name=team.get("shortName"),
                logo_url=team.get("logoUrl512"),
            )
        )
    return sorted(teams, key=lambda item: item.name.lower())


def standings_table(data: dict[str, Any]) -> dict[str, Any]:
    return data["tableList"][0]


def matchup_tables(data: dict[str, Any]) -> list[dict[str, Any]]:
    return [table for table in data["tableList"][1:] if str(table.get("caption", "")).startswith("Gameweek:")]


def normalize_standings(season_slug: str, period: int, data: dict[str, Any]) -> list[StandingRecord]:
    rows = standings_table(data)["rows"]
    output = []
    for row in rows:
        fixed = row["fixedCells"]
        cells = row["cells"]
        output.append(
            StandingRecord(
                season_slug=season_slug,
                period=period,
                team_id=fixed[1]["teamId"],
                rank=int(fixed[0]["content"]),
                team_name=fixed[1]["content"],
                wins=safe_float(cells[0]["content"]),
                losses=safe_float(cells[1]["content"]),
                ties=safe_float(cells[2]["content"]),
                points=safe_float(cells[3]["content"]),
                win_pct=safe_float(cells[4]["content"]),
                games_back=safe_float(cells[5]["content"]),
                category_points_for=safe_float(cells[6]["content"]),
                category_points_against=safe_float(cells[7]["content"]),
                streak_raw=cells[8]["content"],
            )
        )
    return output


def normalize_matchups(season_slug: str, data: dict[str, Any]) -> list[MatchupRecord]:
    matchups = []
    for table in matchup_tables(data):
        caption = str(table.get("caption", ""))
        period = int(caption.split(":")[1].strip())
        category_names = [cell["shortName"] for cell in table["header"]["cells"][4:]]
        for row in table["rows"]:
            values = row["cells"]
            categories = {
                name: values[index + 4]["content"]
                for index, name in enumerate(category_names)
            }
            matchups.append(
                MatchupRecord(
                    season_slug=season_slug,
                    period=period,
                    matchup_id=row["matchupId"],
                    team_id=row["fixedCells"][0]["teamId"],
                    team_name=row["fixedCells"][0]["content"],
                    wins=safe_float(values[0]["content"]),
                    losses=safe_float(values[1]["content"]),
                    ties=safe_float(values[2]["content"]),
                    category_points=safe_float(values[3]["content"]),
                    categories=categories,
                    is_highlighted_team=bool(row.get("highlight", False)),
                )
            )
    return matchups


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output)
    ensure_dir(output_dir)

    raw_files = sorted(input_dir.glob("period-*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No period JSON files found in {input_dir}")

    season_slug = input_dir.parent.name
    teams_written = False
    all_standings: list[StandingRecord] = []
    all_matchups: list[MatchupRecord] = []

    for raw_file in raw_files:
        document = read_json(raw_file)
        data = extract_response_data(document)
        period = data["displayedSelections"]["period"]

        if not teams_written:
            teams = [asdict(record) for record in normalize_teams(data)]
            write_json(output_dir / "teams.json", teams)
            teams_written = True

        standings = normalize_standings(season_slug, period, data)
        write_json(
            output_dir / f"standings-period-{period:02d}.json",
            [asdict(record) for record in standings],
        )
        all_standings.extend(standings)
        all_matchups.extend(normalize_matchups(season_slug, data))

    write_json(output_dir / "standings-all.json", [asdict(record) for record in all_standings])
    write_json(output_dir / "matchups-all.json", [asdict(record) for record in all_matchups])


if __name__ == "__main__":
    main()

