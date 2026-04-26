from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlencode

from playwright.sync_api import Page, sync_playwright


BASE_URL = "https://www.fantrax.com/fxpa/downloadPlayerStats"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk download Fantrax player gameweek CSVs with an authenticated browser session."
    )
    parser.add_argument("--league-id", required=True, help="Fantrax league id")
    parser.add_argument("--season-slug", required=True, help="Folder label, for example 2025-2026")
    parser.add_argument(
        "--season-projection",
        required=True,
        help="Fantrax season code, for example SEASON_925_BY_PERIOD",
    )
    parser.add_argument("--start-date", required=True, help="Season start date, YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Season end date, YYYY-MM-DD")
    parser.add_argument(
        "--team-id",
        required=False,
        help="Your Fantrax team id, as used in the downloadPlayerStats request",
    )
    parser.add_argument(
        "--period-start",
        type=int,
        default=1,
        help="First gameweek to download",
    )
    parser.add_argument(
        "--period-end",
        type=int,
        required=True,
        help="Last gameweek to download",
    )
    parser.add_argument(
        "--output-root",
        default="downloaddata/player_gameweeks",
        help="Folder to save CSV files into",
    )
    parser.add_argument(
        "--position-or-group",
        default="SOCCER_NON_GOALIE",
        help="Fantrax position/group filter",
    )
    parser.add_argument(
        "--status-or-team-filter",
        default="ALL",
        help="Fantrax team/status filter",
    )
    parser.add_argument(
        "--sort-type",
        default="SCORE",
        help="Fantrax sort key",
    )
    parser.add_argument(
        "--scoring-category-type",
        default="5",
        help="Fantrax scoring category type",
    )
    parser.add_argument(
        "--misc-display-type",
        default="1",
        help="Fantrax misc display type",
    )
    parser.add_argument(
        "--max-results-per-page",
        default="20",
        help="Fantrax max results per page parameter for export",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser headless after the flow is stable",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def wait_for_manual_login(page: Page, url: str) -> None:
    page.goto(url, wait_until="domcontentloaded")
    print("")
    print("1. Log handmatig in bij Fantrax in de geopende browser.")
    print("2. Navigeer naar de Players > Stats pagina van dit seizoen.")
    print("3. Controleer dat de filters kloppen.")
    print("4. Druk daarna op Enter om de bulk-download te starten.")
    input()


def build_request_url(args: argparse.Namespace, period: int) -> str:
    params = {
        "leagueId": args.league_id,
        "pageNumber": "1",
        "view": "STATS",
        "positionOrGroup": args.position_or_group,
        "seasonOrProjection": args.season_projection,
        "timeframeTypeCode": "BY_PERIOD",
        "transactionPeriod": str(period),
        "miscDisplayType": args.misc_display_type,
        "sortType": args.sort_type,
        "maxResultsPerPage": args.max_results_per_page,
        "statusOrTeamFilter": args.status_or_team_filter,
        "scoringCategoryType": args.scoring_category_type,
        "timeStartType": "PERIOD_ONLY",
        "schedulePageAdj": "0",
        "searchName": "",
        "startDate": args.start_date,
        "endDate": args.end_date,
    }
    if args.team_id:
        params["teamId"] = args.team_id
    return f"{BASE_URL}?{urlencode(params)}"


def fetch_csv(page: Page, url: str, referer: str) -> str:
    content = page.evaluate(
        """
        async ({ url, referer }) => {
          const response = await fetch(url, {
            method: "GET",
            credentials: "include",
            headers: {
              "accept": "text/csv,text/plain,*/*",
              "referer": referer
            }
          });

          if (!response.ok) {
            throw new Error(`Fantrax CSV download failed: ${response.status} ${response.statusText}`);
          }

          return await response.text();
        }
        """,
        {
            "url": url,
            "referer": referer,
        },
    )
    return str(content)


def output_file_name(period: int) -> str:
    return f"playerdata_gw{period:02d}.csv"


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_root) / args.season_slug
    ensure_dir(output_dir)

    landing_url = (
        f"https://www.fantrax.com/fantasy/league/{args.league_id}/players;"
        f"positionOrGroup={args.position_or_group};miscDisplayType={args.misc_display_type};"
        f"pageNumber=1;statusOrTeamFilter={args.status_or_team_filter};"
        f"seasonOrProjection={args.season_projection};timeframeTypeCode=BY_PERIOD;"
        f"startDate={args.start_date};endDate={args.end_date};transactionPeriod={args.period_start}"
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context()
        page = context.new_page()

        wait_for_manual_login(page, landing_url)

        for period in range(args.period_start, args.period_end + 1):
            request_url = build_request_url(args, period)
            referer = (
                f"https://www.fantrax.com/fantasy/league/{args.league_id}/players;"
                f"positionOrGroup={args.position_or_group};miscDisplayType={args.misc_display_type};"
                f"pageNumber=1;statusOrTeamFilter={args.status_or_team_filter};"
                f"seasonOrProjection={args.season_projection};timeframeTypeCode=BY_PERIOD;"
                f"startDate={args.start_date};endDate={args.end_date};transactionPeriod={period}"
            )
            csv_text = fetch_csv(page, request_url, referer)
            target = output_dir / output_file_name(period)
            target.write_text(csv_text, encoding="utf-8")
            print(f"Saved GW {period}: {target}")

        browser.close()


if __name__ == "__main__":
    main()
