from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from playwright.sync_api import Page, Response, sync_playwright


REQ_URL = "https://www.fantrax.com/fxpa/req"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture Fantrax standings responses while you drive the UI manually."
    )
    parser.add_argument("--league-id", required=True, help="Fantrax league id")
    parser.add_argument("--season-slug", required=True, help="Local folder name, e.g. 2025-2026")
    parser.add_argument(
        "--output-root",
        default="raw",
        help="Root folder for captured responses",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser headless after login flow is stable",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def extract_json(post_data: str | None) -> dict[str, Any] | None:
    if not post_data:
        return None
    try:
        return json.loads(post_data)
    except json.JSONDecodeError:
        return None


def extract_response_data(response_json: dict[str, Any]) -> dict[str, Any]:
    try:
        return response_json["responses"][0]["data"]
    except (KeyError, IndexError, TypeError):
        return {}


def current_period_from_response(response_json: dict[str, Any]) -> int | None:
    data = extract_response_data(response_json)
    displayed = data.get("displayedSelections")
    if isinstance(displayed, dict):
        period = displayed.get("period")
        if isinstance(period, int):
            return period

    table_list = data.get("tableList")
    if isinstance(table_list, list):
        for table in table_list:
            if not isinstance(table, dict):
                continue
            caption = str(table.get("caption", ""))
            if caption.startswith("Gameweek:"):
                value = caption.split(":", 1)[1].strip()
                if value.isdigit():
                    return int(value)
    return None


def period_file_name(period: int) -> str:
    return f"period-{period:02d}.json"


def save_response(output_dir: Path, period: int, payload: dict[str, Any], response: dict[str, Any]) -> Path:
    target = output_dir / period_file_name(period)
    target.write_text(
        json.dumps(
            {
                "captured_period": period,
                "request_payload": payload,
                "response": response,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return target


def save_debug_response(output_dir: Path, payload: dict[str, Any], response: dict[str, Any]) -> Path:
    target = output_dir / "debug-last-response.json"
    target.write_text(
        json.dumps(
            {
                "request_payload": payload,
                "response": response,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return target


def is_standings_request(payload: dict[str, Any] | None) -> bool:
    if not payload:
        return False
    msgs = payload.get("msgs")
    if not isinstance(msgs, list) or not msgs:
        return False
    first = msgs[0]
    if not isinstance(first, dict):
        return False
    return first.get("method") == "getStandings"


def is_valid_standings_response(response_json: dict[str, Any]) -> bool:
    if response_json.get("pageError"):
        return False
    responses = response_json.get("responses")
    if not isinstance(responses, list) or not responses:
        return False
    first = responses[0]
    if not isinstance(first, dict):
        return False
    return "data" in first and not first.get("pageError")


def listen_for_standings(page: Page, output_dir: Path, seen_periods: set[int]) -> None:
    def handle_response(response: Response) -> None:
        if response.request.method != "POST" or response.url != REQ_URL:
            return

        payload = extract_json(response.request.post_data)
        if not is_standings_request(payload):
            return

        try:
            response_json = response.json()
        except Exception:
            return

        if not is_valid_standings_response(response_json):
            debug_path = save_debug_response(output_dir, payload or {}, response_json)
            print(f"Standings request received, but response was invalid. Debug saved to {debug_path}")
            return

        period = current_period_from_response(response_json)
        if period is None:
            debug_path = save_debug_response(output_dir, payload or {}, response_json)
            print(f"Could not determine period from standings response. Debug saved to {debug_path}")
            return

        path = save_response(output_dir, period, payload or {}, response_json)
        status = "updated" if period in seen_periods else "saved"
        seen_periods.add(period)
        print(f"{status.title()} period {period}: {path}")

    page.on("response", handle_response)


def wait_for_manual_login(page: Page, standings_url: str) -> None:
    page.goto(standings_url, wait_until="domcontentloaded")
    print("")
    print("1. Log handmatig in bij Fantrax in de geopende browser.")
    print("2. Ga naar de standings-pagina.")
    print("3. Zet daar de view op 'By Period'.")
    print("4. Druk daarna hier op Enter.")
    input()


def main() -> None:
    args = parse_args()
    standings_url = f"https://www.fantrax.com/fantasy/league/{args.league_id}/standings"
    output_dir = Path(args.output_root) / args.season_slug / "standings"
    ensure_dir(output_dir)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context()
        page = context.new_page()
        seen_periods: set[int] = set()

        listen_for_standings(page, output_dir, seen_periods)
        wait_for_manual_login(page, standings_url)

        print("")
        print("Capture staat aan.")
        print("Wissel nu handmatig van period in Fantrax.")
        print("Bij elke echte getStandings-response slaat het script automatisch een JSON-bestand op.")
        print("Druk op Enter als je klaar bent.")
        input()

        browser.close()


if __name__ == "__main__":
    main()
