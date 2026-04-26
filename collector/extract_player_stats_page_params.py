from __future__ import annotations

import argparse
from urllib.parse import parse_qs, unquote, urlparse

from playwright.sync_api import BrowserContext, Page, sync_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Fantrax player stats URL parameters from the currently open page."
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser headless after the flow is stable",
    )
    return parser.parse_args()


def parse_semicolon_params(url: str) -> dict[str, str]:
    parsed = urlparse(url)
    tail = parsed.path
    if ";" not in tail:
        return {}
    semicolon_part = tail.split(";", 1)[1]
    pairs = semicolon_part.split(";")
    output: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        output[key] = unquote(value)
    return output


def wait_for_navigation(page: Page) -> None:
    print("")
    print("1. Log handmatig in bij Fantrax.")
    print("2. Navigeer naar de juiste Players > Stats pagina voor het gewenste seizoen.")
    print("3. Zet de filters zoals je ze wilt gebruiken.")
    print("4. Druk daarna op Enter.")
    input()


def best_fantrax_page(context: BrowserContext) -> Page | None:
    candidates: list[tuple[int, Page]] = []
    for page in context.pages:
        try:
            url = page.url
        except Exception:
            continue
        score = 0
        if "fantrax.com/fantasy/league/" in url:
            score += 10
        if "/players" in url:
            score += 5
        if "seasonOrProjection=" in url:
            score += 3
        if "transactionPeriod=" in url:
            score += 2
        if score > 0:
            candidates.append((score, page))

    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def main() -> None:
    args = parse_args()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context()
        page = context.new_page()
        page.goto("https://www.fantrax.com", wait_until="domcontentloaded")

        wait_for_navigation(page)

        active_page = best_fantrax_page(context) or page
        url = active_page.url
        semicolon_params = parse_semicolon_params(url)
        parsed = urlparse(url)
        query_params = {
            key: values[0]
            for key, values in parse_qs(parsed.query).items()
            if values
        }

        merged = {**query_params, **semicolon_params}

        print("")
        print("Open Fantrax pages:")
        for open_page in context.pages:
            try:
                print(open_page.url)
            except Exception:
                pass
        print("")
        print("Current page URL:")
        print(url)
        print("")
        print("Detected parameters:")
        for key in sorted(merged):
            print(f"{key}={merged[key]}")

        print("")
        print("Useful values:")
        for key in [
            "seasonOrProjection",
            "startDate",
            "endDate",
            "transactionPeriod",
            "positionOrGroup",
            "statusOrTeamFilter",
            "miscDisplayType",
            "teamId",
        ]:
            if key in merged:
                print(f"{key}: {merged[key]}")

        browser.close()


if __name__ == "__main__":
    main()
