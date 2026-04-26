from __future__ import annotations

import sqlite3
import random
from pathlib import Path

import altair as alt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = BASE_DIR / "database" / "fantrax_v2.db"
IGNORED_STATS = {"BR", "INT", "A", "GA"}
DISPLAY_LABELS = {
    "player_name": "Player",
    "season_slug": "Season",
    "gameweek": "GW",
    "season_gameweek_label": "Season / GW",
    "date_range_label": "Date range",
    "fantrax_team_name": "Fantrax team",
    "premier_league_team_code": "PL team",
    "positions": "Positions",
    "position": "Position",
    "gameweeks_played": "GW played",
    "games_played": "Matches played",
    "played_flag": "Played",
    "standings_points": "Standings points",
    "matchup_wins": "Wins",
    "matchup_draws": "Draws",
    "matchup_losses": "Losses",
    "total_matchup_points_for": "Pts for",
    "total_matchup_points_against": "Pts against",
    "total_category_wins": "Categories won",
    "total_category_losses": "Categories lost",
    "total_category_ties": "Category ties",
    "playoff_matches": "Playoff matches",
    "fantrax_teams_played_for": "Teams played for",
    "total_gameweeks_played": "Total GW",
    "teams_breakdown": "Team breakdown",
    "avg_score": "Avg score",
    "first_gw": "First GW",
    "last_gw": "Last GW",
    "first_date_range": "First date range",
    "last_date_range": "Last date range",
    "stat_value": "Value",
    "is_playoff": "Playoff",
    "result": "Result",
    "transaction_type": "Type",
    "bid_amount": "Bid",
    "priority_value": "Priority",
    "transaction_datetime_text": "Timestamp",
    "transactions": "Transactions",
    "total_bid": "Total bid",
    "avg_bid": "Avg bid",
    "round_number": "Round",
    "pick_in_round": "Pick",
    "overall_pick": "Overall pick",
    "score_rank": "Score rank",
    "draft_value": "Draft value",
    "steal_index": "Draft value",
    "drafted_score": "Drafted score",
    "post_claim_score": "Post-claim score",
    "post_claim_gw": "Post-claim GW",
    "value_per_dollar": "Value per $",
    "from_fantrax_team_name": "From team",
    "to_fantrax_team_name": "To team",
    "post_trade_score": "Post-trade score",
    "post_trade_gw": "Post-trade GW",
    "opponent_name": "Opponent",
    "league_points": "League points",
    "rolling_5_ppg": "Rolling 5 PPG",
    "rolling_15_ppg": "Rolling 15 PPG",
    "primary_position": "Primary position",
    "avg_bid_per_move": "Avg bid / move",
    "claims": "Claims",
    "trades": "Trades",
    "draft_picks": "Draft picks",
    "best_score": "Best score",
    "avg_matchup_points": "Avg matchup points",
    "matchup_points_for": "Matchup points",
    "matchup_points_against": "Against",
    "category_wins_for": "Categories won",
    "category_wins_against": "Categories lost",
    "fantrax_teams": "Fantrax teams",
    "premier_league_teams": "PL teams",
    "key_opponents": "Key opponents",
    "transactions_paid": "Paid bids",
    "zero_euro_claims": "Zero-euro claims",
    "highest_bid": "Highest bid",
    "claims_with_bid": "Claims with bid",
    "percentile": "Percentile",
    "stat_key": "Stat",
    "dropped_in_gw": "Dropped in GW",
    "position_count": "Position count",
    "players_received": "Players received",
    "players_sent": "Players sent",
    "trade_partners": "Trade partners",
    "rank_within_stat": "Pos",
    "gws_after_trade": "GW's ná trade",
    "gws_after_claim": "GW's ná claim",
}
DISPLAY_LABELS.update(
    {
        "gws_after_trade": "GW's na trade",
        "gws_after_claim": "GW's na claim",
        "team_stat_total": "Team total",
        "streak_type": "Streak type",
        "streak_length": "Streak length",
        "start_gw": "Start Jaar-GW",
        "end_gw": "End Jaar-GW",
        "categories_won": "Categories won",
        "categories_lost": "Categories lost",
        "wins": "Wins",
        "draws": "Draws",
        "losses": "Losses",
        "team_name": "Team",
        "representation_index": "Representation index",
        "team_share_pct": "Team share %",
        "league_share_pct": "League share %",
        "player_count": "Player count",
        "move_count": "Moves",
        "trade_count": "Trades",
        "claims_count": "Claims",
        "zero_euro_count": "Zero-euro claims",
        "paid_bid_count": "Paid bids",
        "team_avg_score": "Avg score",
        "stat_per_gw": "Per GW",
        "gw_count": "GW count",
    }
)
NUMERIC_DECIMALS = {
    "avg_score": 2,
    "best_score": 2,
    "score": 2,
    "ppg": 2,
    "rolling_5_ppg": 2,
    "rolling_15_ppg": 2,
    "bid_amount": 1,
    "avg_bid": 1,
    "avg_bid_per_move": 1,
    "total_bid": 1,
    "highest_bid": 1,
    "post_trade_score": 2,
    "post_claim_score": 2,
    "drafted_score": 2,
    "draft_value": 0,
    "steal_index": 0,
    "score_rank": 0,
    "overall_pick": 0,
    "round_number": 0,
    "pick_in_round": 0,
    "priority_value": 0,
    "standings_points": 0,
    "matchup_points_for": 0,
    "matchup_points_against": 0,
    "total_matchup_points_for": 0,
    "total_matchup_points_against": 0,
    "gameweeks_played": 0,
    "games_played": 0,
    "played_flag": 0,
    "claims": 0,
    "trades": 0,
    "transactions": 0,
    "draft_picks": 0,
    "fantrax_teams_played_for": 0,
    "total_gameweeks_played": 0,
    "matchup_wins": 0,
    "matchup_draws": 0,
    "matchup_losses": 0,
    "total_category_wins": 0,
    "total_category_losses": 0,
    "total_category_ties": 0,
    "category_wins_for": 0,
    "category_wins_against": 0,
    "categories_won": 0,
    "categories_lost": 0,
    "streak_length": 0,
    "percentile": 1,
    "representation_index": 2,
    "team_share_pct": 1,
    "league_share_pct": 1,
    "team_stat_total": 0,
    "stat_per_gw": 2,
    "stat_value": 0,
}
TEAM_COLORS = {
    "Athletic Club Bapao": "#0f6b53",
    "BarcelOna '53": "#c84c2f",
    "Klimmania": "#a14fbe",
    "László Hofnár": "#7a5c2f",
    "Mövenpickje FC": "#1e88e5",
    "Nimma Koempels": "#bf8b16",
    "NOAD Athletic": "#287271",
    "One Season Wonderers": "#b83b5e",
    "Poor Man's Pirlo": "#4a6fa5",
    "Redford United": "#d1495b",
    "Sjeeterboys": "#2a9d8f",
    "Wilton Wankers": "#6c757d",
    "FA": "#8a8f98",
    "UNKNOWN": "#8a8f98",
}
TEAM_NAME_ALIASES = {
    "Athletic de Bapao": "Athletic Club Bapao",
    "MÃ¶venpickje FC": "Mövenpickje FC",
    "MÃƒÂ¶venpickje FC": "Mövenpickje FC",
    "LÃ¡szlÃ³ HofnÃ¡r": "László Hofnár",
    "LHO": "László Hofnár",
    "ACB": "Athletic Club Bapao",
    "ADB": "Athletic Club Bapao",
    "BAR": "BarcelOna '53",
    "KLI": "Klimmania",
    "MOV": "Mövenpickje FC",
    "NIM": "Nimma Koempels",
    "NOAD": "NOAD Athletic",
    "OSW": "One Season Wonderers",
    "PNP": "Poor Man's Pirlo",
    "PMP": "Poor Man's Pirlo",
    "FUG": "SSC Fugazi",
    "RED": "Redford United",
    "SJB": "Sjeeterboys",
    "WW": "Wilton Wankers",
    "W_W": "Wilton Wankers",
}
FORMATION_MAP = {
    "5-4-1": (5, 4, 1),
    "5-3-2": (5, 3, 2),
    "4-5-1": (4, 5, 1),
    "4-4-2": (4, 4, 2),
    "4-3-3": (4, 3, 3),
    "3-5-2": (3, 5, 2),
    "3-4-3": (3, 4, 3),
}


def inject_css() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg: #f4efe4;
            --panel: rgba(251,247,239,0.9);
            --ink: #11211c;
            --muted: #5d6d65;
            --line: #d8ceb8;
            --accent: #0f6b53;
            --accent-deep: #0b5644;
            --warm: #d9822b;
        }

        .stApp {
            background:
                radial-gradient(circle at top right, rgba(217,130,43,0.14), transparent 30%),
                radial-gradient(circle at top left, rgba(15,107,83,0.12), transparent 34%),
                linear-gradient(180deg, #f9f4ea 0%, var(--bg) 100%);
            color: var(--ink);
        }

        html, body, [data-testid="stAppViewContainer"], .main {
            background: transparent !important;
            color: var(--ink) !important;
        }

        .main .block-container {
            max-width: 1340px;
            padding-top: 2rem;
            padding-bottom: 3rem;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0e241e 0%, #14342c 100%);
            border-right: 1px solid rgba(255,255,255,0.08);
        }

        [data-testid="stSidebar"] * {
            color: #f7f1e6 !important;
        }

        h1, h2, h3 {
            color: var(--ink);
            letter-spacing: -0.02em;
        }

        .hero {
            background: linear-gradient(135deg, rgba(15,107,83,0.98), rgba(17,33,28,0.97));
            color: #fdf8ee;
            border-radius: 30px;
            padding: 1.45rem 1.6rem 1.6rem 1.6rem;
            box-shadow: 0 18px 50px rgba(17,33,28,0.16);
            margin-bottom: 1rem;
            overflow: hidden;
            position: relative;
        }

        .hero::after {
            content: "";
            position: absolute;
            width: 260px;
            height: 260px;
            border-radius: 999px;
            background: radial-gradient(circle, rgba(217,130,43,0.28), transparent 70%);
            right: -70px;
            top: -70px;
        }

        .hero-kicker {
            color: #cce8df;
            font-size: 0.83rem;
            letter-spacing: 0.13em;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .hero h1 {
            color: #fff8ef;
            margin: 0;
            font-size: 2.45rem;
        }

        .hero p {
            max-width: 820px;
            color: #dbe8e2;
            margin-top: 0.65rem;
            margin-bottom: 0;
            line-height: 1.55;
        }

        .metric-card {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 22px;
            padding: 1rem;
            box-shadow: 0 10px 26px rgba(17,33,28,0.05);
        }

        .metric-label {
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-size: 0.8rem;
        }

        .metric-value {
            color: var(--ink);
            font-weight: 700;
            font-size: 1.95rem;
            margin-top: 0.25rem;
        }

        .section-card {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 24px;
            padding: 1rem 1rem 0.55rem 1rem;
            box-shadow: 0 10px 24px rgba(17,33,28,0.05);
            margin-top: 0.8rem;
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid var(--line);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0 8px 24px rgba(17,33,28,0.04);
        }

        .journeyman-card {
            background: linear-gradient(180deg, #fffaf1 0%, #f7efdf 100%);
            border: 1px solid var(--line);
            border-radius: 22px;
            padding: 1rem;
            margin-bottom: 0.9rem;
            box-shadow: 0 10px 24px rgba(17,33,28,0.05);
        }

        .journeyman-title {
            color: var(--ink);
            font-size: 1.15rem;
            font-weight: 700;
        }

        .journeyman-meta {
            color: var(--muted);
            font-size: 0.94rem;
            margin-top: 0.2rem;
            margin-bottom: 0.7rem;
        }

        .team-chip {
            display: inline-block;
            color: #fffdf8;
            font-weight: 700;
            font-size: 0.85rem;
            border-radius: 999px;
            padding: 0.36rem 0.7rem;
            margin: 0 0.35rem 0.35rem 0;
            box-shadow: inset 0 -1px 0 rgba(255,255,255,0.12);
        }

        .timeline-card {
            background: linear-gradient(180deg, #fffaf1 0%, #f8f1e5 100%);
            border: 1px solid var(--line);
            border-left: 8px solid var(--accent);
            border-radius: 22px;
            padding: 0.95rem 1rem;
            margin-bottom: 0.8rem;
        }

        .timeline-team {
            color: var(--ink);
            font-size: 1.08rem;
            font-weight: 700;
        }

        .timeline-meta {
            color: var(--muted);
            margin-top: 0.25rem;
        }

        .stButton > button, .stDownloadButton > button {
            background: linear-gradient(135deg, var(--accent) 0%, var(--accent-deep) 100%);
            color: #fff9ef;
            border: none;
            border-radius: 999px;
            font-weight: 600;
            padding: 0.55rem 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def normalize_team_name(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    extra_aliases = {
        "MÃ¶venpickje FC": "M\u00f6venpickje FC",
        "MÃƒÂ¶venpickje FC": "M\u00f6venpickje FC",
        "MÃƒÆ’Ã‚Â¶venpickje FC": "M\u00f6venpickje FC",
        "LÃ¡szlÃ³ HofnÃ¡r": "L\u00e1szl\u00f3 Hofn\u00e1r",
        "LÃƒÂ¡szlÃƒÂ³ HofnÃƒÂ¡r": "L\u00e1szl\u00f3 HofnÃ¡r",
        "MOV": "M\u00f6venpickje FC",
        "LHO": "L\u00e1szl\u00f3 HofnÃ¡r",
    }
    return extra_aliases.get(TEAM_NAME_ALIASES.get(cleaned, cleaned), TEAM_NAME_ALIASES.get(cleaned, cleaned))


def team_color(team_name: str | None) -> str:
    normalized = normalize_team_name(team_name) or "UNKNOWN"
    return TEAM_COLORS.get(normalized, "#546e7a")


def rgba_from_hex(hex_color: str, alpha: float) -> str:
    value = hex_color.lstrip("#")
    red = int(value[0:2], 16)
    green = int(value[2:4], 16)
    blue = int(value[4:6], 16)
    return f"rgba({red}, {green}, {blue}, {alpha})"


@st.cache_resource
def get_connection(db_path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


@st.cache_data(show_spinner=False)
def run_query(db_path: str, query: str, params: tuple = ()) -> pd.DataFrame:
    connection = get_connection(db_path)
    return pd.read_sql_query(query, connection, params=params)


@st.cache_data(show_spinner=False)
def load_player_gameweek_base(db_path: str) -> pd.DataFrame:
    query = """
    SELECT
        pg.player_id,
        p.player_name,
        pg.season_slug,
        pg.gameweek,
        pg.position,
        pg.games_played,
        pg.score,
        pg.premier_league_team_code,
        COALESCE(pg.fantrax_team_name, pg.status_snapshot) AS fantrax_team_name,
        g.date_range_label,
        COALESCE(g.is_playoff, 0) AS is_playoff
    FROM player_gameweek pg
    JOIN player p
        ON p.player_id = pg.player_id
    LEFT JOIN gameweek g
        ON g.season_slug = pg.season_slug
       AND g.gameweek = pg.gameweek
    """
    df = run_query(db_path, query)
    if df.empty:
        return df
    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name)
    df["season_gameweek_label"] = (
        df["season_slug"] + " GW" + df["gameweek"].astype(int).astype(str).str.zfill(2)
    )
    df["played_flag"] = (df["games_played"].fillna(0) > 0).astype(int)
    return df


@st.cache_data(show_spinner=False)
def load_player_gameweek_stats(db_path: str) -> pd.DataFrame:
    query = """
    SELECT
        pg.player_id,
        p.player_name,
        pg.season_slug,
        pg.gameweek,
        pg.position,
        pg.games_played,
        pg.score,
        pg.premier_league_team_code,
        COALESCE(pg.fantrax_team_name, pg.status_snapshot) AS fantrax_team_name,
        g.date_range_label,
        COALESCE(g.is_playoff, 0) AS is_playoff,
        pgs.stat_key,
        pgs.stat_value
    FROM player_gameweek_stat pgs
    JOIN player_gameweek pg
        ON pg.player_id = pgs.player_id
       AND pg.season_slug = pgs.season_slug
       AND pg.gameweek = pgs.gameweek
    JOIN player p
        ON p.player_id = pg.player_id
    LEFT JOIN gameweek g
        ON g.season_slug = pg.season_slug
       AND g.gameweek = pg.gameweek
    WHERE pgs.stat_value IS NOT NULL
    """
    df = run_query(db_path, query)
    if df.empty:
        return df
    df = df[~df["stat_key"].str.upper().isin(IGNORED_STATS)].copy()
    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name)
    df["season_gameweek_label"] = (
        df["season_slug"] + " GW" + df["gameweek"].astype(int).astype(str).str.zfill(2)
    )
    df["played_flag"] = (df["games_played"].fillna(0) > 0).astype(int)
    return df


@st.cache_data(show_spinner=False)
def load_transactions_df(db_path: str) -> pd.DataFrame:
    query = """
    SELECT
        season_slug,
        gameweek,
        player_name,
        premier_league_team_code,
        position,
        transaction_type,
        fantrax_team_name,
        bid_amount,
        priority_value,
        transaction_datetime_text
    FROM transaction_event
    """
    df = run_query(db_path, query)
    if df.empty:
        return df
    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")
    df["bid_amount"] = df["bid_amount"].fillna(0)
    return df


@st.cache_data(show_spinner=False)
def load_trades_df(db_path: str) -> pd.DataFrame:
    query = """
    SELECT
        season_slug,
        gameweek,
        player_name,
        premier_league_team_code,
        position,
        from_fantrax_team_name,
        to_fantrax_team_name,
        transaction_datetime_text
    FROM trade_event
    """
    df = run_query(db_path, query)
    if df.empty:
        return df
    df["from_fantrax_team_name"] = df["from_fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")
    df["to_fantrax_team_name"] = df["to_fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")
    return df


@st.cache_data(show_spinner=False)
def load_draft_events_df(db_path: str) -> pd.DataFrame:
    query = """
    SELECT
        season_slug,
        round_number,
        pick_in_round,
        overall_pick,
        fantrax_team_name,
        player_id,
        player_name,
        position,
        premier_league_team_code
    FROM draft_event
    """
    df = run_query(db_path, query)
    if df.empty:
        return df
    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")
    return df


@st.cache_data(show_spinner=False)
def load_matchup_results_df(db_path: str) -> pd.DataFrame:
    query = """
    SELECT
        season_slug,
        gameweek,
        is_playoff,
        team_a_name AS fantrax_team_name,
        team_b_name AS opponent_name,
        team_a_points AS matchup_points_for,
        team_b_points AS matchup_points_against,
        team_a_wins AS category_wins_for,
        team_a_losses AS category_wins_against
    FROM matchup
    UNION ALL
    SELECT
        season_slug,
        gameweek,
        is_playoff,
        team_b_name AS fantrax_team_name,
        team_a_name AS opponent_name,
        team_b_points AS matchup_points_for,
        team_a_points AS matchup_points_against,
        team_b_wins AS category_wins_for,
        team_b_losses AS category_wins_against
    FROM matchup
    """
    df = run_query(db_path, query)
    if df.empty:
        return df
    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name)
    df["opponent_name"] = df["opponent_name"].map(normalize_team_name)
    df["league_points"] = 0
    df.loc[df["matchup_points_for"] > df["matchup_points_against"], "league_points"] = 3
    df.loc[df["matchup_points_for"] == df["matchup_points_against"], "league_points"] = 1
    return df


@st.cache_data(show_spinner=False)
def load_matchup_stat_team_df(db_path: str) -> pd.DataFrame:
    query = """
    SELECT
        m.season_slug,
        m.gameweek,
        m.is_playoff,
        m.team_a_name,
        m.team_b_name,
        msr.stat_key,
        msr.team_a_value,
        msr.team_b_value,
        msr.winner_side
    FROM matchup_stat_result msr
    JOIN matchup m
        ON m.matchup_id = msr.matchup_id
    """
    raw = run_query(db_path, query)
    if raw.empty:
        return raw
    team_a = raw.rename(
        columns={
            "team_a_name": "fantrax_team_name",
            "team_b_name": "opponent_name",
            "team_a_value": "stat_value_for",
            "team_b_value": "stat_value_against",
        }
    ).copy()
    team_a["result"] = team_a["winner_side"].map({"A": "W", "B": "L", "T": "D"})
    team_b = raw.rename(
        columns={
            "team_b_name": "fantrax_team_name",
            "team_a_name": "opponent_name",
            "team_b_value": "stat_value_for",
            "team_a_value": "stat_value_against",
        }
    ).copy()
    team_b["result"] = team_b["winner_side"].map({"B": "W", "A": "L", "T": "D"})
    df = pd.concat(
        [
            team_a[["season_slug", "gameweek", "is_playoff", "fantrax_team_name", "opponent_name", "stat_key", "stat_value_for", "stat_value_against", "result"]],
            team_b[["season_slug", "gameweek", "is_playoff", "fantrax_team_name", "opponent_name", "stat_key", "stat_value_for", "stat_value_against", "result"]],
        ],
        ignore_index=True,
    )
    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name)
    df["opponent_name"] = df["opponent_name"].map(normalize_team_name)
    return df


@st.cache_data(show_spinner=False)
def load_draft_value_df(db_path: str) -> pd.DataFrame:
    query = """
    WITH drops AS (
        SELECT
            season_slug,
            player_name,
            MIN(gameweek) AS dropped_in_gw
        FROM transaction_event
        WHERE transaction_type = 'Drop'
        GROUP BY season_slug, player_name
    ),
    scored AS (
        SELECT
            d.season_slug,
            d.round_number,
            d.pick_in_round,
            d.overall_pick,
            d.fantrax_team_name,
            d.player_id,
            d.player_name,
            d.position,
            d.premier_league_team_code,
            ps.score AS drafted_score,
            drops.dropped_in_gw
        FROM draft_event d
        LEFT JOIN player_season ps
            ON ps.player_id = d.player_id
           AND ps.season_slug = d.season_slug
        LEFT JOIN drops
            ON drops.season_slug = d.season_slug
           AND drops.player_name = d.player_name
    ),
    outfield_ranked AS (
        SELECT
            season_slug,
            overall_pick,
            RANK() OVER (
                PARTITION BY season_slug
                ORDER BY drafted_score DESC
            ) AS score_rank
        FROM scored
        WHERE INSTR(COALESCE(position, ''), 'G') = 0
          AND drafted_score IS NOT NULL
    )
    SELECT
        s.season_slug,
        s.round_number,
        s.pick_in_round,
        s.overall_pick,
        s.fantrax_team_name,
        s.player_id,
        s.player_name,
        s.position,
        s.premier_league_team_code,
        s.drafted_score,
        o.score_rank,
        s.dropped_in_gw,
        CASE
            WHEN o.score_rank IS NULL THEN NULL
            ELSE s.overall_pick - o.score_rank
        END AS draft_value,
        CASE WHEN INSTR(COALESCE(s.position, ''), 'G') > 0 THEN 1 ELSE 0 END AS is_goalkeeper
    FROM scored s
    LEFT JOIN outfield_ranked o
        ON o.season_slug = s.season_slug
       AND o.overall_pick = s.overall_pick
    """
    df = run_query(db_path, query)
    if df.empty:
        return df
    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")
    return df


def metric_card(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def navigate_to(page_name: str) -> None:
    st.session_state["page_radio"] = page_name
    st.rerun()


def navigate_to_record(section: str, title: str, detail: str) -> None:
    st.session_state["record_focus"] = {
        "section": section,
        "title": title,
        "detail": detail,
    }
    navigate_to("Records")


def render_header() -> None:
    st.markdown(
        """
        <div class="hero">
            <h1>Gerrie Senden Bokaal-dashboard</h1>
            <p>
                Hoe je nóg meer tijd kwijt kunt zijn aan dat kutspel
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_team_columns(df: pd.DataFrame, columns: list[str]) -> pd.io.formats.style.Styler:
    def apply_style(value: object) -> str:
        if pd.isna(value):
            return ""
        color = team_color(str(value))
        return f"background-color: {rgba_from_hex(color, 0.16)}; color: #11211c; font-weight: 600;"

    styler = df.style
    for column in columns:
        if column in df.columns:
            styler = styler.map(apply_style, subset=[column])
    return styler


def format_columns(df: pd.DataFrame) -> pd.DataFrame:
    formatted = df.copy()
    formatted.columns = [DISPLAY_LABELS.get(column, humanize_label(column)) for column in formatted.columns]
    return formatted


def humanize_label(column: object) -> str:
    text = str(column)
    if not text:
        return text
    text = text.replace("_", " ").strip()
    replacements = {
        "gw": "GW",
        "ppg": "PPG",
        "pl": "PL",
        "xi": "XI",
        "id": "ID",
    }
    words = []
    for word in text.split():
        lower = word.lower()
        if lower in replacements:
            words.append(replacements[lower])
        elif word.isupper():
            words.append(word)
        else:
            words.append(lower.capitalize())
    return " ".join(words)


def format_values(df: pd.DataFrame) -> pd.DataFrame:
    formatted = df.copy()
    for column in formatted.columns:
        series = formatted[column]
        candidate = series
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            non_null = series.dropna()
            if non_null.empty:
                continue
            as_text = non_null.astype(str).str.strip()
            numeric_like = as_text.str.fullmatch(r"[-+]?\d+(?:[.,]\d+)?")
            if numeric_like.all():
                candidate = pd.to_numeric(series.astype(str).str.replace(",", ".", regex=False), errors="coerce")

        if pd.api.types.is_numeric_dtype(candidate):
            non_null_numeric = candidate.dropna()
            if non_null_numeric.empty:
                continue
            if ((non_null_numeric % 1).abs() < 1e-9).all():
                formatted[column] = candidate.round(0).astype("Int64")
            else:
                decimals = NUMERIC_DECIMALS.get(str(column), 2)
                formatted[column] = candidate.round(decimals)
    return formatted


def pretty_df(df: pd.DataFrame) -> pd.DataFrame:
    return format_columns(format_values(df))


def strip_trailing_zero_text(value: object, decimals: int = 2) -> object:
    if pd.isna(value):
        return value
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if abs(number - round(number)) < 1e-9:
        return int(round(number))
    return f"{number:.{decimals}f}".rstrip("0").rstrip(".").replace(".", ",")


def format_decimal_text(value: object, decimals: int = 1) -> str | None:
    if pd.isna(value):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:.{decimals}f}".replace(".", ",")


def primary_position_from_value(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return "UNK"
    return text.split("|")[0].split(",")[0].strip() or "UNK"


def render_radar_chart(rose_df: pd.DataFrame, title: str) -> None:
    if rose_df.empty:
        return
    labels = rose_df["stat_key"].tolist()
    values = rose_df["percentile"].astype(float).tolist()
    angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
    values += values[:1]
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw={"polar": True})
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylim(0, 100)
    ax.set_yticks([20, 40, 60, 80, 100])
    ax.set_yticklabels(["20", "40", "60", "80", "100"], fontsize=8)
    ax.plot(angles, values, color="#0f6b53", linewidth=2.4)
    ax.fill(angles, values, color="#2a9d8f", alpha=0.35)
    ax.grid(color="#b7b0a2", alpha=0.55)
    ax.set_title(title, y=1.08, fontsize=13)
    st.pyplot(fig, use_container_width=True)
    plt.close(fig)


def render_grouped_quality_chart(quality_df: pd.DataFrame) -> None:
    if quality_df.empty:
        return
    pivoted = (
        quality_df.pivot(index="fantrax_team_name", columns="source", values="avg_score")
        .fillna(0)
    )
    pivoted["sort_value"] = pivoted.mean(axis=1)
    pivoted = pivoted.sort_values("sort_value", ascending=False).drop(columns=["sort_value"])
    teams = pivoted.index.tolist()
    drafted = pivoted.get("Drafted players", pd.Series(index=teams, dtype=float)).fillna(0).tolist()
    claimed = pivoted.get("Claimed players", pd.Series(index=teams, dtype=float)).fillna(0).tolist()

    min_score = min(drafted + claimed) if teams else 0.0
    max_score = max(drafted + claimed) if teams else 1.0
    lower = max(0.0, np.floor((min_score - 2) / 5) * 5)
    upper = np.ceil((max_score + 2) / 5) * 5

    x = np.arange(len(teams))
    width = 0.36
    fig, ax = plt.subplots(figsize=(12, 4.8))
    drafted_bars = ax.bar(x - width / 2, drafted, width, label="Drafted players", color="#d9822b")
    claimed_bars = ax.bar(x + width / 2, claimed, width, label="Claimed players", color="#0f6b53")
    ax.set_ylabel("Avg score")
    ax.set_xlabel("Fantrax team")
    ax.set_ylim(lower, upper)
    ax.set_xticks(x)
    ax.set_xticklabels(teams, rotation=40, ha="right")
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.25)
    ax.bar_label(drafted_bars, padding=3, fmt="%.1f", fontsize=9)
    ax.bar_label(claimed_bars, padding=3, fmt="%.1f", fontsize=9)
    fig.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close(fig)


def render_ownership_timeline_matplotlib(ownership_df: pd.DataFrame) -> None:
    if ownership_df.empty:
        return
    fig, ax = plt.subplots(figsize=(12, 4.6))
    y_pos = 10
    tick_positions: list[float] = []
    tick_labels: list[str] = []

    for row in ownership_df.itertuples(index=False):
        start = float(row.start_index)
        width = max(float(row.end_index - row.start_index + 1), 0.8)
        color = team_color(row.fantrax_team_name)
        ax.broken_barh([(start, width)], (y_pos, 7), facecolors=color, edgecolors="none", alpha=0.9)
        tick_positions.extend([start, start + width - 1])
        tick_labels.extend([str(row.first_gw), str(row.last_gw)])

    ax.set_ylim(5, 20)
    ax.set_yticks([])
    ax.set_ylabel("")
    ax.set_xlabel("Jaar-GW")
    ax.set_title("Ownership history", loc="left")
    ordered_ticks = []
    seen = set()
    for pos, label in zip(tick_positions, tick_labels):
        key = (round(pos, 3), label)
        if key not in seen:
            seen.add(key)
            ordered_ticks.append((pos, label))
    ax.set_xticks([pos for pos, _ in ordered_ticks])
    ax.set_xticklabels([label for _, label in ordered_ticks], rotation=40, ha="right")
    ax.grid(axis="x", alpha=0.2)
    legend_handles = []
    for team in ownership_df["fantrax_team_name"].dropna().unique().tolist():
        legend_handles.append(plt.Rectangle((0, 0), 1, 1, color=team_color(team), label=team))
    ax.legend(handles=legend_handles, title="Fantrax team", frameon=False, bbox_to_anchor=(1.01, 1), loc="upper left")
    fig.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close(fig)


def render_formation_pitch(defs: list[str], mids: list[str], fwds: list[str], gk: list[str], team_name: str) -> None:
    def row(players: list[str]) -> str:
        chips = "".join(
            f'<span class="team-chip" style="background:{team_color(team_name)}; margin: 0 1.15rem 0.85rem 1.15rem; padding: 0.48rem 1.05rem;">{player}</span>'
            for player in players
        )
        return f"<div style='text-align:center; margin:1.5rem 0'>{chips}</div>"

    html = """
    <div style="
        background: linear-gradient(180deg, #3f925d 0%, #2f7d4b 100%);
        border: 3px solid rgba(255,255,255,0.75);
        border-radius: 28px;
        padding: 1.7rem 1.35rem 1.95rem 1.35rem;
        box-shadow: inset 0 0 0 2px rgba(255,255,255,0.15);
        margin-top: 0.6rem;
        position: relative;
    ">
    """
    for players in [fwds, mids, defs, gk]:
        if players:
            html += row(players)
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def render_best_xi_pitch(lineup_df: pd.DataFrame, formation_name: str, team_name: str = "NOAD Athletic") -> None:
    lineup = lineup_df.copy()
    lineup["primary_position"] = lineup["positions"].map(primary_position_from_value)
    lineup = lineup.sort_values(["score_percentile", "avg_score", "player_name"], ascending=[False, False, True])

    def pick_players(candidates: pd.DataFrame, amount: int) -> list[str]:
        return candidates["player_name"].head(amount).tolist()

    gk = pick_players(lineup[lineup["primary_position"].str.contains("G", na=False)], 1)
    def_count, mid_count, fwd_count = FORMATION_MAP[formation_name]
    defs = pick_players(lineup[lineup["primary_position"].str.contains("D", na=False)], def_count)
    mids = pick_players(lineup[lineup["primary_position"].str.contains("M", na=False)], mid_count)
    fwds = pick_players(lineup[lineup["primary_position"].str.contains("F", na=False)], fwd_count)
    render_formation_pitch(defs, mids, fwds, gk, team_name)


def build_streak_rows(
    df: pd.DataFrame,
    entity_cols: list[str],
    result_col: str,
    mode_name: str,
    predicate,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    working = df.sort_values(entity_cols + ["season_slug", "gameweek"]).copy()
    rows: list[dict[str, object]] = []
    for entity_values, group in working.groupby(entity_cols, dropna=False):
        if not isinstance(entity_values, tuple):
            entity_values = (entity_values,)
        start_label = None
        end_label = None
        length = 0
        for row in group.itertuples(index=False):
            result = getattr(row, result_col)
            label = f"{row.season_slug} GW{int(row.gameweek):02d}"
            if predicate(result):
                if length == 0:
                    start_label = label
                end_label = label
                length += 1
            else:
                if length > 0:
                    record = {col: value for col, value in zip(entity_cols, entity_values)}
                    record.update(
                        {
                            "streak_type": mode_name,
                            "streak_length": length,
                            "start_gw": start_label,
                            "end_gw": end_label,
                        }
                    )
                    rows.append(record)
                start_label = None
                end_label = None
                length = 0
        if length > 0:
            record = {col: value for col, value in zip(entity_cols, entity_values)}
            record.update(
                {
                    "streak_type": mode_name,
                    "streak_length": length,
                    "start_gw": start_label,
                    "end_gw": end_label,
                }
            )
            rows.append(record)
    return pd.DataFrame(rows)


def get_overview_metrics(db_path: str) -> dict[str, int]:
    query = """
    SELECT
        (SELECT COUNT(*) FROM player) AS players,
        (SELECT COUNT(*) FROM season) AS seasons,
        (SELECT COUNT(*) FROM matchup) AS matchups,
        (SELECT COUNT(*) FROM player_gameweek) AS player_gameweeks,
        (SELECT COUNT(*) FROM transaction_event) AS transactions,
        (SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'trade_event') AS has_trade_table
    """
    row = run_query(db_path, query).iloc[0].to_dict()
    trade_count = 0
    if row.get("has_trade_table"):
        trade_count = int(run_query(db_path, "SELECT COUNT(*) AS c FROM trade_event").iloc[0]["c"])
    return {
        "players": int(row["players"]),
        "seasons": int(row["seasons"]),
        "matchups": int(row["matchups"]),
        "player_gameweeks": int(row["player_gameweeks"]),
        "transactions": int(row["transactions"]),
        "trades": trade_count,
    }


def render_overview(db_path: str) -> None:
    metrics = get_overview_metrics(db_path)
    cols = st.columns(6)
    cards = [
        ("Players", f"{metrics['players']:,}"),
        ("Seasons", f"{metrics['seasons']:,}"),
        ("Matchups", f"{metrics['matchups']:,}"),
        ("Player GWs", f"{metrics['player_gameweeks']:,}"),
        ("Transactions", f"{metrics['transactions']:,}"),
        ("Trades", f"{metrics['trades']:,}"),
    ]
    for col, (label, value) in zip(cols, cards):
        with col:
            metric_card(label, value)

    top_journeyman_query = """
    WITH player_team_counts AS (
        SELECT
            pg.player_id,
            p.player_name,
            pg.fantrax_team_name,
            COUNT(*) AS gameweeks_played
        FROM player_gameweek pg
        JOIN player p ON p.player_id = pg.player_id
        WHERE pg.fantrax_team_name IS NOT NULL
          AND pg.fantrax_team_name NOT IN ('', 'FA', 'UNKNOWN', 'None/Bye')
          AND COALESCE(pg.games_played, 0) > 0
        GROUP BY pg.player_id, p.player_name, pg.fantrax_team_name
    )
    SELECT
        player_name,
        COUNT(*) AS fantrax_teams_played_for,
        SUM(gameweeks_played) AS total_gameweeks_played
    FROM player_team_counts
    GROUP BY player_id, player_name
    ORDER BY fantrax_teams_played_for DESC, total_gameweeks_played DESC, player_name
    LIMIT 1
    """
    strongest_team_query = """
    SELECT
        team_name,
        standings_points,
        matches_played
    FROM v_all_time_team_standings_regular_season
    ORDER BY standings_points DESC, matchup_wins DESC, total_matchup_points_for DESC
    LIMIT 1
    """
    best_ppg_query = """
    SELECT
        team_name,
        ppg,
        matches_played
    FROM v_all_time_team_standings_regular_season
    ORDER BY ppg DESC, standings_points DESC, team_name
    LIMIT 1
    """
    best_team_score_query = """
    SELECT
        fantrax_team_name,
        opponent_name,
        season_slug,
        gameweek,
        matchup_points_for
    FROM (
        SELECT
            team_a_name AS fantrax_team_name,
            team_b_name AS opponent_name,
            season_slug,
            gameweek,
            team_a_points AS matchup_points_for
        FROM matchup
        WHERE team_b_name <> 'None/Bye'
        UNION ALL
        SELECT
            team_b_name AS fantrax_team_name,
            team_a_name AS opponent_name,
            season_slug,
            gameweek,
            team_b_points AS matchup_points_for
        FROM matchup
        WHERE team_a_name <> 'None/Bye'
    )
    ORDER BY matchup_points_for DESC, season_slug, gameweek
    LIMIT 1
    """
    most_trades_query = """
    SELECT
        team_name,
        SUM(trades) AS trades
    FROM (
        SELECT from_fantrax_team_name AS team_name, COUNT(*) AS trades
        FROM trade_event
        GROUP BY from_fantrax_team_name
        UNION ALL
        SELECT to_fantrax_team_name AS team_name, COUNT(*) AS trades
        FROM trade_event
        GROUP BY to_fantrax_team_name
    )
    GROUP BY team_name
    ORDER BY trades DESC, team_name
    LIMIT 1
    """

    lower = st.columns(3)
    journeyman = run_query(db_path, top_journeyman_query)
    strongest = run_query(db_path, strongest_team_query)
    best_ppg = run_query(db_path, best_ppg_query)
    best_team_score = run_query(db_path, best_team_score_query)
    most_trades = run_query(db_path, most_trades_query)

    with lower[0]:
        if not strongest.empty:
            row = strongest.iloc[0]
            st.markdown(
                f"""
                <div class="journeyman-card">
                    <div class="journeyman-title">{normalize_team_name(row['team_name'])}</div>
                    <div class="journeyman-meta">All-time leader</div>
                    <div>{int(row['standings_points'])} standings points in {int(row['matches_played'])} matches</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    with lower[1]:
        if not best_ppg.empty:
            row = best_ppg.iloc[0]
            st.markdown(
                f"""
                <div class="journeyman-card">
                    <div class="journeyman-title">{normalize_team_name(row['team_name'])}</div>
                    <div class="journeyman-meta">Best PPG</div>
                    <div>{strip_trailing_zero_text(row['ppg'])} PPG over {int(row['matches_played'])} matches</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    with lower[2]:
        if not journeyman.empty:
            row = journeyman.iloc[0]
            st.markdown(
                f"""
                <div class="journeyman-card">
                    <div class="journeyman-title">{row['player_name']}</div>
                    <div class="journeyman-meta">Top journeyman</div>
                    <div>{int(row['fantrax_teams_played_for'])} teams • {int(row['total_gameweeks_played'])} GW</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    extras = st.columns(2)
    with extras[0]:
        if not best_team_score.empty:
            row = best_team_score.iloc[0]
            st.markdown(
                f"""
                <div class="journeyman-card">
                    <div class="journeyman-title">{normalize_team_name(row['fantrax_team_name'])}</div>
                    <div class="journeyman-meta">Highest team score</div>
                    <div>{strip_trailing_zero_text(row['matchup_points_for'])} vs {normalize_team_name(row['opponent_name'])} in {row['season_slug']} GW{int(row['gameweek']):02d}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
    with extras[1]:
        if not most_trades.empty:
            row = most_trades.iloc[0]
            st.markdown(
                f"""
                <div class="journeyman-card">
                    <div class="journeyman-title">{normalize_team_name(row['team_name'])}</div>
                    <div class="journeyman-meta">Most active trader</div>
                    <div>{int(row['trades'])} trade involvements</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_all_time_standings(db_path: str) -> None:
    st.subheader("All-Time Team Standings")
    query = """
    SELECT
        team_name,
        matches_played,
        standings_points,
        matchup_wins,
        matchup_draws,
        matchup_losses,
        total_matchup_points_for,
        total_matchup_points_against,
        total_category_wins,
        total_category_losses,
        total_category_ties
    FROM v_all_time_team_standings_regular_season
    ORDER BY
        standings_points DESC,
        matchup_wins DESC,
        total_matchup_points_for DESC,
        team_name
    """
    df = run_query(db_path, query)
    for column in ["total_matchup_points_for", "total_matchup_points_against"]:
        if column in df.columns:
            df[column] = df[column].round(0).astype("Int64")
    display_df = pretty_df(df)
    st.dataframe(style_team_columns(display_df, ["Team"]), use_container_width=True, hide_index=True)


def build_team_options(base_df: pd.DataFrame) -> list[str]:
    excluded = {"FA", "UNKNOWN", "None/Bye"}
    teams = sorted(
        team
        for team in base_df["fantrax_team_name"].dropna().unique().tolist()
        if team and team not in excluded
    )
    return teams


def build_player_options(base_df: pd.DataFrame) -> list[str]:
    return sorted(base_df["player_name"].dropna().unique().tolist())


def apply_common_filters(
    df: pd.DataFrame,
    seasons: list[str],
    fantrax_teams: list[str],
    premier_teams: list[str],
    positions: list[str],
    include_playoffs: bool,
) -> pd.DataFrame:
    filtered = df.copy()
    if seasons:
        filtered = filtered[filtered["season_slug"].isin(seasons)]
    if fantrax_teams:
        filtered = filtered[filtered["fantrax_team_name"].isin(fantrax_teams)]
    if premier_teams:
        filtered = filtered[filtered["premier_league_team_code"].isin(premier_teams)]
    if positions:
        pattern = "|".join(positions)
        filtered = filtered[filtered["position"].fillna("").str.contains(pattern, regex=True)]
    if not include_playoffs and "is_playoff" in filtered.columns:
        filtered = filtered[filtered["is_playoff"] == 0]
    return filtered


def stat_aggregation_mode(stat_key: str) -> str:
    if stat_key == "Score":
        return "mean"
    if "%" in stat_key:
        return "mean"
    return "sum"


def aggregate_stat_records(df: pd.DataFrame, stat_key: str, scope: str) -> pd.DataFrame:
    working = df.copy()
    agg_mode = stat_aggregation_mode(stat_key)

    if scope == "Gameweek":
        if stat_key == "Score":
            result = (
                working.groupby(
                    [
                        "player_name",
                        "season_slug",
                        "gameweek",
                        "season_gameweek_label",
                        "date_range_label",
                        "fantrax_team_name",
                        "premier_league_team_code",
                        "position",
                    ],
                    dropna=False,
                )["score"]
                .mean()
                .reset_index(name="stat_value")
            )
        else:
            result = (
                working.groupby(
                    [
                        "player_name",
                        "season_slug",
                        "gameweek",
                        "season_gameweek_label",
                        "date_range_label",
                        "fantrax_team_name",
                        "premier_league_team_code",
                        "position",
                    ],
                    dropna=False,
                )["stat_value"]
                .sum()
                .reset_index()
            )
        result["games_played"] = 1
        return result

    if scope == "Season":
        value_column = "score" if stat_key == "Score" else "stat_value"
        grouped = (
            working.groupby(["player_name", "season_slug"], dropna=False)
            .agg(
                stat_value=(value_column, "mean" if agg_mode == "mean" else "sum"),
                gameweeks_played=("played_flag", "sum"),
                fantrax_teams=("fantrax_team_name", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
                premier_league_teams=(
                    "premier_league_team_code",
                    lambda s: " | ".join(sorted({x for x in s.dropna() if x})),
                ),
                positions=("position", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
            )
            .reset_index()
        )
        return grouped

    value_column = "score" if stat_key == "Score" else "stat_value"
    grouped = (
        working.groupby(["player_name"], dropna=False)
        .agg(
            stat_value=(value_column, "mean" if agg_mode == "mean" else "sum"),
            gameweeks_played=("played_flag", "sum"),
            seasons=("season_slug", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
            fantrax_teams=("fantrax_team_name", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
            premier_league_teams=(
                "premier_league_team_code",
                lambda s: " | ".join(sorted({x for x in s.dropna() if x})),
            ),
            positions=("position", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
        )
        .reset_index()
    )
    return grouped


def render_stat_explorer(db_path: str) -> None:
    st.subheader("Player Stat Explorer")
    base_df = load_player_gameweek_base(db_path)
    stats_df = load_player_gameweek_stats(db_path)
    stat_options = ["Score"] + sorted(stats_df["stat_key"].dropna().unique().tolist())

    top_controls = st.columns([1.1, 1.1, 1.1, 1.1])
    with top_controls[0]:
        stat_key = st.selectbox("Categorie / stat", stat_options)
    with top_controls[1]:
        scope = st.selectbox("Niveau", ["Gameweek", "Season", "Career"])
    with top_controls[2]:
        sort_direction = st.selectbox("Sortering", ["Hoog naar laag", "Laag naar hoog"])
    with top_controls[3]:
        show_all = st.toggle("Toon alle resultaten", value=True)

    filter_cols = st.columns(6)
    seasons = sorted(base_df["season_slug"].dropna().unique().tolist())
    teams = build_team_options(base_df)
    pl_teams = sorted(base_df["premier_league_team_code"].dropna().unique().tolist())
    positions = sorted(
        {
            part
            for value in base_df["position"].dropna().tolist()
            for part in [item.strip() for item in str(value).split(",")]
            if part
        }
    )
    with filter_cols[0]:
        selected_seasons = st.multiselect("Seizoen", seasons, default=[])
    with filter_cols[1]:
        selected_fantrax_teams = st.multiselect("Fantrax-team", teams, default=[])
    with filter_cols[2]:
        selected_pl_teams = st.multiselect("PL-team", pl_teams, default=[])
    with filter_cols[3]:
        selected_positions = st.multiselect("Positie", positions, default=[])
    with filter_cols[4]:
        include_playoffs = st.toggle("Playoffs meenemen", value=False)

    player_search = st.text_input("Zoek speler", value="", placeholder="Bijvoorbeeld: Pedro, Son, Watkins")

    source_df = base_df if stat_key == "Score" else stats_df[stats_df["stat_key"] == stat_key].copy()
    filtered_df = apply_common_filters(
        source_df,
        selected_seasons,
        selected_fantrax_teams,
        selected_pl_teams,
        selected_positions,
        include_playoffs,
    )
    if player_search.strip():
        filtered_df = filtered_df[
            filtered_df["player_name"].str.contains(player_search.strip(), case=False, na=False)
        ]

    result = aggregate_stat_records(filtered_df, stat_key, scope)

    all_stats_source = apply_common_filters(
        stats_df,
        selected_seasons,
        selected_fantrax_teams,
        selected_pl_teams,
        selected_positions,
        include_playoffs,
    )
    if player_search.strip():
        all_stats_source = all_stats_source[
            all_stats_source["player_name"].str.contains(player_search.strip(), case=False, na=False)
        ]
    if not all_stats_source.empty:
        if scope == "Gameweek":
            key_cols = [
                "player_name",
                "season_slug",
                "gameweek",
                "season_gameweek_label",
                "date_range_label",
                "fantrax_team_name",
                "premier_league_team_code",
                "position",
            ]
            all_stats_wide = (
                all_stats_source.pivot_table(
                    index=key_cols,
                    columns="stat_key",
                    values="stat_value",
                    aggfunc="sum",
                )
                .reset_index()
            )
        elif scope == "Season":
            all_stats_wide = (
                all_stats_source.pivot_table(
                    index=["player_name", "season_slug"],
                    columns="stat_key",
                    values="stat_value",
                    aggfunc="sum",
                )
                .reset_index()
            )
        else:
            all_stats_wide = (
                all_stats_source.pivot_table(
                    index=["player_name"],
                    columns="stat_key",
                    values="stat_value",
                    aggfunc="sum",
                )
                .reset_index()
            )
        if stat_key != "Score" and stat_key in all_stats_wide.columns:
            all_stats_wide = all_stats_wide.drop(columns=[stat_key])
        join_cols = [col for col in all_stats_wide.columns if col in result.columns and col != "stat_value"]
        value_cols = [col for col in all_stats_wide.columns if col not in join_cols]
        if value_cols:
            result = result.merge(all_stats_wide, on=join_cols, how="left")
    ascending = sort_direction == "Laag naar hoog"
    if "stat_value" in result.columns:
        result = result.sort_values(["stat_value", "player_name"], ascending=[ascending, True])
    if not show_all:
        max_rows = st.slider("Aantal rijen in view", 25, 500, 100, 25, key="stat_explorer_limit")
        result = result.head(max_rows)

    display_df = result.copy()
    if "stat_value" in display_df.columns:
        display_df = display_df.rename(columns={"stat_value": stat_key})
    display_df = pretty_df(display_df)
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_ranking_history(db_path: str) -> None:
    st.subheader("All-Time Ranking Development")
    query = """
    WITH team_matchups AS (
        SELECT
            m.season_slug,
            m.gameweek,
            m.is_playoff,
            m.team_a_name AS team_name,
            CASE
                WHEN m.team_a_points > m.team_b_points THEN 3
                WHEN m.team_a_points = m.team_b_points THEN 1
                ELSE 0
            END AS standings_points_earned
        FROM matchup m

        UNION ALL

        SELECT
            m.season_slug,
            m.gameweek,
            m.is_playoff,
            m.team_b_name AS team_name,
            CASE
                WHEN m.team_b_points > m.team_a_points THEN 3
                WHEN m.team_b_points = m.team_a_points THEN 1
                ELSE 0
            END AS standings_points_earned
        FROM matchup m
    )
    SELECT
        season_slug,
        gameweek,
        is_playoff,
        team_name,
        standings_points_earned
    FROM team_matchups
    """
    df = run_query(db_path, query)
    if df.empty:
        st.info("Geen matchupdata gevonden.")
        return

    df["team_name"] = df["team_name"].map(normalize_team_name)
    include_playoffs = st.toggle("Gebruik playoffs in ranking history", value=False, key="ranking_playoffs")
    if not include_playoffs:
        df = df[df["is_playoff"] == 0]

    df["season_gameweek_label"] = (
        df["season_slug"] + " GW" + df["gameweek"].astype(int).astype(str).str.zfill(2)
    )
    df = df.sort_values(["season_slug", "gameweek", "team_name"])
    df["standings_points_earned"] = df["standings_points_earned"].astype(float)
    df["cumulative_points"] = df.groupby("team_name")["standings_points_earned"].cumsum()
    df["ranking"] = (
        df.groupby(["season_slug", "gameweek"])["cumulative_points"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    all_teams = sorted(df["team_name"].dropna().unique().tolist())
    default_teams = all_teams[:6]
    selected_teams = st.multiselect("Teams in grafiek", all_teams, default=default_teams)
    if selected_teams:
        df = df[df["team_name"].isin(selected_teams)]

    color_domain = all_teams
    color_range = [team_color(team) for team in color_domain]

    rank_chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X("season_gameweek_label:N", sort=None, title="Season / gameweek"),
            y=alt.Y("ranking:Q", scale=alt.Scale(reverse=True, domainMin=1), title="All-time rank"),
            color=alt.Color(
                "team_name:N",
                scale=alt.Scale(domain=color_domain, range=color_range),
                legend=alt.Legend(title="Fantrax team"),
            ),
            tooltip=[
                alt.Tooltip("team_name:N", title="Team"),
                alt.Tooltip("season_slug:N", title="Season"),
                alt.Tooltip("gameweek:Q", title="GW"),
                alt.Tooltip("ranking:Q", title="Rank"),
                alt.Tooltip("cumulative_points:Q", title="Cum. points"),
            ],
        )
        .properties(height=430)
        .interactive()
    )
    points_chart = (
        alt.Chart(df)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X("season_gameweek_label:N", sort=None, title="Season / gameweek"),
            y=alt.Y("cumulative_points:Q", title="Cumulative standings points"),
            color=alt.Color(
                "team_name:N",
                scale=alt.Scale(domain=color_domain, range=color_range),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("team_name:N", title="Team"),
                alt.Tooltip("season_slug:N", title="Season"),
                alt.Tooltip("gameweek:Q", title="GW"),
                alt.Tooltip("cumulative_points:Q", title="Cumulative points"),
                alt.Tooltip("ranking:Q", title="Rank"),
            ],
        )
        .properties(height=430)
        .interactive()
    )
    top, bottom = st.tabs(["Ranking", "Points"])
    with top:
        st.altair_chart(rank_chart, use_container_width=True)
    with bottom:
        st.altair_chart(points_chart, use_container_width=True)


def build_team_roster_table(base_df: pd.DataFrame, stats_df: pd.DataFrame, selected_team: str) -> pd.DataFrame:
    team_base = base_df[
        (base_df["fantrax_team_name"] == selected_team) & (base_df["played_flag"] > 0)
    ].copy()
    team_stats = stats_df[
        (stats_df["fantrax_team_name"] == selected_team) & (stats_df["played_flag"] > 0)
    ].copy()

    if team_base.empty:
        return pd.DataFrame()

    overview = (
        team_base.groupby(["player_id", "player_name"], dropna=False)
        .agg(
            gameweeks_played=("played_flag", "sum"),
            avg_score=("score", "mean"),
            seasons=("season_slug", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
            premier_league_teams=(
                "premier_league_team_code",
                lambda s: " | ".join(sorted({x for x in s.dropna() if x})),
            ),
            positions=("position", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
        )
        .reset_index()
    )

    stat_frames: list[pd.DataFrame] = []
    for stat_key, stat_group in team_stats.groupby("stat_key"):
        if str(stat_key).upper() in IGNORED_STATS:
            continue
        agg_mode = "mean" if "%" in stat_key else "sum"
        frame = (
            stat_group.groupby(["player_id"], dropna=False)["stat_value"]
            .agg(agg_mode)
            .reset_index()
            .rename(columns={"stat_value": stat_key})
        )
        stat_frames.append(frame)

    roster_df = overview.copy()
    for frame in stat_frames:
        roster_df = roster_df.merge(frame, on="player_id", how="left")

    return roster_df.sort_values(["gameweeks_played", "player_name"], ascending=[False, True])


def render_team_roster_section(
    selected_team: str,
    base_df: pd.DataFrame,
    stats_df: pd.DataFrame,
    selected_seasons: list[str] | None = None,
    selected_positions: list[str] | None = None,
) -> None:
    st.markdown("### Team Roster Archive")
    seasons = sorted(base_df["season_slug"].dropna().unique().tolist())
    positions = sorted(
        {
            part
            for value in base_df["position"].dropna().tolist()
            for part in [item.strip() for item in str(value).split(",")]
            if part
        }
    )
    roster_controls = st.columns(4)
    with roster_controls[0]:
        roster_seasons = st.multiselect(
            "Roster season filter",
            seasons,
            default=selected_seasons or [],
            key=f"roster_seasons_{selected_team}",
        )
    with roster_controls[1]:
        roster_positions = st.multiselect(
            "Roster position filter",
            positions,
            default=selected_positions or [],
            key=f"roster_positions_{selected_team}",
        )
    with roster_controls[2]:
        sort_column = st.text_input("Sorteer op kolom", value="gameweeks_played", key=f"roster_sort_col_{selected_team}")
    with roster_controls[3]:
        ascending = st.toggle("Oplopend sorteren", value=False, key=f"roster_sort_{selected_team}")

    filtered_base = apply_common_filters(
        base_df,
        roster_seasons,
        [selected_team],
        [],
        roster_positions,
        True,
    )
    filtered_stats = apply_common_filters(
        stats_df,
        roster_seasons,
        [selected_team],
        [],
        roster_positions,
        True,
    )

    roster_df = build_team_roster_table(filtered_base, filtered_stats, selected_team)
    if roster_df.empty:
        st.info("Geen spelers gevonden voor dit team met de huidige filters.")
        return

    if sort_column in roster_df.columns:
        roster_df = roster_df.sort_values(sort_column, ascending=ascending, na_position="last")

    roster_display = roster_df.drop(columns=["player_id"], errors="ignore")
    st.dataframe(pretty_df(roster_display), use_container_width=True, hide_index=True)

    formation_name = st.selectbox(
        "Formatie",
        list(FORMATION_MAP.keys()),
        index=2,
        key="roster_formation",
    )
    st.markdown(f"#### Best XI ({formation_name})")
    lineup = roster_df.copy()
    lineup["primary_position"] = lineup["positions"].map(primary_position_from_value)
    lineup = lineup.sort_values(["gameweeks_played", "avg_score", "player_name"], ascending=[False, False, True])

    def pick_players(candidates: pd.DataFrame, amount: int) -> list[str]:
        return candidates["player_name"].head(amount).tolist()

    gk = pick_players(lineup[lineup["primary_position"].str.contains("G", na=False)], 1)
    def_count, mid_count, fwd_count = FORMATION_MAP[formation_name]
    defs = pick_players(lineup[lineup["primary_position"].str.contains("D", na=False)], def_count)
    mids = pick_players(lineup[lineup["primary_position"].str.contains("M", na=False)], mid_count)
    fwds = pick_players(lineup[lineup["primary_position"].str.contains("F", na=False)], fwd_count)

    render_formation_pitch(defs, mids, fwds, gk, selected_team)


def render_journeymen(db_path: str) -> None:
    st.subheader("Journeymen")
    base_df = load_player_gameweek_base(db_path)
    limit = st.slider("Top spelers", 5, 25, 10, 1, key="journeymen_limit")

    query = """
    WITH player_team_counts AS (
        SELECT
            pg.player_id,
            p.player_name,
            pg.fantrax_team_name,
            COUNT(*) AS gameweeks_played
        FROM player_gameweek pg
        JOIN player p
            ON p.player_id = pg.player_id
        WHERE pg.fantrax_team_name IS NOT NULL
          AND pg.fantrax_team_name <> ''
          AND pg.fantrax_team_name <> 'FA'
          AND COALESCE(pg.games_played, 0) > 0
        GROUP BY
            pg.player_id,
            p.player_name,
            pg.fantrax_team_name
    )
    SELECT
        player_id,
        player_name,
        fantrax_team_name,
        gameweeks_played
    FROM player_team_counts
    """
    df = run_query(db_path, query)
    if df.empty:
        st.info("Geen journeymen-data gevonden.")
        return

    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name)
    summary = (
        df.groupby(["player_id", "player_name"], dropna=False)
        .agg(
            fantrax_teams_played_for=("fantrax_team_name", "nunique"),
            total_gameweeks_played=("gameweeks_played", "sum"),
        )
        .reset_index()
        .sort_values(
            ["fantrax_teams_played_for", "total_gameweeks_played", "player_name"],
            ascending=[False, False, True],
        )
        .head(limit)
    )

    breakdown = (
        df.groupby(["player_id", "player_name"], dropna=False)
        .apply(
            lambda group: [
                (team, int(gw))
                for team, gw in group.sort_values(
                    ["gameweeks_played", "fantrax_team_name"], ascending=[False, True]
                )[["fantrax_team_name", "gameweeks_played"]].itertuples(index=False, name=None)
            ]
        )
        .reset_index(name="teams")
    )

    merged = summary.merge(breakdown, on=["player_id", "player_name"], how="left")
    for row in merged.itertuples(index=False):
        chips = "".join(
            [
                f'<span class="team-chip" style="background:{team_color(team)}">{team}: {gw} GW</span>'
                for team, gw in row.teams
            ]
        )
        st.markdown(
            f"""
            <div class="journeyman-card">
                <div class="journeyman-title">{row.player_name}</div>
                <div class="journeyman-meta">
                    {row.fantrax_teams_played_for} teams • {row.total_gameweeks_played} total GW
                </div>
                <div>{chips}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_player_timeline(db_path: str) -> None:
    st.subheader("Player Ownership Timeline")
    base_df = load_player_gameweek_base(db_path)
    players = build_player_options(base_df)
    default_index = players.index("Pedro Porro") if "Pedro Porro" in players else 0
    selected_player = st.selectbox("Speler", players, index=default_index)

    query = """
    WITH ordered AS (
        SELECT
            p.player_name,
            COALESCE(pg.fantrax_team_name, pg.status_snapshot) AS fantrax_team_name,
            pg.season_slug,
            pg.gameweek,
            g.date_range_label,
            LAG(COALESCE(pg.fantrax_team_name, pg.status_snapshot)) OVER (
                PARTITION BY p.player_name
                ORDER BY pg.season_slug, pg.gameweek
            ) AS previous_team
        FROM player_gameweek pg
        JOIN player p
            ON p.player_id = pg.player_id
        LEFT JOIN gameweek g
            ON g.season_slug = pg.season_slug
           AND g.gameweek = pg.gameweek
        WHERE p.player_name = ?
          AND COALESCE(pg.games_played, 0) > 0
    ),
    flagged AS (
        SELECT
            *,
            CASE
                WHEN previous_team IS NULL OR previous_team <> fantrax_team_name THEN 1
                ELSE 0
            END AS new_segment
        FROM ordered
    ),
    segmented AS (
        SELECT
            *,
            SUM(new_segment) OVER (
                ORDER BY season_slug, gameweek
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS segment_id
        FROM flagged
    )
    SELECT
        player_name,
        fantrax_team_name,
        COUNT(*) AS gameweeks_played,
        MIN(season_slug || ' GW' || printf('%02d', gameweek)) AS first_gw,
        MAX(season_slug || ' GW' || printf('%02d', gameweek)) AS last_gw,
        MIN(date_range_label) AS first_date_range,
        MAX(date_range_label) AS last_date_range
    FROM segmented
    GROUP BY player_name, fantrax_team_name, segment_id
    ORDER BY first_gw
    """
    df = run_query(db_path, query, (selected_player,))
    if df.empty:
        st.info("Geen gameweeks gevonden voor deze speler.")
        return

    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")

    cols = st.columns(3)
    with cols[0]:
        metric_card("Segments", str(len(df)))
    with cols[1]:
        metric_card("Teams", str(df["fantrax_team_name"].nunique()))
    with cols[2]:
        metric_card("Gameweeks", str(int(df["gameweeks_played"].sum())))

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    for row in df.itertuples(index=False):
        st.markdown(
            f"""
            <div class="timeline-card" style="border-left-color:{team_color(row.fantrax_team_name)}">
                <div class="timeline-team">{row.fantrax_team_name}</div>
                <div class="timeline-meta">{row.gameweeks_played} GW • {row.first_gw} t/m {row.last_gw}</div>
                <div class="timeline-meta">{row.first_date_range or 'Onbekende datum'} → {row.last_date_range or 'Onbekende datum'}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


def render_head_to_head(db_path: str) -> None:
    st.subheader("Head-to-Head Explorer")
    base_df = load_player_gameweek_base(db_path)
    stats_df = load_player_gameweek_stats(db_path)
    matchup_df = load_matchup_results_df(db_path)
    teams = build_team_options(base_df)
    if len(teams) < 2:
        st.info("Niet genoeg teams gevonden voor een onderlinge vergelijking.")
        return

    overview_df = matchup_df[
        matchup_df["fantrax_team_name"].notna()
        & matchup_df["opponent_name"].notna()
        & matchup_df["fantrax_team_name"].isin(teams)
        & matchup_df["opponent_name"].isin(teams)
    ].copy()
    overview_df["win"] = (overview_df["matchup_points_for"] > overview_df["matchup_points_against"]).astype(int)
    overview_df["draw"] = (overview_df["matchup_points_for"] == overview_df["matchup_points_against"]).astype(int)
    overview_df["loss"] = (overview_df["matchup_points_for"] < overview_df["matchup_points_against"]).astype(int)
    summary = (
        overview_df.groupby(["fantrax_team_name", "opponent_name"], dropna=False)[["win", "draw", "loss"]]
        .sum()
        .reset_index()
    )
    summary["record"] = (
        summary["win"].astype(int).astype(str)
        + "-"
        + summary["draw"].astype(int).astype(str)
        + "-"
        + summary["loss"].astype(int).astype(str)
    )
    matrix = (
        summary.pivot(index="fantrax_team_name", columns="opponent_name", values="record")
        .reindex(index=teams, columns=teams)
        .fillna("")
        .reset_index()
        .rename(columns={"fantrax_team_name": "team_name"})
    )
    st.markdown("#### All teams head-to-head")
    st.dataframe(format_columns(matrix), use_container_width=True, hide_index=True)

    team_cols = st.columns(3)
    with team_cols[0]:
        team_a = st.selectbox("Team A", teams, index=0)
    with team_cols[1]:
        default_b = 1 if len(teams) > 1 else 0
        team_b = st.selectbox("Team B", teams, index=default_b)
    with team_cols[2]:
        include_playoffs = st.toggle("Playoffs meenemen", value=False, key="h2h_playoffs")

    if team_a == team_b:
        st.warning("Kies twee verschillende teams.")
        return

    matchup_query = """
    SELECT
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
        team_b_ties
    FROM matchup
    WHERE
        (team_a_name = ? AND team_b_name = ?)
        OR
        (team_a_name = ? AND team_b_name = ?)
    ORDER BY season_slug, gameweek
    """
    matchups = run_query(db_path, matchup_query, (team_a, team_b, team_b, team_a))
    if matchups.empty:
        st.info("Geen onderlinge matchups gevonden.")
        return

    if not include_playoffs:
        matchups = matchups[matchups["is_playoff"] == 0]

    if matchups.empty:
        st.info("Geen onderlinge matchups over na de huidige playoff-filter.")
        return

    summary_rows = []
    team_a_matchup_wins = 0
    team_a_matchup_draws = 0
    team_a_matchup_losses = 0
    team_a_cat_for = 0.0
    team_a_cat_against = 0.0
    team_b_matchup_wins = 0
    team_b_matchup_draws = 0
    team_b_matchup_losses = 0
    team_b_cat_for = 0.0
    team_b_cat_against = 0.0

    for row in matchups.itertuples(index=False):
        if row.team_a_name == team_a:
            points_for = float(row.team_a_points)
            points_against = float(row.team_b_points)
            cat_for = float(row.team_a_wins or 0)
            cat_against = float(row.team_a_losses or 0)
        else:
            points_for = float(row.team_b_points)
            points_against = float(row.team_a_points)
            cat_for = float(row.team_b_wins or 0)
            cat_against = float(row.team_b_losses or 0)

        if points_for > points_against:
            team_a_matchup_wins += 1
            team_b_matchup_losses += 1
            result_label = f"{team_a} win"
        elif points_for < points_against:
            team_a_matchup_losses += 1
            team_b_matchup_wins += 1
            result_label = f"{team_b} win"
        else:
            team_a_matchup_draws += 1
            team_b_matchup_draws += 1
            result_label = "Draw"

        team_a_cat_for += cat_for
        team_a_cat_against += cat_against
        team_b_cat_for += cat_against
        team_b_cat_against += cat_for

        summary_rows.append(
            {
                "season_slug": row.season_slug,
                "gameweek": row.gameweek,
                "is_playoff": row.is_playoff,
                "result": result_label,
                team_a: points_for,
                team_b: points_against,
                f"{team_a} categories": cat_for,
                f"{team_b} categories": cat_against,
            }
        )

    summary_df = pd.DataFrame(summary_rows)

    top_metrics = st.columns(2)
    with top_metrics[0]:
        st.markdown(
            f"""
            <div class="timeline-card" style="border-left-color:{team_color(team_a)}">
                <div class="timeline-team">{team_a}</div>
                <div class="timeline-meta">
                    {team_a_matchup_wins}-{team_a_matchup_draws}-{team_a_matchup_losses}
                    in matchups • Categories: {int(team_a_cat_for)}-{int(team_a_cat_against)}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with top_metrics[1]:
        st.markdown(
            f"""
            <div class="timeline-card" style="border-left-color:{team_color(team_b)}">
                <div class="timeline-team">{team_b}</div>
                <div class="timeline-meta">
                    {team_b_matchup_wins}-{team_b_matchup_draws}-{team_b_matchup_losses}
                    in matchups • Categories: {int(team_b_cat_for)}-{int(team_b_cat_against)}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("#### Matchup log")
    st.dataframe(pretty_df(summary_df), use_container_width=True, hide_index=True)

    fear_df = overview_df.groupby(["fantrax_team_name", "opponent_name"], dropna=False).agg(
        wins=("win", "sum"),
        draws=("draw", "sum"),
        losses=("loss", "sum"),
    ).reset_index()
    fear_df = fear_df.sort_values(["losses", "draws", "wins"], ascending=[False, False, True])
    st.markdown("#### Angstgegner")
    st.dataframe(
        style_team_columns(
            pretty_df(
                fear_df[
                    [
                        "fantrax_team_name",
                        "opponent_name",
                        "wins",
                        "draws",
                        "losses",
                    ]
                ]
            ),
            ["Fantrax team", "Opponent"],
        ),
        use_container_width=True,
        hide_index=True,
    )

    matchup_keys = matchups[["season_slug", "gameweek"]].drop_duplicates().copy()
    matchup_keys["key"] = matchup_keys["season_slug"] + "|" + matchup_keys["gameweek"].astype(str)
    key_set = set(matchup_keys["key"].tolist())

    relevant_base = base_df.copy()
    relevant_base["key"] = relevant_base["season_slug"] + "|" + relevant_base["gameweek"].astype(str)
    relevant_base = relevant_base[
        relevant_base["key"].isin(key_set)
        & relevant_base["fantrax_team_name"].isin([team_a, team_b])
        & (relevant_base["played_flag"] > 0)
    ]

    relevant_stats = stats_df.copy()
    relevant_stats["key"] = relevant_stats["season_slug"] + "|" + relevant_stats["gameweek"].astype(str)
    relevant_stats = relevant_stats[
        relevant_stats["key"].isin(key_set)
        & relevant_stats["fantrax_team_name"].isin([team_a, team_b])
        & (relevant_stats["played_flag"] > 0)
    ]

    stat_options = ["Score"] + sorted(relevant_stats["stat_key"].dropna().unique().tolist())
    stat_choice = st.selectbox(
        "Spelerstat om op te sorteren in deze head-to-head matchups",
        stat_options,
        index=0,
        key="h2h_stat_choice",
    )

    player_tables = st.tabs([team_a, team_b])
    for selected_team, tab in zip([team_a, team_b], player_tables):
        with tab:
            team_base = relevant_base[relevant_base["fantrax_team_name"] == selected_team].copy()
            team_stats = relevant_stats[relevant_stats["fantrax_team_name"] == selected_team].copy()
            roster_df = build_team_roster_table(team_base, team_stats, selected_team)
            if roster_df.empty:
                st.info("Geen spelers gevonden voor deze matchups.")
                continue

            sort_col = stat_choice if stat_choice in roster_df.columns else "gameweeks_played"
            roster_df = roster_df.sort_values(sort_col, ascending=False, na_position="last")

            preferred_columns = [
                "player_name",
                "gameweeks_played",
                "avg_score",
                "positions",
                "premier_league_teams",
                "seasons",
            ]
            if stat_choice in roster_df.columns and stat_choice not in preferred_columns:
                preferred_columns.append(stat_choice)
            remaining_columns = [col for col in roster_df.columns if col not in preferred_columns + ["player_id"]]
            display_columns = [col for col in preferred_columns if col in roster_df.columns] + remaining_columns
            st.dataframe(
                pretty_df(roster_df[display_columns]),
                use_container_width=True,
                hide_index=True,
            )


def render_player_by_team(db_path: str) -> None:
    st.subheader("Player Stats by Team")
    base_df = load_player_gameweek_base(db_path)
    stats_df = load_player_gameweek_stats(db_path)
    players = build_player_options(base_df)
    default_index = players.index("Pedro Porro") if "Pedro Porro" in players else 0
    selected_player = st.selectbox("Speler", players, index=default_index, key="player_by_team_select")

    player_base = base_df[
        (base_df["player_name"] == selected_player) & (base_df["played_flag"] > 0)
    ].copy()
    player_stats = stats_df[
        (stats_df["player_name"] == selected_player) & (stats_df["played_flag"] > 0)
    ].copy()

    if player_base.empty:
        st.info("Geen gameweeks gevonden voor deze speler.")
        return

    summary = (
        player_base.groupby("fantrax_team_name", dropna=False)
        .agg(
            gameweeks_played=("played_flag", "sum"),
            avg_score=("score", "mean"),
            seasons=("season_slug", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
            premier_league_teams=(
                "premier_league_team_code",
                lambda s: " | ".join(sorted({x for x in s.dropna() if x})),
            ),
            positions=("position", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
        )
        .reset_index()
    )

    for stat_key, stat_group in player_stats.groupby("stat_key"):
        if str(stat_key).upper() in IGNORED_STATS:
            continue
        agg_mode = "mean" if "%" in stat_key else "sum"
        values = (
            stat_group.groupby("fantrax_team_name", dropna=False)["stat_value"]
            .agg(agg_mode)
            .reset_index()
            .rename(columns={"stat_value": stat_key})
        )
        summary = summary.merge(values, on="fantrax_team_name", how="left")

    summary["fantrax_team_name"] = summary["fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")
    summary = summary.sort_values(["gameweeks_played", "fantrax_team_name"], ascending=[False, True])
    best_avg_score = summary["avg_score"].max()
    summary["avg_score"] = summary["avg_score"].map(strip_trailing_zero_text)

    cols = st.columns(3)
    with cols[0]:
        metric_card("Teams", str(summary["fantrax_team_name"].nunique()))
    with cols[1]:
        metric_card("Gameweeks", str(int(summary["gameweeks_played"].sum())))
    with cols[2]:
        metric_card("Best avg score", str(strip_trailing_zero_text(best_avg_score)))

    styled = pretty_df(summary)
    st.dataframe(style_team_columns(styled, ["Fantrax team"]), use_container_width=True, hide_index=True)


def render_transactions(db_path: str) -> None:
    st.subheader("Transaction Explorer")
    query = """
    SELECT
        season_slug,
        gameweek,
        player_name,
        premier_league_team_code,
        position,
        transaction_type,
        fantrax_team_name,
        bid_amount,
        priority_value,
        transaction_datetime_text
    FROM transaction_event
    """
    df = run_query(db_path, query)
    if df.empty:
        st.info("Geen transaction-data gevonden.")
        return

    df["fantrax_team_name"] = df["fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")
    df["bid_amount"] = df["bid_amount"].fillna(0)

    filter_cols = st.columns(6)
    with filter_cols[0]:
        seasons = sorted(df["season_slug"].dropna().unique().tolist())
        selected_seasons = st.multiselect("Season", seasons, default=[])
    with filter_cols[1]:
        teams = sorted(df["fantrax_team_name"].dropna().unique().tolist())
        selected_teams = st.multiselect("Fantrax team", teams, default=[])
    with filter_cols[2]:
        types = sorted(df["transaction_type"].dropna().unique().tolist())
        selected_types = st.multiselect("Type", types, default=types)
    with filter_cols[3]:
        min_gw = int(df["gameweek"].min())
        max_gw = int(df["gameweek"].max())
        gw_range = st.slider("GW range", min_gw, max_gw, (min_gw, max_gw))
    with filter_cols[4]:
        min_bid = float(df["bid_amount"].min())
        max_bid = float(df["bid_amount"].max())
        bid_range = st.slider("Bid range", min_bid, max_bid, (min_bid, max_bid))
    with filter_cols[5]:
        positions = sorted(df["position"].dropna().unique().tolist())
        selected_positions = st.multiselect("Position", positions, default=[])

    player_search = st.text_input("Zoek speler in transactions", value="", placeholder="Bijvoorbeeld: Pedro, Son, Watkins")

    filtered = df.copy()
    if selected_seasons:
        filtered = filtered[filtered["season_slug"].isin(selected_seasons)]
    if selected_teams:
        filtered = filtered[filtered["fantrax_team_name"].isin(selected_teams)]
    if selected_types:
        filtered = filtered[filtered["transaction_type"].isin(selected_types)]
    if selected_positions:
        filtered = filtered[filtered["position"].isin(selected_positions)]
    filtered = filtered[
        (filtered["gameweek"] >= gw_range[0])
        & (filtered["gameweek"] <= gw_range[1])
        & (filtered["bid_amount"] >= bid_range[0])
        & (filtered["bid_amount"] <= bid_range[1])
    ]
    if player_search.strip():
        filtered = filtered[
            filtered["player_name"].str.contains(player_search.strip(), case=False, na=False)
        ]

    metrics = st.columns(4)
    with metrics[0]:
        metric_card("Transactions", f"{len(filtered):,}")
    with metrics[1]:
        metric_card("Teams active", str(filtered["fantrax_team_name"].nunique()))
    with metrics[2]:
        metric_card("Total bid", str(strip_trailing_zero_text(filtered["bid_amount"].sum())))
    with metrics[3]:
        metric_card("Avg bid", str(strip_trailing_zero_text(filtered["bid_amount"].mean())))

    gw_summary = (
        filtered.groupby(["season_slug", "gameweek"], dropna=False)
        .agg(
            transactions=("player_name", "count"),
            total_bid=("bid_amount", "sum"),
            avg_bid=("bid_amount", "mean"),
        )
        .reset_index()
        .sort_values(["season_slug", "gameweek"])
    )
    st.markdown("#### Transactions by GW")
    st.dataframe(pretty_df(gw_summary), use_container_width=True, hide_index=True)

    if not gw_summary.empty:
        chart_df = gw_summary.copy()
        chart_df["season_gameweek_label"] = (
            chart_df["season_slug"].astype(str) + " GW" + chart_df["gameweek"].astype(int).astype(str)
        )
        chart = (
            alt.Chart(chart_df)
            .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5)
            .encode(
                x=alt.X("season_gameweek_label:N", sort=None, title="Season / GW"),
                y=alt.Y("avg_bid:Q", title="Avg bid"),
                tooltip=["season_slug", "gameweek", "transactions", "total_bid", "avg_bid"],
                color=alt.value("#0f6b53"),
            )
            .properties(height=280)
        )
        st.markdown("#### Gemiddeld bod per GW")
        st.altair_chart(chart, use_container_width=True)

    team_summary = (
        filtered.groupby("fantrax_team_name", dropna=False)
        .agg(
            transactions=("player_name", "count"),
            total_bid=("bid_amount", "sum"),
            avg_bid=("bid_amount", "mean"),
        )
        .reset_index()
        .sort_values(["transactions", "total_bid"], ascending=[False, False])
    )
    team_summary["avg_bid"] = team_summary["avg_bid"].map(lambda value: format_decimal_text(value, 1))
    st.markdown("#### Team activity")
    st.dataframe(
        style_team_columns(pretty_df(team_summary), ["Fantrax team"]),
        use_container_width=True,
        hide_index=True,
    )

    position_summary = (
        filtered.groupby("position", dropna=False)
        .agg(
            transactions=("player_name", "count"),
            total_bid=("bid_amount", "sum"),
            avg_bid=("bid_amount", "mean"),
        )
        .reset_index()
        .sort_values(["transactions", "avg_bid"], ascending=[False, False])
    )
    st.markdown("#### Activity by position")
    pos_chart = (
        alt.Chart(position_summary)
        .mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5, color="#d9822b")
        .encode(
            x=alt.X("position:N", title="Position"),
            y=alt.Y("transactions:Q", title="Transactions"),
            tooltip=["position", "transactions", "total_bid", "avg_bid"],
        )
        .properties(height=260)
    )
    st.altair_chart(pos_chart, use_container_width=True)
    position_summary["avg_bid"] = position_summary["avg_bid"].map(lambda value: format_decimal_text(value, 1))
    st.dataframe(pretty_df(position_summary), use_container_width=True, hide_index=True)

    st.markdown("#### Transaction log")
    st.dataframe(
        style_team_columns(pretty_df(filtered.sort_values(["season_slug", "gameweek", "transaction_datetime_text"])), ["Fantrax team"]),
        use_container_width=True,
        hide_index=True,
    )


def render_team_profile(db_path: str) -> None:
    st.subheader("GM / Team Profile")
    base_df = load_player_gameweek_base(db_path)
    stats_df = load_player_gameweek_stats(db_path)
    transactions_df = load_transactions_df(db_path)
    trades_df = load_trades_df(db_path)
    draft_df = load_draft_events_df(db_path)
    matchup_df = load_matchup_results_df(db_path)
    matchup_stat_df = load_matchup_stat_team_df(db_path)

    teams = build_team_options(base_df)
    default_team = "Nimma Koempels" if "Nimma Koempels" in teams else teams[0]
    selected_team = st.selectbox("Fantrax team", teams, index=teams.index(default_team), key="team_profile_select")

    standings_query = """
    SELECT *
    FROM v_all_time_team_standings
    WHERE team_name = ?
    """
    standings = run_query(db_path, standings_query, (selected_team,))

    team_base = base_df[(base_df["fantrax_team_name"] == selected_team) & (base_df["played_flag"] > 0)].copy()
    team_transactions = transactions_df[transactions_df["fantrax_team_name"] == selected_team].copy()
    team_trades = trades_df[
        (trades_df["from_fantrax_team_name"] == selected_team)
        | (trades_df["to_fantrax_team_name"] == selected_team)
    ].copy()
    draft_values_df = load_draft_value_df(db_path)
    team_drafts = draft_values_df[
        (draft_values_df["fantrax_team_name"] == selected_team)
        & (draft_values_df["is_goalkeeper"] == 0)
    ].copy()
    team_matchups = matchup_df[matchup_df["fantrax_team_name"] == selected_team].copy()
    team_matchup_stats = matchup_stat_df[matchup_stat_df["fantrax_team_name"] == selected_team].copy()
    drafted_player_ids = set(team_drafts["player_id"].dropna().tolist()) if not team_drafts.empty else set()
    claimed_players = set(
        team_transactions[
            team_transactions["transaction_type"].isin(["Claim", "Add"])
        ]["player_name"].dropna().tolist()
    )
    fear_matchups = team_matchups.copy()
    if not fear_matchups.empty:
        fear_matchups["win_flag"] = (fear_matchups["matchup_points_for"] > fear_matchups["matchup_points_against"]).astype(int)
        fear_matchups["loss_flag"] = (fear_matchups["matchup_points_for"] < fear_matchups["matchup_points_against"]).astype(int)
        fear_matchups["draw_flag"] = (fear_matchups["matchup_points_for"] == fear_matchups["matchup_points_against"]).astype(int)
        fear_summary = fear_matchups.groupby("opponent_name", dropna=False).agg(
            matchup_wins=("win_flag", "sum"),
            matchup_draws=("draw_flag", "sum"),
            matchup_losses=("loss_flag", "sum"),
            categories_won=("category_wins_for", "sum"),
            categories_lost=("category_wins_against", "sum"),
        ).reset_index()
        fear_summary = fear_summary.sort_values(
            ["matchup_losses", "categories_lost", "matchup_wins"],
            ascending=[False, False, True],
        )
    else:
        fear_summary = pd.DataFrame()
    draft_season_filter = st.multiselect(
        "Draft season filter",
        sorted(team_drafts["season_slug"].dropna().unique().tolist()) if not team_drafts.empty else [],
        default=[],
        key="team_profile_draft_seasons",
    )
    if draft_season_filter:
        team_drafts = team_drafts[team_drafts["season_slug"].isin(draft_season_filter)]

    cols = st.columns(6)
    with cols[0]:
        metric_card("Standings points", str(int(standings["standings_points"].iloc[0])) if not standings.empty else "0")
    with cols[1]:
        ppg = standings["standings_points"].iloc[0] / max(standings["matches_played"].iloc[0], 1) if not standings.empty else 0
        metric_card("PPG", str(strip_trailing_zero_text(ppg)))
    with cols[2]:
        metric_card("Transactions", f"{len(team_transactions):,}")
    with cols[3]:
        metric_card("Trades", f"{len(team_trades):,}")
    with cols[4]:
        metric_card("Draft picks", f"{len(team_drafts):,}")
    with cols[5]:
        avg_bid = team_transactions[team_transactions["transaction_type"].isin(["Claim", "Add"])]["bid_amount"].mean() if not team_transactions.empty else 0
        metric_card("Avg spend / pickup", str(strip_trailing_zero_text(avg_bid)))

    st.markdown("#### PL team footprint")
    pl_team_footprint = (
        team_base.groupby("premier_league_team_code", dropna=False)
        .agg(
            players=("player_name", "nunique"),
            gameweeks_played=("played_flag", "sum"),
            avg_score=("score", "mean"),
        )
        .reset_index()
    )
    league_pl = (
        base_df[base_df["played_flag"] > 0]
        .groupby("premier_league_team_code", dropna=False)["player_name"]
        .nunique()
        .reset_index(name="league_players")
    )
    pl_team_footprint = pl_team_footprint.merge(league_pl, on="premier_league_team_code", how="left")
    total_players = pl_team_footprint["players"].sum() or 1
    total_league_players = pl_team_footprint["league_players"].sum() or 1
    pl_team_footprint["team_share_pct"] = (pl_team_footprint["players"] / total_players) * 100
    pl_team_footprint["league_share_pct"] = (pl_team_footprint["league_players"] / total_league_players) * 100
    pl_team_footprint["representation_index"] = pl_team_footprint["team_share_pct"] - pl_team_footprint["league_share_pct"]
    pl_team_footprint = pl_team_footprint.sort_values(
        ["representation_index", "players", "gameweeks_played"],
        ascending=[False, False, False],
    )
    st.dataframe(pretty_df(pl_team_footprint), use_container_width=True, hide_index=True)

    chart_cols = st.columns([1.2, 1])
    with chart_cols[0]:
        st.markdown("#### Rolling form")
        if team_matchups.empty:
            st.info("Geen matchupdata voor dit team.")
        else:
            history = team_matchups.sort_values(["season_slug", "gameweek"]).copy()
            history["season_gameweek_label"] = (
                history["season_slug"] + " GW" + history["gameweek"].astype(int).astype(str).str.zfill(2)
            )
            history["rolling_15_ppg"] = history["league_points"].rolling(15, min_periods=1).mean()
            history["rolling_15_matchup_points"] = history["matchup_points_for"].rolling(15, min_periods=1).mean()
            chart = (
                alt.Chart(history)
                .mark_line(point=True, strokeWidth=3)
                .encode(
                    x=alt.X("season_gameweek_label:N", sort=None, title="Season / GW"),
                    y=alt.Y("rolling_15_ppg:Q", title="Rolling 15 PPG"),
                    tooltip=[
                        alt.Tooltip("season_slug:N", title="Season"),
                        alt.Tooltip("gameweek:Q", title="GW"),
                        alt.Tooltip("league_points:Q", title="Standings points"),
                        alt.Tooltip("rolling_15_ppg:Q", title="Rolling 15 PPG"),
                        alt.Tooltip("rolling_15_matchup_points:Q", title="Rolling 15 matchup points"),
                    ],
                    color=alt.value(team_color(selected_team)),
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)

    with chart_cols[1]:
        st.markdown("#### Positional profile")
        if team_base.empty:
            st.info("Geen spelersdata voor dit team.")
        else:
            profile = team_base.copy()
            profile["primary_position"] = profile["position"].fillna("").str.split(",").str[0].str.strip()
            profile.loc[profile["primary_position"] == "", "primary_position"] = "UNK"
            pos_df = (
                profile.groupby("primary_position", dropna=False)
                .agg(
                    gameweeks_played=("played_flag", "sum"),
                    avg_score=("score", "mean"),
                    players=("player_id", "nunique"),
                )
                .reset_index()
                .sort_values(["gameweeks_played", "avg_score"], ascending=[False, False])
            )
            pie = (
                alt.Chart(pos_df)
                .mark_arc(outerRadius=110)
                .encode(
                    theta=alt.Theta("gameweeks_played:Q", title="GW played"),
                    color=alt.Color("primary_position:N", title="Position"),
                    tooltip=["primary_position", "gameweeks_played", "avg_score", "players"],
                )
                .properties(height=300)
            )
            st.altair_chart(pie, use_container_width=True)

    lower_cols = st.columns(2)
    with lower_cols[0]:
        st.markdown("#### Draft footprint")
        if team_drafts.empty:
            st.info("Geen draftdata voor dit team.")
        else:
            draft_view = team_drafts[
                [
                    "season_slug",
                    "overall_pick",
                    "round_number",
                    "pick_in_round",
                    "player_name",
                    "position",
                    "premier_league_team_code",
                    "drafted_score",
                    "score_rank",
                    "draft_value",
                    "dropped_in_gw",
                ]
            ].copy()
            draft_view = draft_view.sort_values(["season_slug", "overall_pick"], ascending=[True, True], na_position="last")
            st.dataframe(pretty_df(draft_view), use_container_width=True, hide_index=True)

    with lower_cols[1]:
        st.markdown("#### Transaction mix")
        if team_transactions.empty:
            st.info("Geen transactiondata voor dit team.")
        else:
            claims_only = team_transactions[team_transactions["transaction_type"].isin(["Claim", "Add"])].copy()
            claim_count = len(claims_only)
            trade_count = len(team_trades)
            avg_spend_pick = claims_only["bid_amount"].mean() if claim_count else 0
            mix_cols = st.columns(3)
            with mix_cols[0]:
                metric_card("Claims", str(claim_count))
            with mix_cols[1]:
                metric_card("Trades", str(trade_count))
            with mix_cols[2]:
                metric_card("Avg spend / pickup", str(strip_trailing_zero_text(avg_spend_pick)))

            mix = (
                claims_only.groupby("position", dropna=False)
                .agg(
                    position_count=("player_name", "count"),
                    avg_bid=("bid_amount", "mean"),
                )
                .reset_index()
                .sort_values(["avg_bid", "position_count"], ascending=[False, False])
            )
            pos_chart = (
                alt.Chart(mix)
                .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6, color=team_color(selected_team))
                .encode(
                    x=alt.X("position:N", title="Position"),
                    y=alt.Y("avg_bid:Q", title="Avg spend"),
                    tooltip=[
                        alt.Tooltip("position:N", title="Position"),
                        alt.Tooltip("position_count:Q", title="Claims"),
                        alt.Tooltip("avg_bid:Q", title="Avg spend"),
                    ],
                )
                .properties(height=280)
            )
            st.altair_chart(pos_chart, use_container_width=True)
            st.dataframe(pretty_df(mix), use_container_width=True, hide_index=True)

    st.markdown("#### Angstgegner")
    if fear_summary.empty:
        st.info("Geen matchupdata gevonden.")
    else:
        fear_display = fear_summary[
            [
                "opponent_name",
                "matchup_wins",
                "matchup_draws",
                "matchup_losses",
                "categories_won",
                "categories_lost",
            ]
        ].rename(
            columns={
                "matchup_wins": "wins",
                "matchup_draws": "draws",
                "matchup_losses": "losses",
                "categories_won": "categories won",
                "categories_lost": "categories lost",
            }
        )
        st.dataframe(
            style_team_columns(pretty_df(fear_display), ["Opponent"]),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### Drafted vs claimed player quality")
    comparison_base = base_df[base_df["played_flag"] > 0].copy()
    all_claims = transactions_df[transactions_df["transaction_type"].isin(["Claim", "Add"])].copy()
    all_drafts = draft_values_df[draft_values_df["is_goalkeeper"] == 0].copy()
    drafted_scores = []
    claimed_scores = []
    for team_name in build_team_options(base_df):
        team_gameweeks = comparison_base[comparison_base["fantrax_team_name"] == team_name].copy()
        team_draft_ids = set(all_drafts[all_drafts["fantrax_team_name"] == team_name]["player_id"].dropna().tolist())
        team_claim_names = set(all_claims[all_claims["fantrax_team_name"] == team_name]["player_name"].dropna().tolist())
        drafted_only = team_gameweeks[team_gameweeks["player_id"].isin(team_draft_ids)]
        claimed_only = team_gameweeks[team_gameweeks["player_name"].isin(team_claim_names)]
        drafted_scores.append(
            {
                "fantrax_team_name": team_name,
                "source": "Drafted players",
                "avg_score": float(drafted_only["score"].mean()) if not drafted_only.empty else 0.0,
            }
        )
        claimed_scores.append(
            {
                "fantrax_team_name": team_name,
                "source": "Claimed players",
                "avg_score": float(claimed_only["score"].mean()) if not claimed_only.empty else 0.0,
            }
        )

    drafted_only_team = team_base[team_base["player_id"].isin(drafted_player_ids)] if drafted_player_ids else team_base.iloc[0:0]
    claimed_only_team = team_base[team_base["player_name"].isin(claimed_players)] if claimed_players else team_base.iloc[0:0]
    quality_cols = st.columns(2)
    with quality_cols[0]:
        metric_card(
            "Avg score drafted players",
            str(strip_trailing_zero_text(drafted_only_team["score"].mean() if not drafted_only_team.empty else 0)),
        )
    with quality_cols[1]:
        metric_card(
            "Avg score claimed players",
            str(strip_trailing_zero_text(claimed_only_team["score"].mean() if not claimed_only_team.empty else 0)),
        )

    quality_df = pd.DataFrame(drafted_scores + claimed_scores)
    render_grouped_quality_chart(quality_df)

    st.markdown("---")
    render_team_roster_section(
        selected_team=selected_team,
        base_df=base_df,
        stats_df=stats_df,
        selected_seasons=[],
        selected_positions=[],
    )


def render_player_card(db_path: str) -> None:
    st.subheader("Player Card")
    base_df = load_player_gameweek_base(db_path)
    stats_df = load_player_gameweek_stats(db_path)
    matchups_df = load_matchup_results_df(db_path)

    players = build_player_options(base_df)
    default_index = players.index("Pedro Porro") if "Pedro Porro" in players else 0
    selected_player = st.selectbox("Speler", players, index=default_index, key="player_card_select")

    player_base = base_df[(base_df["player_name"] == selected_player) & (base_df["played_flag"] > 0)].copy()
    player_stats = stats_df[(stats_df["player_name"] == selected_player) & (stats_df["played_flag"] > 0)].copy()
    if player_base.empty:
        st.info("Geen gameweeks gevonden voor deze speler.")
        return

    player_base = player_base.merge(
        matchups_df[
            [
                "season_slug",
                "gameweek",
                "fantrax_team_name",
                "opponent_name",
                "category_wins_for",
                "category_wins_against",
                "matchup_points_for",
                "matchup_points_against",
            ]
        ],
        on=["season_slug", "gameweek", "fantrax_team_name"],
        how="left",
    )

    metrics = st.columns(5)
    with metrics[0]:
        metric_card("Teams", str(player_base["fantrax_team_name"].nunique()))
    with metrics[1]:
        metric_card("Seasons", str(player_base["season_slug"].nunique()))
    with metrics[2]:
        metric_card("GW played", str(int(player_base["played_flag"].sum())))
    with metrics[3]:
        metric_card("Avg score", str(strip_trailing_zero_text(player_base["score"].mean())))
    with metrics[4]:
        metric_card("Best score", str(strip_trailing_zero_text(player_base["score"].max())))

    season_summary = (
        player_base.groupby("season_slug", dropna=False)
        .agg(
            gameweeks_played=("played_flag", "sum"),
            avg_score=("score", "mean"),
            fantrax_teams=("fantrax_team_name", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
            premier_league_teams=("premier_league_team_code", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
        )
        .reset_index()
        .sort_values("season_slug")
    )

    best_gameweeks = player_base[
        [
            "season_slug",
            "gameweek",
            "date_range_label",
            "fantrax_team_name",
            "opponent_name",
            "premier_league_team_code",
            "score",
        ]
    ].sort_values(["score", "season_slug", "gameweek"], ascending=[False, True, True]).head(12)

    versus_teams = (
        player_base.groupby("opponent_name", dropna=False)
        .agg(
            gameweeks_played=("played_flag", "sum"),
            avg_score=("score", "mean"),
            best_score=("score", "max"),
            categories_won=("category_wins_for", "sum"),
            categories_lost=("category_wins_against", "sum"),
            matchup_points_for=("matchup_points_for", "sum"),
            matchup_points_against=("matchup_points_against", "sum"),
        )
        .reset_index()
        .sort_values(["avg_score", "gameweeks_played"], ascending=[False, False])
    )

    timeline_source = player_base.sort_values(["season_slug", "gameweek"]).reset_index(drop=True).copy()
    timeline_source["gw_index"] = range(1, len(timeline_source) + 1)
    timeline_source["previous_team"] = timeline_source["fantrax_team_name"].shift(1)
    timeline_source["new_segment"] = (
        timeline_source["previous_team"].isna()
        | (timeline_source["previous_team"] != timeline_source["fantrax_team_name"])
    ).astype(int)
    timeline_source["segment_id"] = timeline_source["new_segment"].cumsum()
    ownership = (
        timeline_source.groupby(["segment_id", "fantrax_team_name"], dropna=False)
        .agg(
            gameweeks_played=("played_flag", "sum"),
            avg_score=("score", "mean"),
            first_gw=("season_gameweek_label", "first"),
            last_gw=("season_gameweek_label", "last"),
            start_index=("gw_index", "min"),
            end_index=("gw_index", "max"),
        )
        .reset_index()
        .sort_values("start_index")
    )
    tick_pairs: list[tuple[float, str]] = []
    for row in ownership.itertuples(index=False):
        tick_pairs.append((float(row.start_index), str(row.first_gw)))
        tick_pairs.append((float(row.end_index), str(row.last_gw)))
    seen_ticks: set[tuple[float, str]] = set()
    unique_ticks: list[tuple[float, str]] = []
    for pair in tick_pairs:
        if pair not in seen_ticks:
            seen_ticks.add(pair)
            unique_ticks.append(pair)
    tick_values = [value for value, _ in unique_ticks]
    label_expr_parts = [f"datum.value == {int(value)} ? '{label}'" for value, label in unique_ticks]
    axis_label_expr = " : ".join(label_expr_parts) + " : ''" if label_expr_parts else "''"

    if not player_stats.empty:
        totals = (
            player_stats.groupby("stat_key", dropna=False)
            .agg(stat_value=("stat_value", "sum"), stat_per_gw=("stat_value", "mean"))
            .reset_index()
        )
        stat_totals = totals.sort_values("stat_value", ascending=False)
    else:
        stat_totals = pd.DataFrame()

    left, right = st.columns(2)
    with left:
        st.markdown("#### Production by season")
        st.dataframe(style_team_columns(pretty_df(season_summary), ["Fantrax teams"]), use_container_width=True, hide_index=True)
        st.markdown("#### Best gameweeks")
        st.dataframe(style_team_columns(pretty_df(best_gameweeks), ["Fantrax team", "Opponent"]), use_container_width=True, hide_index=True)

    with right:
        st.markdown("#### Ownership history")
        timeline_df = ownership.copy()
        timeline_chart = (
            alt.Chart(timeline_df)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X(
                    "start_index:Q",
                    title="Jaar-GW",
                    axis=alt.Axis(values=tick_values, labelAngle=-40, labelExpr=axis_label_expr),
                ),
                x2="end_index:Q",
                y=alt.Y("avg_score:Q", title="Avg score"),
                color=alt.Color(
                    "fantrax_team_name:N",
                    scale=alt.Scale(
                        domain=sorted(timeline_df["fantrax_team_name"].dropna().unique().tolist()),
                        range=[team_color(team) for team in sorted(timeline_df["fantrax_team_name"].dropna().unique().tolist())],
                    ),
                    legend=alt.Legend(title="Fantrax team"),
                ),
                tooltip=[
                    alt.Tooltip("fantrax_team_name:N", title="Fantrax team"),
                    alt.Tooltip("gameweeks_played:Q", title="GW played"),
                    alt.Tooltip("avg_score:Q", title="Avg score"),
                    alt.Tooltip("first_gw:N", title="First GW"),
                    alt.Tooltip("last_gw:N", title="Last GW"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(timeline_chart, use_container_width=True)
        ownership_display = ownership[
            ["fantrax_team_name", "gameweeks_played", "avg_score", "first_gw", "last_gw"]
        ].copy()
        st.dataframe(style_team_columns(pretty_df(ownership_display), ["Fantrax team"]), use_container_width=True, hide_index=True)
        st.markdown("#### Best opponents")
        best_opponents_display = versus_teams.head(12).rename(
            columns={
                "categories_won": "categories won",
                "categories_lost": "categories lost",
                "matchup_points_for": "matchup points for",
                "matchup_points_against": "matchup points against",
            }
        )
        st.dataframe(style_team_columns(pretty_df(best_opponents_display), ["Opponent"]), use_container_width=True, hide_index=True)

    if not stat_totals.empty:
        st.markdown("#### Career stat totals")
        common_stats = ["G", "SOT", "KP", "TkW", "CSD", "AP", "AER", "CoSF"]
        radar_controls = st.columns(2)
        with radar_controls[0]:
            comparison_options = ["Geen vergelijking"] + players
            compare_player = st.selectbox("Vergelijk met", comparison_options, index=0, key="player_card_compare")
        with radar_controls[1]:
            radar_scope = st.selectbox(
                "Scope voor radar",
                ["Carrière"] + sorted(base_df["season_slug"].dropna().unique().tolist()),
                index=0,
                key="player_card_scope",
            )
        selected_seasons = [] if radar_scope == "Carrière" else [radar_scope]
        primary_names = [selected_player]
        if compare_player != "Geen vergelijking":
            primary_names.append(compare_player)
        radar_frames = []
        eligible_base = base_df[base_df["played_flag"] > 0].copy()
        if selected_seasons:
            eligible_base = eligible_base[eligible_base["season_slug"].isin(selected_seasons)]
        player_gw_count = int(player_base["played_flag"].sum())
        if player_gw_count >= 30:
            peer_counts = (
                eligible_base
                .groupby("player_name", dropna=False)["played_flag"]
                .sum()
                .reset_index(name="gameweeks_played")
            )
            eligible_players = set(peer_counts[peer_counts["gameweeks_played"] >= 30]["player_name"])
            peer_rates = (
                stats_df[
                    stats_df["player_name"].isin(eligible_players)
                    & stats_df["stat_key"].isin(common_stats)
                    & (stats_df["played_flag"] > 0)
                ]
                .pipe(lambda df: df[df["season_slug"].isin(selected_seasons)] if selected_seasons else df)
                .groupby(["player_name", "stat_key"], dropna=False)["stat_value"]
                .mean()
                .reset_index(name="stat_per_gw")
            )
            for player_name in primary_names:
                player_base_candidate = base_df[(base_df["player_name"] == player_name) & (base_df["played_flag"] > 0)].copy()
                if selected_seasons:
                    player_base_candidate = player_base_candidate[player_base_candidate["season_slug"].isin(selected_seasons)]
                if int(player_base_candidate["played_flag"].sum()) < 30:
                    continue
                player_stats_candidate = stats_df[
                    (stats_df["player_name"] == player_name)
                    & (stats_df["stat_key"].isin(common_stats))
                    & (stats_df["played_flag"] > 0)
                ].copy()
                if selected_seasons:
                    player_stats_candidate = player_stats_candidate[player_stats_candidate["season_slug"].isin(selected_seasons)]
                player_rates = (
                    player_stats_candidate.groupby("stat_key", dropna=False)["stat_value"]
                    .mean()
                    .reindex(common_stats)
                )
                rose_rows = []
                for stat in common_stats:
                    peers = peer_rates[peer_rates["stat_key"] == stat]["stat_per_gw"]
                    player_rate = float(player_rates.get(stat, 0) or 0)
                    percentile = float((peers <= player_rate).mean() * 100) if not peers.empty else 0.0
                    rose_rows.append(
                        {
                            "player_name": player_name,
                            "stat_key": stat,
                            "stat_per_gw": player_rate,
                            "percentile": percentile,
                        }
                    )
                radar_frames.append(pd.DataFrame(rose_rows))
            st.caption("De roosgrafiek toont per stat de percentielscore per gespeelde gameweek, vergeleken met alle spelers die minimaal 30 gameweeks hebben gespeeld binnen de gekozen seizoensfilter.")
            if radar_frames:
                radar_df = pd.concat(radar_frames, ignore_index=True)
                labels = common_stats
                fig, ax = plt.subplots(figsize=(4.5, 4.5), subplot_kw={"polar": True})
                ax.set_theta_offset(np.pi / 2)
                ax.set_theta_direction(-1)
                angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
                angles += angles[:1]
                palette = ["#0f6b53", "#d9822b"]
                for idx, player_name in enumerate(radar_df["player_name"].dropna().unique().tolist()):
                    frame = radar_df[radar_df["player_name"] == player_name]
                    values = frame.set_index("stat_key").reindex(labels)["percentile"].astype(float).tolist()
                    values += values[:1]
                    color = palette[idx % len(palette)]
                    ax.plot(angles, values, color=color, linewidth=2.2, label=player_name)
                    ax.fill(angles, values, color=color, alpha=0.18)
                ax.set_xticks(angles[:-1])
                ax.set_xticklabels(labels, fontsize=9)
                ax.set_ylim(0, 100)
                ax.set_yticks([20, 40, 60, 80, 100])
                ax.set_yticklabels(["20", "40", "60", "80", "100"], fontsize=7)
                ax.grid(color="#b7b0a2", alpha=0.5)
                ax.legend(loc="upper right", bbox_to_anchor=(1.25, 1.15), frameon=False)
                st.pyplot(fig, use_container_width=False)
                plt.close(fig)
        else:
            st.info("Roosdiagram beschikbaar vanaf minimaal 30 gespeelde gameweeks voor deze speler.")


def render_trade_lab(db_path: str) -> None:
    st.subheader("Trade Impact")
    trades_df = load_trades_df(db_path)
    base_df = load_player_gameweek_base(db_path)
    if trades_df.empty:
        st.info("Geen trades gevonden.")
        return
    trades_df = trades_df[
        trades_df["player_name"].notna()
        & ~trades_df["player_name"].astype(str).str.fullmatch(r"\(Drop\)|Drop", case=False)
    ].copy()

    seasons = sorted(trades_df["season_slug"].dropna().unique().tolist())
    selected_seasons = st.multiselect("Season", seasons, default=[])
    if selected_seasons:
        trades_df = trades_df[trades_df["season_slug"].isin(selected_seasons)]

    scored = base_df[base_df["played_flag"] > 0][["player_name", "season_slug", "gameweek", "fantrax_team_name", "score"]].copy()
    rows = []
    for trade in trades_df.itertuples(index=False):
        post = scored[
            (scored["player_name"] == trade.player_name)
            & (scored["season_slug"] == trade.season_slug)
            & (scored["gameweek"] >= trade.gameweek)
            & (scored["fantrax_team_name"] == trade.to_fantrax_team_name)
        ]
        rows.append(
            {
                "season_slug": trade.season_slug,
                "gameweek": trade.gameweek,
                "player_name": trade.player_name,
                "from_fantrax_team_name": trade.from_fantrax_team_name,
                "to_fantrax_team_name": trade.to_fantrax_team_name,
                "position": trade.position,
                "premier_league_team_code": trade.premier_league_team_code,
                "gws_after_trade": int(post["gameweek"].nunique()),
                "post_trade_score": float(post["score"].mean()) if not post.empty else 0.0,
            }
        )

    trade_impact = pd.DataFrame(rows).sort_values(["post_trade_score", "gws_after_trade"], ascending=[False, False])
    if not trade_impact.empty:
        trade_impact["post_trade_score"] = trade_impact["post_trade_score"].round(2)
    summary = (
        trade_impact.groupby("to_fantrax_team_name", dropna=False)
        .agg(
            trades=("player_name", "count"),
            post_trade_score=("post_trade_score", "mean"),
            gws_after_trade=("gws_after_trade", "sum"),
        )
        .reset_index()
        .sort_values(["post_trade_score", "trades"], ascending=[False, False])
    )
    if not summary.empty:
        summary["post_trade_score"] = summary["post_trade_score"].round(2)
    trade_log = (
        trades_df.groupby(
            ["season_slug", "gameweek", "transaction_datetime_text", "from_fantrax_team_name", "to_fantrax_team_name"],
            dropna=False,
        )
        .agg(
            players_received=("player_name", lambda s: ", ".join(sorted(pd.unique(s)))),
            trade_partners=("player_name", "count"),
        )
        .reset_index()
        .sort_values(["season_slug", "gameweek", "transaction_datetime_text", "from_fantrax_team_name"])
    )

    cols = st.columns(3)
    with cols[0]:
        metric_card("Trade assets", f"{len(trade_impact):,}")
    with cols[1]:
        metric_card("Best asset", str(strip_trailing_zero_text(trade_impact["post_trade_score"].max() if not trade_impact.empty else 0)))
    with cols[2]:
        metric_card("Teams involved", str(pd.unique(pd.concat([trade_impact["from_fantrax_team_name"], trade_impact["to_fantrax_team_name"]])).size if not trade_impact.empty else 0))

    st.caption("Post-trade score = gemiddelde Fantrax score per gameweek die een speler nog produceerde voor het ontvangende team vanaf die trade-GW. GW's ná trade = aantal gameweeks waarin die speler daarna voor dat team speelde.")
    st.markdown("#### Acquired value by team")
    st.dataframe(style_team_columns(pretty_df(summary), ["To team"]), use_container_width=True, hide_index=True)
    st.markdown("#### Best traded assets")
    st.dataframe(style_team_columns(pretty_df(trade_impact.head(30)), ["From team", "To team"]), use_container_width=True, hide_index=True)
    st.markdown("#### Chronological trade log")
    st.dataframe(style_team_columns(pretty_df(trade_log), ["From team", "To team"]), use_container_width=True, hide_index=True)


def render_waiver_lab(db_path: str) -> None:
    st.subheader("Waiver Value")
    st.caption("Post-claim score = totale Fantrax score die een speler na een claim/add nog produceerde voor dat team in dat seizoen. Value per $ = post-claim score gedeeld door het bod; bij een bod van 0 tonen we gewoon de totale post-claim score.")
    season_filter = st.multiselect(
        "Season",
        sorted(load_transactions_df(db_path)["season_slug"].dropna().unique().tolist()),
        default=[],
    )
    placeholders = ",".join("?" for _ in season_filter) if season_filter else ""
    season_where = f"AND t.season_slug IN ({placeholders})" if season_filter else ""
    query = f"""
    WITH claims AS (
        SELECT
            t.season_slug,
            t.gameweek,
            t.player_name,
            COALESCE(t.fantrax_team_name, 'UNKNOWN') AS fantrax_team_name,
            t.position,
            t.premier_league_team_code,
            COALESCE(t.bid_amount, 0) AS bid_amount
        FROM transaction_event t
        WHERE t.transaction_type IN ('Claim', 'Add')
        {season_where}
    ),
    scored AS (
        SELECT
            pg.season_slug,
            pg.gameweek,
            pg.fantrax_team_name,
            p.player_name,
            pg.score
        FROM player_gameweek pg
        JOIN player p ON p.player_id = pg.player_id
    )
    SELECT
        c.season_slug,
        c.gameweek,
        c.player_name,
        c.fantrax_team_name,
        c.position,
        c.premier_league_team_code,
        c.bid_amount,
        COUNT(DISTINCT s.gameweek) AS post_claim_gw,
        COALESCE(SUM(s.score), 0) AS post_claim_score
    FROM claims c
    LEFT JOIN scored s
        ON s.season_slug = c.season_slug
       AND s.fantrax_team_name = c.fantrax_team_name
       AND s.player_name = c.player_name
       AND s.gameweek >= c.gameweek
    GROUP BY
        c.season_slug,
        c.gameweek,
        c.player_name,
        c.fantrax_team_name,
        c.position,
        c.premier_league_team_code,
        c.bid_amount
    """
    waiver_df = run_query(db_path, query, tuple(season_filter))
    if waiver_df.empty:
        st.info("Geen claims/adds voor de huidige filters.")
        return
    waiver_df["fantrax_team_name"] = waiver_df["fantrax_team_name"].map(normalize_team_name).fillna("UNKNOWN")
    waiver_df = waiver_df.rename(columns={"post_claim_gw": "gws_after_claim"})
    waiver_df["post_claim_score"] = waiver_df.apply(
        lambda row: float(row["post_claim_score"]) / row["gws_after_claim"] if float(row["gws_after_claim"] or 0) > 0 else 0.0,
        axis=1,
    )
    waiver_df["post_claim_score"] = waiver_df["post_claim_score"].round(2)

    leaderboard = (
        waiver_df[waiver_df["gws_after_claim"] > 0].groupby("fantrax_team_name", dropna=False)
        .agg(
            claims=("player_name", "count"),
            total_bid=("bid_amount", "sum"),
            post_claim_score=("post_claim_score", "mean"),
            gws_after_claim=("gws_after_claim", "sum"),
        )
        .reset_index()
        .sort_values(["post_claim_score", "claims"], ascending=[False, False])
    )
    if not leaderboard.empty:
        leaderboard["post_claim_score"] = leaderboard["post_claim_score"].round(2)

    left, right = st.columns(2)
    with left:
        st.markdown("#### Best average score after claim")
        best_value = waiver_df[waiver_df["gws_after_claim"] >= 10].sort_values(["post_claim_score", "gws_after_claim"], ascending=[False, False]).head(25)
        st.dataframe(style_team_columns(pretty_df(best_value), ["Fantrax team"]), use_container_width=True, hide_index=True)
    with right:
        st.markdown("#### Most expensive claims")
        expensive = waiver_df.sort_values(["bid_amount", "post_claim_score"], ascending=[False, False]).head(25)
        st.dataframe(style_team_columns(pretty_df(expensive), ["Fantrax team"]), use_container_width=True, hide_index=True)

    st.markdown("#### Waiver leaderboard")
    st.dataframe(style_team_columns(pretty_df(leaderboard), ["Fantrax team"]), use_container_width=True, hide_index=True)


def render_records(db_path: str) -> None:
    st.subheader("Records Hall of Fame")
    focus = st.session_state.get("record_focus")
    if focus:
        st.info(
            f"Geopend vanuit overview: {focus.get('title', '')} ({focus.get('section', '')}). "
            f"{focus.get('detail', '')}"
        )
    base_df = load_player_gameweek_base(db_path)
    stats_df = load_player_gameweek_stats(db_path)
    transactions_df = load_transactions_df(db_path)
    matchup_df = load_matchup_results_df(db_path)
    matchup_stat_df = load_matchup_stat_team_df(db_path)
    real_matchups = matchup_df[
        matchup_df["opponent_name"].fillna("").ne("None/Bye")
        & matchup_df["fantrax_team_name"].fillna("").ne("None/Bye")
    ].copy()
    real_matchups["result_code"] = real_matchups.apply(
        lambda row: "W" if row["matchup_points_for"] > row["matchup_points_against"] else ("D" if row["matchup_points_for"] == row["matchup_points_against"] else "L"),
        axis=1,
    )

    team_records = real_matchups.sort_values(
        ["matchup_points_for", "category_wins_for"], ascending=[False, False]
    ).head(12)[
        [
            "season_slug",
            "gameweek",
            "fantrax_team_name",
            "opponent_name",
            "matchup_points_for",
            "matchup_points_against",
            "category_wins_for",
            "category_wins_against",
            "is_playoff",
        ]
    ]

    player_opponents = real_matchups[
        ["season_slug", "gameweek", "fantrax_team_name", "opponent_name"]
    ].drop_duplicates()
    stat_pool = stats_df.merge(
        player_opponents,
        on=["season_slug", "gameweek", "fantrax_team_name"],
        how="left",
    )
    stat_pool = stat_pool[stat_pool["opponent_name"].notna()].copy()
    top5_by_stat = (
        stat_pool.sort_values(["stat_key", "stat_value"], ascending=[True, False])
        .groupby("stat_key", dropna=False)
        .head(5)[
            [
                "stat_key",
                "player_name",
                "season_slug",
                "gameweek",
                "fantrax_team_name",
                "opponent_name",
                "premier_league_team_code",
                "stat_value",
            ]
        ]
        .copy()
    )
    top5_by_stat["rank_within_stat"] = top5_by_stat.groupby("stat_key").cumcount() + 1

    transaction_top_paid_bids = (
        transactions_df[transactions_df["bid_amount"] > 0]
        .groupby("fantrax_team_name", dropna=False)
        .agg(claims_with_bid=("player_name", "count"))
        .reset_index()
        .sort_values("claims_with_bid", ascending=False)
    )
    transaction_zero_claims = (
        transactions_df[
            (transactions_df["transaction_type"].isin(["Claim", "Add"]))
            & (transactions_df["bid_amount"] == 0)
        ]
        .groupby("fantrax_team_name", dropna=False)
        .agg(zero_euro_claims=("player_name", "count"))
        .reset_index()
        .sort_values("zero_euro_claims", ascending=False)
    )
    highest_bids = transactions_df.sort_values(["bid_amount", "season_slug", "gameweek"], ascending=[False, True, True]).head(10)[
        ["season_slug", "gameweek", "fantrax_team_name", "player_name", "bid_amount", "transaction_type"]
    ]
    draft_records = load_draft_value_df(db_path)
    draft_records = draft_records[(draft_records["is_goalkeeper"] == 0) & draft_records["draft_value"].notna()].copy()
    draft_records = draft_records.sort_values(["draft_value", "drafted_score"], ascending=[False, False]).head(20)
    base_xi_source = base_df[base_df["played_flag"] > 0].copy()
    available_xi_seasons = sorted(base_xi_source["season_slug"].dropna().unique().tolist())
    team_stat_top5 = (
        stat_pool.groupby(
            ["stat_key", "season_slug", "gameweek", "fantrax_team_name", "opponent_name"],
            dropna=False,
        )["stat_value"]
        .sum()
        .reset_index()
        .sort_values(["stat_key", "stat_value"], ascending=[True, False])
        .groupby("stat_key", dropna=False)
        .head(5)
    )
    streak_frames = [
        build_streak_rows(real_matchups, ["fantrax_team_name"], "result_code", "Win streak", lambda result: result == "W"),
        build_streak_rows(real_matchups, ["fantrax_team_name"], "result_code", "Unbeaten streak", lambda result: result in {"W", "D"}),
        build_streak_rows(real_matchups, ["fantrax_team_name"], "result_code", "No-win streak", lambda result: result in {"L", "D"}),
        build_streak_rows(real_matchups, ["fantrax_team_name"], "result_code", "Loss streak", lambda result: result == "L"),
    ]
    streak_df = pd.concat(streak_frames, ignore_index=True) if streak_frames else pd.DataFrame()
    category_source = matchup_stat_df[
        matchup_stat_df["fantrax_team_name"].fillna("").ne("None/Bye")
        & matchup_stat_df["opponent_name"].fillna("").ne("None/Bye")
    ].copy()
    category_source = category_source[~category_source["stat_key"].isin(["A", "GA", "INT", "BR"])].copy()
    category_win_streaks = build_streak_rows(category_source, ["fantrax_team_name", "stat_key"], "result", "Category win streak", lambda result: result == "W")
    category_loss_streaks = build_streak_rows(category_source, ["fantrax_team_name", "stat_key"], "result", "Category loss streak", lambda result: result == "L")
    score_profile = (
        base_xi_source
        .groupby(["player_id", "player_name"], dropna=False)
        .agg(
            gameweeks_played=("played_flag", "sum"),
            avg_score=("score", "mean"),
            positions=("position", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
            premier_league_teams=("premier_league_team_code", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
        )
        .reset_index()
    )
    eligible_xi = score_profile[score_profile["gameweeks_played"] >= 39].copy()
    if not eligible_xi.empty:
        eligible_xi["score_percentile"] = eligible_xi["avg_score"].rank(pct=True) * 100
        eligible_xi = eligible_xi.sort_values(["score_percentile", "avg_score"], ascending=[False, False])

    tabs = st.tabs(["Team records", "Player stat top 5", "Team stat top 5", "Best XI", "Transactions", "Draft value", "Streaks"])
    with tabs[0]:
        st.markdown("#### Highest team matchup scores")
        st.caption("Alleen echte wedstrijden; duels tegen `None/Bye` zijn weggefilterd.")
        for row in team_records.itertuples(index=False):
            title = (
                f"{row.fantrax_team_name} vs {row.opponent_name} "
                f"— {strip_trailing_zero_text(row.matchup_points_for)}-"
                f"{strip_trailing_zero_text(row.matchup_points_against)} "
                f"({row.season_slug} GW{int(row.gameweek):02d})"
            )
            with st.expander(title):
                detail = pd.DataFrame(
                    [
                        {
                            "season_slug": row.season_slug,
                            "gameweek": row.gameweek,
                            "fantrax_team_name": row.fantrax_team_name,
                            "opponent_name": row.opponent_name,
                            "matchup_points_for": row.matchup_points_for,
                            "matchup_points_against": row.matchup_points_against,
                            "category_wins_for": row.category_wins_for,
                            "category_wins_against": row.category_wins_against,
                            "is_playoff": row.is_playoff,
                        }
                    ]
                )
                st.dataframe(
                    style_team_columns(pretty_df(detail), ["Fantrax team", "Opponent"]),
                    use_container_width=True,
                    hide_index=True,
                )
                lineup_cols = st.columns(2)
                team_a_base = base_df[
                    (base_df["season_slug"] == row.season_slug)
                    & (base_df["gameweek"] == row.gameweek)
                    & (base_df["fantrax_team_name"] == row.fantrax_team_name)
                    & (base_df["played_flag"] > 0)
                ].copy()
                team_a_stats = stats_df[
                    (stats_df["season_slug"] == row.season_slug)
                    & (stats_df["gameweek"] == row.gameweek)
                    & (stats_df["fantrax_team_name"] == row.fantrax_team_name)
                    & (stats_df["played_flag"] > 0)
                ].copy()
                team_b_base = base_df[
                    (base_df["season_slug"] == row.season_slug)
                    & (base_df["gameweek"] == row.gameweek)
                    & (base_df["fantrax_team_name"] == row.opponent_name)
                    & (base_df["played_flag"] > 0)
                ].copy()
                team_b_stats = stats_df[
                    (stats_df["season_slug"] == row.season_slug)
                    & (stats_df["gameweek"] == row.gameweek)
                    & (stats_df["fantrax_team_name"] == row.opponent_name)
                    & (stats_df["played_flag"] > 0)
                ].copy()
                with lineup_cols[0]:
                    st.markdown(f"##### {row.fantrax_team_name}")
                    team_a_roster = build_team_roster_table(team_a_base, team_a_stats, row.fantrax_team_name)
                    if not team_a_roster.empty:
                        st.dataframe(pretty_df(team_a_roster), use_container_width=True, hide_index=True)
                with lineup_cols[1]:
                    st.markdown(f"##### {row.opponent_name}")
                    team_b_roster = build_team_roster_table(team_b_base, team_b_stats, row.opponent_name)
                    if not team_b_roster.empty:
                        st.dataframe(pretty_df(team_b_roster), use_container_width=True, hide_index=True)
    with tabs[1]:
        st.markdown("#### Top 5 per stat")
        stat_blocks = sorted(top5_by_stat["stat_key"].dropna().unique().tolist())
        block_cols = st.columns(2)
        for index, stat_key in enumerate(stat_blocks):
            with block_cols[index % 2]:
                st.markdown(f"##### {stat_key}")
                block = top5_by_stat[top5_by_stat["stat_key"] == stat_key][
                    [
                        "rank_within_stat",
                        "player_name",
                        "season_slug",
                        "gameweek",
                        "fantrax_team_name",
                        "opponent_name",
                        "stat_value",
                    ]
                ]
                st.dataframe(
                    style_team_columns(pretty_df(block), ["Fantrax team", "Opponent"]),
                    use_container_width=True,
                    hide_index=True,
                )
    with tabs[2]:
        st.markdown("#### Top 5 team totals per stat")
        for row in team_stat_top5.itertuples(index=False):
            title = (
                f"{row.stat_key}: {row.fantrax_team_name} "
                f"({row.season_slug} GW{int(row.gameweek):02d}) - {strip_trailing_zero_text(row.stat_value)}"
            )
            with st.expander(title):
                contributors = stat_pool[
                    (stat_pool["stat_key"] == row.stat_key)
                    & (stat_pool["season_slug"] == row.season_slug)
                    & (stat_pool["gameweek"] == row.gameweek)
                    & (stat_pool["fantrax_team_name"] == row.fantrax_team_name)
                ][
                    ["player_name", "premier_league_team_code", "opponent_name", "stat_value"]
                ].sort_values("stat_value", ascending=False)
                st.dataframe(
                    style_team_columns(pretty_df(contributors), ["Opponent"]),
                    use_container_width=True,
                    hide_index=True,
                )
    with tabs[3]:
        st.markdown("#### All-Time Best XI")
        xi_scope = st.selectbox(
            "Best XI scope",
            ["All-time"] + available_xi_seasons,
            index=0,
            key="records_best_xi_scope",
        )
        xi_source = base_xi_source.copy()
        if xi_scope != "All-time":
            xi_source = xi_source[xi_source["season_slug"] == xi_scope]
        xi_profile = (
            xi_source.groupby(["player_id", "player_name"], dropna=False)
            .agg(
                gameweeks_played=("played_flag", "sum"),
                avg_score=("score", "mean"),
                positions=("position", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
                premier_league_teams=("premier_league_team_code", lambda s: " | ".join(sorted({x for x in s.dropna() if x}))),
            )
            .reset_index()
        )
        xi_eligible = xi_profile[xi_profile["gameweeks_played"] >= 39].copy()
        if not xi_eligible.empty:
            xi_eligible["score_percentile"] = xi_eligible["avg_score"].rank(pct=True) * 100
            xi_eligible = xi_eligible.sort_values(["score_percentile", "avg_score"], ascending=[False, False])
        st.caption("Gebaseerd op scorepercentiel over alle spelers met minimaal 39 gespeelde gameweeks binnen de gekozen scope.")
        if xi_eligible.empty:
            st.info("Niet genoeg spelerdata voor een Best XI.")
        else:
            formation_name = st.selectbox(
                "Formatie voor Best XI",
                list(FORMATION_MAP.keys()),
                index=2,
                key="records_best_xi_formation",
            )
            render_best_xi_pitch(xi_eligible, formation_name, "BarcelOna '53")
            best_xi_table = xi_eligible[
                ["player_name", "positions", "premier_league_teams", "gameweeks_played", "avg_score", "score_percentile"]
            ].copy()
            st.dataframe(pretty_df(best_xi_table.head(25)), use_container_width=True, hide_index=True)
    with tabs[4]:
        sub_tabs = st.tabs(["Most paid bids", "Most zero-euro claims", "Highest bids"])
        with sub_tabs[0]:
            st.dataframe(style_team_columns(pretty_df(transaction_top_paid_bids), ["Fantrax team"]), use_container_width=True, hide_index=True)
        with sub_tabs[1]:
            st.dataframe(style_team_columns(pretty_df(transaction_zero_claims), ["Fantrax team"]), use_container_width=True, hide_index=True)
        with sub_tabs[2]:
            st.dataframe(style_team_columns(pretty_df(highest_bids), ["Fantrax team"]), use_container_width=True, hide_index=True)
    with tabs[5]:
        st.caption("Draft value = draftpositie minus eindrang op seizoensscore. Hoe hoger dit getal, hoe later iemand gedraft werd ten opzichte van hoe goed zijn seizoen uiteindelijk was.")
        draft_display = draft_records[
            [
                "season_slug",
                "overall_pick",
                "round_number",
                "fantrax_team_name",
                "player_name",
                "position",
                "premier_league_team_code",
                "drafted_score",
                "score_rank",
                "draft_value",
                "dropped_in_gw",
            ]
        ].copy()
        st.dataframe(style_team_columns(pretty_df(draft_display), ["Fantrax team"]), use_container_width=True, hide_index=True)
    with tabs[6]:
        streak_tabs = st.tabs(
            [
                "Win streaks",
                "Unbeaten streaks",
                "No-win streaks",
                "Loss streaks",
                "Category win streaks",
                "Category loss streaks",
            ]
        )
        if not streak_df.empty:
            mapping = {
                "Win streak": 0,
                "Unbeaten streak": 1,
                "No-win streak": 2,
                "Loss streak": 3,
            }
            for streak_type, tab_index in mapping.items():
                with streak_tabs[tab_index]:
                    subset = (
                        streak_df[streak_df["streak_type"] == streak_type]
                        .sort_values(["streak_length", "start_gw"], ascending=[False, True])
                        .head(10)
                    )
                    st.dataframe(
                        style_team_columns(
                            pretty_df(
                                subset[["fantrax_team_name", "streak_length", "start_gw", "end_gw"]].rename(
                                    columns={
                                        "streak_length": "streak length",
                                        "start_gw": "start Jaar-GW",
                                        "end_gw": "end Jaar-GW",
                                    }
                                )
                            ),
                            ["Fantrax team"],
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )
        else:
            for idx in range(4):
                with streak_tabs[idx]:
                    st.info("Geen streakdata gevonden.")
        with streak_tabs[4]:
            if category_win_streaks.empty:
                st.info("Geen category win streaks gevonden.")
            else:
                for stat_key in sorted(category_win_streaks["stat_key"].dropna().unique().tolist()):
                    st.markdown(f"##### {stat_key}")
                    subset = (
                        category_win_streaks[category_win_streaks["stat_key"] == stat_key]
                        .sort_values(["streak_length", "start_gw"], ascending=[False, True])
                        .head(5)
                    )
                    st.dataframe(
                        style_team_columns(
                            pretty_df(
                                subset[
                                    ["fantrax_team_name", "streak_length", "start_gw", "end_gw"]
                                ].rename(
                                    columns={
                                        "streak_length": "streak length",
                                        "start_gw": "start Jaar-GW",
                                        "end_gw": "end Jaar-GW",
                                    }
                                )
                            ),
                            ["Fantrax team"],
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )
        with streak_tabs[5]:
            if category_loss_streaks.empty:
                st.info("Geen category loss streaks gevonden.")
            else:
                for stat_key in sorted(category_loss_streaks["stat_key"].dropna().unique().tolist()):
                    st.markdown(f"##### {stat_key}")
                    subset = (
                        category_loss_streaks[category_loss_streaks["stat_key"] == stat_key]
                        .sort_values(["streak_length", "start_gw"], ascending=[False, True])
                        .head(5)
                    )
                    st.dataframe(
                        style_team_columns(
                            pretty_df(
                                subset[
                                    ["fantrax_team_name", "streak_length", "start_gw", "end_gw"]
                                ].rename(
                                    columns={
                                        "streak_length": "streak length",
                                        "start_gw": "start Jaar-GW",
                                        "end_gw": "end Jaar-GW",
                                    }
                                )
                            ),
                            ["Fantrax team"],
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )


def render_draft_history(db_path: str) -> None:
    st.subheader("Draft History")
    draft_df = load_draft_value_df(db_path)
    if draft_df.empty:
        st.info("Geen draftdata gevonden.")
        return
    draft_df = draft_df[draft_df["is_goalkeeper"] == 0].copy()

    seasons = sorted(draft_df["season_slug"].dropna().unique().tolist())
    selected_season = st.selectbox("Season", seasons, index=len(seasons) - 1)
    teams = sorted(draft_df[draft_df["season_slug"] == selected_season]["fantrax_team_name"].dropna().unique().tolist())
    selected_team = st.selectbox("Team filter", ["All teams"] + teams)
    pl_teams = sorted(draft_df[draft_df["season_slug"] == selected_season]["premier_league_team_code"].dropna().unique().tolist())
    selected_pl_team = st.selectbox("PL team filter", ["All PL teams"] + pl_teams)

    season_draft = draft_df[draft_df["season_slug"] == selected_season].copy()
    if selected_team != "All teams":
        season_draft = season_draft[season_draft["fantrax_team_name"] == selected_team]
    if selected_pl_team != "All PL teams":
        season_draft = season_draft[season_draft["premier_league_team_code"] == selected_pl_team]

    draft_board = season_draft.sort_values(["overall_pick"], ascending=[True], na_position="last")[
        [
            "overall_pick",
            "round_number",
            "pick_in_round",
            "fantrax_team_name",
            "player_name",
            "position",
            "premier_league_team_code",
            "drafted_score",
            "score_rank",
            "draft_value",
            "dropped_in_gw",
        ]
    ]
    st.caption("Draft value = draftpositie minus eindrang op seizoensscore. Een hogere waarde betekent dus meer surplus ten opzichte van waar iemand gekozen werd.")
    st.markdown("#### Draft board")
    st.dataframe(style_team_columns(pretty_df(draft_board), ["Fantrax team"]), use_container_width=True, hide_index=True)


def render_overview(db_path: str) -> None:
    st.subheader("Overview")
    st.caption("Snelle startpagina met links naar de tabjes en 10 willekeurige recordspotlights.")

    sections = [
        ("Team Profile", "Teamprofiel, beste XI, roster-archief, draft/claim-kwaliteit en PL-team footprint."),
        ("Stat Explorer", "Doorzoek en sorteer spelerscores per gameweek, seizoen of carrière."),
        ("Player Card", "Diep profiel per speler: beste weeks, ownership history en radarprofiel."),
        ("Ranking History", "Ontwikkeling van ranking en standings points door de tijd."),
        ("Head-to-Head", "Onderlinge resultaten, matchupgeschiedenis en angstgegners."),
        ("Transactions", "Claims, drops en biedingen met filters per seizoen, team en positie."),
        ("Trade Lab", "Trade-overzichten, chronologische trade log en impact na trade."),
        ("Waiver Lab", "Beste claims, duurste pickups en waiver leaderboards."),
        ("Records", "Hall of fame met teamrecords, spelerrecords, draft value, streaks en all-time XI."),
        ("Draft History", "Draft board per seizoen met draft value, score rank en drop-GW."),
        ("All-Time Standings", "Reguliere seizoensstand aller tijden."),
    ]
    st.markdown("#### Pagina's")
    for start in range(0, len(sections), 2):
        row = sections[start : start + 2]
        cols = st.columns(len(row))
        for col, (page_name, description) in zip(cols, row):
            with col:
                st.markdown(
                    f"""
                    <div class="journeyman-card">
                        <div class="journeyman-title">{page_name}</div>
                        <div>{description}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                if st.button(f"Open {page_name}", key=f"overview_nav_{page_name}", use_container_width=True):
                    navigate_to(page_name)

    record_pool: list[dict[str, str]] = []
    lightweight_queries = [
        {
            "section": "Records > Team records",
            "title": "Highest team matchup score",
            "sql": """
                SELECT fantrax_team_name, opponent_name, season_slug, gameweek, matchup_points_for, category_wins_for
                FROM (
                    SELECT
                        season_slug,
                        gameweek,
                        team_a_name AS fantrax_team_name,
                        team_b_name AS opponent_name,
                        team_a_points AS matchup_points_for,
                        team_a_wins AS category_wins_for
                    FROM matchup
                    UNION ALL
                    SELECT
                        season_slug,
                        gameweek,
                        team_b_name AS fantrax_team_name,
                        team_a_name AS opponent_name,
                        team_b_points AS matchup_points_for,
                        team_b_wins AS category_wins_for
                    FROM matchup
                )
                WHERE opponent_name <> 'None/Bye'
                ORDER BY matchup_points_for DESC, category_wins_for DESC
                LIMIT 1
            """,
            "detail": lambda row: (
                normalize_team_name(row["fantrax_team_name"]),
                f"{strip_trailing_zero_text(row['matchup_points_for'])} vs {normalize_team_name(row['opponent_name'])} in {row['season_slug']} GW{int(row['gameweek']):02d}",
            ),
        },
        {
            "section": "Records > Transactions",
            "title": "Highest bid",
            "sql": """
                SELECT fantrax_team_name, player_name, season_slug, gameweek, bid_amount
                FROM transaction_event
                WHERE COALESCE(bid_amount, 0) > 0
                ORDER BY bid_amount DESC, season_slug, gameweek
                LIMIT 1
            """,
            "detail": lambda row: (
                normalize_team_name(row["fantrax_team_name"]),
                f"{format_decimal_text(row['bid_amount'], 0)} for {row['player_name']} in {row['season_slug']} GW{int(row['gameweek']):02d}",
            ),
        },
        {
            "section": "Records > Draft value",
            "title": "Best draft value",
            "sql": """
                WITH drops AS (
                    SELECT
                        season_slug,
                        player_name,
                        MIN(gameweek) AS dropped_in_gw
                    FROM transaction_event
                    WHERE transaction_type = 'Drop'
                    GROUP BY season_slug, player_name
                ),
                scored AS (
                    SELECT
                        d.season_slug,
                        d.round_number,
                        d.pick_in_round,
                        d.overall_pick,
                        d.fantrax_team_name,
                        d.player_id,
                        d.player_name,
                        d.position,
                        d.premier_league_team_code,
                        ps.score AS drafted_score,
                        drops.dropped_in_gw
                    FROM draft_event d
                    LEFT JOIN player_season ps
                      ON ps.player_id = d.player_id
                     AND ps.season_slug = d.season_slug
                    LEFT JOIN drops
                      ON drops.season_slug = d.season_slug
                     AND drops.player_name = d.player_name
                ),
                outfield_ranked AS (
                    SELECT
                        season_slug,
                        overall_pick,
                        RANK() OVER (
                            PARTITION BY season_slug
                            ORDER BY drafted_score DESC
                        ) AS score_rank
                    FROM scored
                    WHERE INSTR(COALESCE(position, ''), 'G') = 0
                      AND drafted_score IS NOT NULL
                )
                SELECT
                    s.fantrax_team_name,
                    s.player_name,
                    s.season_slug,
                    s.overall_pick,
                    CASE WHEN o.score_rank IS NULL THEN NULL ELSE s.overall_pick - o.score_rank END AS draft_value,
                    s.drafted_score,
                    CASE WHEN INSTR(COALESCE(s.position, ''), 'G') > 0 THEN 1 ELSE 0 END AS is_goalkeeper
                FROM scored s
                LEFT JOIN outfield_ranked o
                  ON o.season_slug = s.season_slug
                 AND o.overall_pick = s.overall_pick
                WHERE is_goalkeeper = 0 AND draft_value IS NOT NULL
                ORDER BY draft_value DESC, drafted_score DESC
                LIMIT 1
            """,
            "detail": lambda row: (
                row["player_name"],
                f"{normalize_team_name(row['fantrax_team_name'])} at pick {int(row['overall_pick'])} in {row['season_slug']} (value {strip_trailing_zero_text(row['draft_value'])})",
            ),
        },
    ]

    stat_queries = [
        ("G", "Goals"),
        ("SOT", "Shots on target"),
        ("KP", "Key passes"),
        ("TkW", "Tackles won"),
        ("CSD", "Clean sheet defensive"),
        ("AP", "Accurate passes"),
        ("AER", "Aerials"),
        ("CoSF", "Corners/freekicks"),
        ("Sv%", "Save percentage"),
        ("DPt2", "Defensive points"),
    ]
    for stat_key, stat_title in stat_queries:
        sql = f"""
            SELECT
                p.player_name,
                pg.fantrax_team_name,
                mr.opponent_name,
                pg.season_slug,
                pg.gameweek,
                pgs.stat_value
            FROM player_gameweek_stat pgs
            JOIN player_gameweek pg
              ON pg.player_id = pgs.player_id
             AND pg.season_slug = pgs.season_slug
             AND pg.gameweek = pgs.gameweek
            JOIN player p
              ON p.player_id = pg.player_id
            LEFT JOIN (
                SELECT
                    season_slug,
                    gameweek,
                    team_a_name AS fantrax_team_name,
                    team_b_name AS opponent_name
                FROM matchup
                UNION ALL
                SELECT
                    season_slug,
                    gameweek,
                    team_b_name AS fantrax_team_name,
                    team_a_name AS opponent_name
                FROM matchup
            ) mr
              ON mr.season_slug = pg.season_slug
             AND mr.gameweek = pg.gameweek
             AND mr.fantrax_team_name = pg.fantrax_team_name
            WHERE pgs.stat_key = '{stat_key}'
              AND pg.fantrax_team_name NOT IN ('FA', 'UNKNOWN', 'None/Bye')
            ORDER BY pgs.stat_value DESC, pg.season_slug, pg.gameweek
            LIMIT 1
        """
        lightweight_queries.append(
            {
                "section": "Records > Player stat top 5",
                "title": stat_title,
                "sql": sql,
                "detail": lambda row, stat_key=stat_key: (
                    row["player_name"],
                    f"{strip_trailing_zero_text(row['stat_value'])} ({stat_key}) for {normalize_team_name(row['fantrax_team_name'])} vs {normalize_team_name(row['opponent_name'])} in {row['season_slug']} GW{int(row['gameweek']):02d}",
                ),
            }
        )

    for record in lightweight_queries:
        result = run_query(db_path, record["sql"])
        if result.empty:
            continue
        row = result.iloc[0]
        winner, detail = record["detail"](row)
        record_pool.append(
            {
                "section": record["section"],
                "title": record["title"],
                "winner": winner,
                "detail": detail,
            }
        )

    if not record_pool:
        st.info("Geen records gevonden.")
        return

    spotlight = random.sample(record_pool, min(10, len(record_pool)))
    st.markdown("#### 10 willekeurige records")
    for start in range(0, len(spotlight), 2):
        row = spotlight[start : start + 2]
        cols = st.columns(len(row))
        for col, card in zip(cols, row):
            with col:
                st.markdown(
                    f"""
                    <div class="journeyman-card">
                        <div class="journeyman-meta">{card['section']}</div>
                        <div class="journeyman-title">{card['title']}</div>
                        <div style="margin-top:0.35rem;font-weight:600;">{card['winner']}</div>
                        <div>{card['detail']}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                if st.button(f"Open {card['title']}", key=f"overview_record_{start}_{card['title']}", use_container_width=True):
                    navigate_to_record(card["section"], card["title"], card["detail"])


def main() -> None:
    st.set_page_config(
        page_title="Fantrax Dashboard",
        page_icon="⚽",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_css()

    st.sidebar.title("Fantrax Dashboard")
    db_path = st.sidebar.text_input("SQLite database", str(DEFAULT_DB_PATH))
    page = st.sidebar.radio(
        "View",
        [
            "Overview",
            "Team Profile",
            "Stat Explorer",
            "Player Card",
            "Ranking History",
            "Head-to-Head",
            "Journeymen",
            "Transactions",
            "Trade Lab",
            "Waiver Lab",
            "Records",
            "Draft History",
            "All-Time Standings",
        ],
        key="page_radio",
    )

    render_header()

    if not Path(db_path).exists():
        st.error(f"Database niet gevonden: {db_path}")
        return

    if page == "Overview":
        render_overview(db_path)
    elif page == "Team Profile":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_team_profile(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Stat Explorer":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_stat_explorer(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Player Card":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_player_card(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Ranking History":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_ranking_history(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Head-to-Head":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_head_to_head(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Journeymen":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_journeymen(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Transactions":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_transactions(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Trade Lab":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_trade_lab(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Waiver Lab":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_waiver_lab(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Records":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_records(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "Draft History":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_draft_history(db_path)
        st.markdown("</div>", unsafe_allow_html=True)
    elif page == "All-Time Standings":
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        render_all_time_standings(db_path)
        st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
