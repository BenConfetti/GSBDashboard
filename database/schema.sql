PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS player (
    player_id TEXT PRIMARY KEY,
    player_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS season (
    season_slug TEXT PRIMARY KEY,
    start_year INTEGER NOT NULL,
    end_year INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS premier_league_team (
    team_code TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS fantasy_team (
    fantasy_team_id TEXT PRIMARY KEY,
    team_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS player_season (
    player_id TEXT NOT NULL,
    season_slug TEXT NOT NULL,
    premier_league_team_code TEXT,
    position TEXT,
    rank_overall INTEGER,
    score REAL,
    games_played INTEGER,
    adp REAL,
    raw_row_json TEXT NOT NULL,
    PRIMARY KEY (player_id, season_slug),
    FOREIGN KEY (player_id) REFERENCES player(player_id),
    FOREIGN KEY (season_slug) REFERENCES season(season_slug),
    FOREIGN KEY (premier_league_team_code) REFERENCES premier_league_team(team_code)
);

CREATE TABLE IF NOT EXISTS stat_category (
    stat_key TEXT PRIMARY KEY,
    first_seen_season_slug TEXT NOT NULL,
    FOREIGN KEY (first_seen_season_slug) REFERENCES season(season_slug)
);

CREATE TABLE IF NOT EXISTS player_season_stat (
    player_id TEXT NOT NULL,
    season_slug TEXT NOT NULL,
    stat_key TEXT NOT NULL,
    stat_value REAL,
    raw_value TEXT NOT NULL,
    PRIMARY KEY (player_id, season_slug, stat_key),
    FOREIGN KEY (player_id) REFERENCES player(player_id),
    FOREIGN KEY (season_slug) REFERENCES season(season_slug),
    FOREIGN KEY (stat_key) REFERENCES stat_category(stat_key)
);

CREATE INDEX IF NOT EXISTS idx_player_season_season
    ON player_season (season_slug);

CREATE INDEX IF NOT EXISTS idx_player_season_team
    ON player_season (premier_league_team_code);

CREATE INDEX IF NOT EXISTS idx_player_season_stat_season
    ON player_season_stat (season_slug);

CREATE INDEX IF NOT EXISTS idx_player_season_stat_key
    ON player_season_stat (stat_key);

CREATE TABLE IF NOT EXISTS player_gameweek (
    player_id TEXT NOT NULL,
    season_slug TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    premier_league_team_code TEXT,
    position TEXT,
    rank_overall INTEGER,
    score REAL,
    games_played INTEGER,
    status_snapshot TEXT,
    fantrax_team_name TEXT,
    opponent_snapshot TEXT,
    ros TEXT,
    plus_minus TEXT,
    raw_row_json TEXT NOT NULL,
    PRIMARY KEY (player_id, season_slug, gameweek),
    FOREIGN KEY (player_id) REFERENCES player(player_id),
    FOREIGN KEY (season_slug, gameweek) REFERENCES gameweek(season_slug, gameweek),
    FOREIGN KEY (premier_league_team_code) REFERENCES premier_league_team(team_code)
);

CREATE TABLE IF NOT EXISTS player_gameweek_stat (
    player_id TEXT NOT NULL,
    season_slug TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    stat_key TEXT NOT NULL,
    stat_value REAL,
    raw_value TEXT NOT NULL,
    PRIMARY KEY (player_id, season_slug, gameweek, stat_key),
    FOREIGN KEY (player_id, season_slug, gameweek)
        REFERENCES player_gameweek(player_id, season_slug, gameweek)
        ON DELETE CASCADE,
    FOREIGN KEY (stat_key) REFERENCES stat_category(stat_key)
);

CREATE INDEX IF NOT EXISTS idx_player_gameweek_season
    ON player_gameweek (season_slug, gameweek);

CREATE INDEX IF NOT EXISTS idx_player_gameweek_team
    ON player_gameweek (premier_league_team_code);

CREATE INDEX IF NOT EXISTS idx_player_gameweek_fantrax_team
    ON player_gameweek (fantrax_team_name);

CREATE INDEX IF NOT EXISTS idx_player_gameweek_stat_key
    ON player_gameweek_stat (stat_key);

CREATE INDEX IF NOT EXISTS idx_player_gameweek_stat_season
    ON player_gameweek_stat (season_slug, gameweek);

CREATE TABLE IF NOT EXISTS transaction_event (
    transaction_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    season_slug TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    player_name TEXT NOT NULL,
    premier_league_team_code TEXT,
    position TEXT,
    transaction_type TEXT NOT NULL,
    fantrax_team_name TEXT,
    bid_amount REAL,
    priority_value INTEGER,
    transaction_datetime_text TEXT,
    transaction_datetime_iso TEXT,
    raw_row_json TEXT NOT NULL,
    UNIQUE (
        season_slug,
        gameweek,
        player_name,
        transaction_type,
        fantrax_team_name,
        transaction_datetime_text
    ),
    FOREIGN KEY (season_slug, gameweek) REFERENCES gameweek(season_slug, gameweek),
    FOREIGN KEY (season_slug) REFERENCES season(season_slug),
    FOREIGN KEY (premier_league_team_code) REFERENCES premier_league_team(team_code)
);

CREATE INDEX IF NOT EXISTS idx_transaction_event_season_gameweek
    ON transaction_event (season_slug, gameweek);

CREATE INDEX IF NOT EXISTS idx_transaction_event_player
    ON transaction_event (player_name);

CREATE INDEX IF NOT EXISTS idx_transaction_event_team
    ON transaction_event (fantrax_team_name);

CREATE INDEX IF NOT EXISTS idx_transaction_event_type
    ON transaction_event (transaction_type);

CREATE TABLE IF NOT EXISTS trade_event (
    trade_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    season_slug TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    player_name TEXT NOT NULL,
    premier_league_team_code TEXT,
    position TEXT,
    from_fantrax_team_name TEXT,
    to_fantrax_team_name TEXT,
    transaction_datetime_text TEXT,
    transaction_datetime_iso TEXT,
    raw_row_json TEXT NOT NULL,
    UNIQUE (
        season_slug,
        gameweek,
        player_name,
        from_fantrax_team_name,
        to_fantrax_team_name,
        transaction_datetime_text
    ),
    FOREIGN KEY (season_slug, gameweek) REFERENCES gameweek(season_slug, gameweek),
    FOREIGN KEY (season_slug) REFERENCES season(season_slug),
    FOREIGN KEY (premier_league_team_code) REFERENCES premier_league_team(team_code)
);

CREATE INDEX IF NOT EXISTS idx_trade_event_season_gameweek
    ON trade_event (season_slug, gameweek);

CREATE INDEX IF NOT EXISTS idx_trade_event_player
    ON trade_event (player_name);

CREATE INDEX IF NOT EXISTS idx_trade_event_from_team
    ON trade_event (from_fantrax_team_name);

CREATE INDEX IF NOT EXISTS idx_trade_event_to_team
    ON trade_event (to_fantrax_team_name);

CREATE TABLE IF NOT EXISTS draft_event (
    draft_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    season_slug TEXT NOT NULL,
    round_number INTEGER NOT NULL,
    pick_in_round INTEGER NOT NULL,
    overall_pick INTEGER NOT NULL,
    fantrax_team_name TEXT NOT NULL,
    player_id TEXT,
    player_name TEXT NOT NULL,
    position TEXT,
    premier_league_team_code TEXT,
    raw_player_team_text TEXT,
    raw_row_json TEXT NOT NULL,
    UNIQUE (season_slug, overall_pick),
    FOREIGN KEY (season_slug) REFERENCES season(season_slug),
    FOREIGN KEY (player_id) REFERENCES player(player_id),
    FOREIGN KEY (premier_league_team_code) REFERENCES premier_league_team(team_code)
);

CREATE INDEX IF NOT EXISTS idx_draft_event_season
    ON draft_event (season_slug, overall_pick);

CREATE INDEX IF NOT EXISTS idx_draft_event_team
    ON draft_event (fantrax_team_name);

CREATE INDEX IF NOT EXISTS idx_draft_event_player
    ON draft_event (player_name);

CREATE TABLE IF NOT EXISTS gameweek (
    season_slug TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    date_range_label TEXT,
    is_playoff INTEGER NOT NULL DEFAULT 0,
    stage_label TEXT,
    PRIMARY KEY (season_slug, gameweek),
    FOREIGN KEY (season_slug) REFERENCES season(season_slug)
);

CREATE TABLE IF NOT EXISTS stat_weight (
    stat_key TEXT PRIMARY KEY,
    points_for_win REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS matchup (
    matchup_id INTEGER PRIMARY KEY AUTOINCREMENT,
    season_slug TEXT NOT NULL,
    gameweek INTEGER NOT NULL,
    is_playoff INTEGER NOT NULL DEFAULT 0,
    team_a_name TEXT NOT NULL,
    team_b_name TEXT NOT NULL,
    team_a_points REAL NOT NULL,
    team_b_points REAL NOT NULL,
    team_a_wins REAL,
    team_a_losses REAL,
    team_a_ties REAL,
    team_b_wins REAL,
    team_b_losses REAL,
    team_b_ties REAL,
    source_text TEXT,
    FOREIGN KEY (season_slug, gameweek) REFERENCES gameweek(season_slug, gameweek)
);

CREATE TABLE IF NOT EXISTS matchup_stat_result (
    matchup_id INTEGER NOT NULL,
    stat_key TEXT NOT NULL,
    team_a_value REAL,
    team_b_value REAL,
    winner_side TEXT CHECK (winner_side IN ('A', 'B', 'TIE')),
    points_for_win REAL NOT NULL,
    PRIMARY KEY (matchup_id, stat_key),
    FOREIGN KEY (matchup_id) REFERENCES matchup(matchup_id) ON DELETE CASCADE,
    FOREIGN KEY (stat_key) REFERENCES stat_category(stat_key)
);

CREATE INDEX IF NOT EXISTS idx_gameweek_season
    ON gameweek (season_slug, gameweek);

CREATE INDEX IF NOT EXISTS idx_matchup_season_gameweek
    ON matchup (season_slug, gameweek);

CREATE INDEX IF NOT EXISTS idx_matchup_playoff
    ON matchup (is_playoff);

CREATE INDEX IF NOT EXISTS idx_matchup_team_a
    ON matchup (team_a_name);

CREATE INDEX IF NOT EXISTS idx_matchup_team_b
    ON matchup (team_b_name);

CREATE INDEX IF NOT EXISTS idx_matchup_stat_key
    ON matchup_stat_result (stat_key);

INSERT INTO stat_weight (stat_key, points_for_win)
VALUES ('G', 2.0)
ON CONFLICT (stat_key) DO UPDATE SET
    points_for_win = excluded.points_for_win;

CREATE VIEW IF NOT EXISTS v_player_best_score AS
SELECT
    ps.player_id,
    p.player_name,
    MAX(ps.score) AS best_score
FROM player_season ps
JOIN player p ON p.player_id = ps.player_id
GROUP BY ps.player_id, p.player_name;

CREATE VIEW IF NOT EXISTS v_player_best_stat AS
SELECT
    pss.stat_key,
    pss.player_id,
    p.player_name,
    MAX(pss.stat_value) AS best_stat_value
FROM player_season_stat pss
JOIN player p ON p.player_id = pss.player_id
WHERE pss.stat_value IS NOT NULL
GROUP BY pss.stat_key, pss.player_id, p.player_name;

CREATE VIEW IF NOT EXISTS v_all_time_team_standings AS
WITH team_matchups AS (
    SELECT
        m.season_slug,
        m.gameweek,
        m.is_playoff,
        m.team_a_name AS team_name,
        m.team_b_name AS opponent_name,
        m.team_a_points AS matchup_points_for,
        m.team_b_points AS matchup_points_against,
        m.team_a_wins AS category_wins,
        m.team_a_losses AS category_losses,
        m.team_a_ties AS category_ties,
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
        m.team_a_name AS opponent_name,
        m.team_b_points AS matchup_points_for,
        m.team_a_points AS matchup_points_against,
        m.team_b_wins AS category_wins,
        m.team_b_losses AS category_losses,
        m.team_b_ties AS category_ties,
        CASE
            WHEN m.team_b_points > m.team_a_points THEN 3
            WHEN m.team_b_points = m.team_a_points THEN 1
            ELSE 0
        END AS standings_points_earned
    FROM matchup m
),
team_stats AS (
    SELECT
        m.team_a_name AS team_name,
        msr.stat_key,
        msr.team_a_value AS stat_value
    FROM matchup_stat_result msr
    JOIN matchup m ON m.matchup_id = msr.matchup_id
    WHERE msr.team_a_value IS NOT NULL

    UNION ALL

    SELECT
        m.team_b_name AS team_name,
        msr.stat_key,
        msr.team_b_value AS stat_value
    FROM matchup_stat_result msr
    JOIN matchup m ON m.matchup_id = msr.matchup_id
    WHERE msr.team_b_value IS NOT NULL
),
team_stat_totals AS (
    SELECT
        team_name,
        AVG(CASE WHEN stat_key = 'Sv%' THEN stat_value END) AS avg_sv_pct,
        SUM(CASE WHEN stat_key <> 'Sv%' THEN stat_value ELSE 0 END) AS total_non_sv_stats,
        SUM(CASE WHEN stat_key = 'G' THEN stat_value ELSE 0 END) AS total_g,
        SUM(CASE WHEN stat_key = 'A' THEN stat_value ELSE 0 END) AS total_a,
        SUM(CASE WHEN stat_key = 'SOT' THEN stat_value ELSE 0 END) AS total_sot,
        SUM(CASE WHEN stat_key = 'KP' THEN stat_value ELSE 0 END) AS total_kp,
        SUM(CASE WHEN stat_key = 'TkW' THEN stat_value ELSE 0 END) AS total_tkw,
        SUM(CASE WHEN stat_key = 'CSD' THEN stat_value ELSE 0 END) AS total_csd,
        SUM(CASE WHEN stat_key = 'AP' THEN stat_value ELSE 0 END) AS total_ap,
        SUM(CASE WHEN stat_key = 'AER' THEN stat_value ELSE 0 END) AS total_aer,
        SUM(CASE WHEN stat_key = 'CoSF' THEN stat_value ELSE 0 END) AS total_cosf,
        SUM(CASE WHEN stat_key = 'DPt2' THEN stat_value ELSE 0 END) AS total_dpt2,
        SUM(CASE WHEN stat_key = 'BR' THEN stat_value ELSE 0 END) AS total_br,
        SUM(CASE WHEN stat_key = 'Int' THEN stat_value ELSE 0 END) AS total_int
    FROM team_stats
    GROUP BY team_name
)
SELECT
    tm.team_name,
    COUNT(*) AS matches_played,
    SUM(tm.standings_points_earned) AS standings_points,
    SUM(CASE WHEN tm.standings_points_earned = 3 THEN 1 ELSE 0 END) AS matchup_wins,
    SUM(CASE WHEN tm.standings_points_earned = 1 THEN 1 ELSE 0 END) AS matchup_draws,
    SUM(CASE WHEN tm.standings_points_earned = 0 THEN 1 ELSE 0 END) AS matchup_losses,
    SUM(tm.matchup_points_for) AS total_matchup_points_for,
    SUM(tm.matchup_points_against) AS total_matchup_points_against,
    SUM(tm.category_wins) AS total_category_wins,
    SUM(tm.category_losses) AS total_category_losses,
    SUM(tm.category_ties) AS total_category_ties,
    SUM(CASE WHEN tm.is_playoff = 1 THEN 1 ELSE 0 END) AS playoff_matches,
    SUM(CASE WHEN tm.is_playoff = 0 THEN 1 ELSE 0 END) AS regular_season_matches,
    ROUND(tst.avg_sv_pct, 2) AS avg_sv_pct,
    tst.total_non_sv_stats,
    tst.total_g,
    tst.total_a,
    tst.total_sot,
    tst.total_kp,
    tst.total_tkw,
    tst.total_csd,
    tst.total_ap,
    tst.total_aer,
    tst.total_cosf,
    tst.total_dpt2,
    tst.total_br,
    tst.total_int
FROM team_matchups tm
LEFT JOIN team_stat_totals tst
    ON tst.team_name = tm.team_name
GROUP BY
    tm.team_name,
    tst.avg_sv_pct,
    tst.total_non_sv_stats,
    tst.total_g,
    tst.total_a,
    tst.total_sot,
    tst.total_kp,
    tst.total_tkw,
    tst.total_csd,
    tst.total_ap,
    tst.total_aer,
    tst.total_cosf,
    tst.total_dpt2,
    tst.total_br,
    tst.total_int;
