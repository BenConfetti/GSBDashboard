PRAGMA foreign_keys = ON;

-- Fix playoff gameweek numbering so playoff rounds always follow the
-- last regular-season gameweek for the same season.
--
-- Current assumption:
-- Playoffs 1 => last_regular_gameweek + 1
-- Playoffs 2 => last_regular_gameweek + 2
-- Playoffs 3 => last_regular_gameweek + 3

BEGIN TRANSACTION;

DROP TABLE IF EXISTS matchup_gameweek_fix;

CREATE TEMP TABLE matchup_gameweek_fix AS
WITH regular_max AS (
    SELECT
        season_slug,
        COALESCE(MAX(gameweek), 0) AS max_regular_gameweek
    FROM gameweek
    WHERE is_playoff = 0
    GROUP BY season_slug
),
playoff_targets AS (
    SELECT
        g.season_slug,
        g.gameweek AS old_gameweek,
        g.date_range_label,
        g.is_playoff,
        g.stage_label,
        rm.max_regular_gameweek +
            CAST(REPLACE(g.stage_label, 'Playoffs ', '') AS INTEGER) AS new_gameweek
    FROM gameweek g
    JOIN regular_max rm
      ON rm.season_slug = g.season_slug
    WHERE g.is_playoff = 1
      AND g.stage_label LIKE 'Playoffs %'
)
SELECT * FROM playoff_targets;

UPDATE matchup
SET gameweek = (
    SELECT new_gameweek
    FROM matchup_gameweek_fix f
    WHERE f.season_slug = matchup.season_slug
      AND f.old_gameweek = matchup.gameweek
      AND f.is_playoff = 1
)
WHERE is_playoff = 1
  AND EXISTS (
      SELECT 1
      FROM matchup_gameweek_fix f
      WHERE f.season_slug = matchup.season_slug
        AND f.old_gameweek = matchup.gameweek
        AND f.is_playoff = 1
  );

DELETE FROM gameweek
WHERE is_playoff = 1;

INSERT INTO gameweek (season_slug, gameweek, date_range_label, is_playoff, stage_label)
SELECT DISTINCT
    season_slug,
    new_gameweek,
    date_range_label,
    1,
    stage_label
FROM matchup_gameweek_fix;

DROP TABLE matchup_gameweek_fix;

COMMIT;
