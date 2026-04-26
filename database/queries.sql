-- Hoogste algemene seizoen-score per speler over alle seizoenen
SELECT
    v.player_name,
    v.best_score
FROM v_player_best_score v
ORDER BY v.best_score DESC, v.player_name;

-- Hoogste waarde per speler voor een specifieke stat over alle seizoenen
-- Vervang 'G' door een andere stat_key zoals 'KP', 'SOT', 'TkW', 'AER'
SELECT
    s.player_name,
    s.best_stat_value
FROM v_player_best_stat s
WHERE s.stat_key = 'G'
ORDER BY s.best_stat_value DESC, s.player_name;

-- Beste seizoen per speler voor een specifieke stat, inclusief seizoen en club
SELECT
    p.player_name,
    pss.stat_key,
    pss.stat_value,
    pss.season_slug,
    ps.premier_league_team_code,
    ps.position
FROM player_season_stat pss
JOIN player p ON p.player_id = pss.player_id
JOIN player_season ps
    ON ps.player_id = pss.player_id
   AND ps.season_slug = pss.season_slug
WHERE pss.stat_key = 'G'
  AND pss.stat_value IS NOT NULL
  AND pss.stat_value = (
      SELECT MAX(pss2.stat_value)
      FROM player_season_stat pss2
      WHERE pss2.player_id = pss.player_id
        AND pss2.stat_key = pss.stat_key
  )
ORDER BY pss.stat_value DESC, p.player_name, pss.season_slug;

-- All-time recordhouder per stat
SELECT
    leaders.stat_key,
    leaders.player_name,
    leaders.stat_value,
    leaders.season_slug,
    leaders.premier_league_team_code
FROM (
    SELECT
        pss.stat_key,
        p.player_name,
        pss.stat_value,
        pss.season_slug,
        ps.premier_league_team_code,
        ROW_NUMBER() OVER (
            PARTITION BY pss.stat_key
            ORDER BY pss.stat_value DESC, p.player_name, pss.season_slug
        ) AS rn
    FROM player_season_stat pss
    JOIN player p ON p.player_id = pss.player_id
    JOIN player_season ps
        ON ps.player_id = pss.player_id
       AND ps.season_slug = pss.season_slug
    WHERE pss.stat_value IS NOT NULL
) AS leaders
WHERE leaders.rn = 1
ORDER BY leaders.stat_key;
