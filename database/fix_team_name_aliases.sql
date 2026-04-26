PRAGMA foreign_keys = ON;

UPDATE matchup
SET team_a_name = 'Athletic Club Bapao'
WHERE team_a_name = 'Athletic de Bapao';

UPDATE matchup
SET team_b_name = 'Athletic Club Bapao'
WHERE team_b_name = 'Athletic de Bapao';

INSERT INTO fantasy_team (fantasy_team_id, team_name)
SELECT 'athletic-club-bapao', 'Athletic Club Bapao'
WHERE NOT EXISTS (
    SELECT 1
    FROM fantasy_team
    WHERE team_name = 'Athletic Club Bapao'
);
