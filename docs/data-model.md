# Eerste Datamodel

Dit is het minimale model voor standings en gameweek-resultaten.

## Tabellen

### `fantasy_teams`

- `team_id`
- `name`
- `short_name`
- `logo_url`

### `periods`

- `season_slug`
- `period`
- `label`

### `standings_by_period`

- `season_slug`
- `period`
- `team_id`
- `rank`
- `wins`
- `losses`
- `ties`
- `points`
- `win_pct`
- `games_back`
- `category_points_for`
- `category_points_against`
- `streak_raw`

### `matchups_by_period`

- `season_slug`
- `period`
- `matchup_id`
- `team_id`
- `opponent_team_id`
- `is_highlighted_team`
- `wins`
- `losses`
- `ties`
- `category_points`
- `categories_json`

## Opmerking

In de eerste versie bewaren we sommige categorie-data nog als JSON. Dat is expres: eerst data veiligstellen, daarna pas volledig normaliseren.

