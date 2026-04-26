# Player Database

De spelersdatabase gebruikt twee lagen voor seizoeninfo:

## Vaste spelerlaag

- `player`
  Een speler staat hier precies één keer in, op basis van Fantrax `ID`.

## Seizoenlaag

- `player_season`
  Dit bevat de vaste seizoensvelden zoals:
  - Premier League team
  - positie
  - overall rank
  - score
  - games played
  - ADP

## Wisselende statistieklaag

- `player_season_stat`
  Elke statistiek wordt opgeslagen als een aparte rij:
  - `player_id`
  - `season_slug`
  - `stat_key`
  - `stat_value`

Dit is expres flexibel, omdat kolommen per seizoen veranderen.

## Velden die nu bewust niet leidend zijn

- `Status`
  Dit is een momentopname van het Fantrax-team en wordt nu niet gemodelleerd als kerngegeven.
- `Opponent`
  Dit lijkt een momentopname van een matchup-context en niet van de seizoensprestatie.

Beide velden blijven wel bewaard in `raw_row_json` binnen `player_season`, zodat we later niets kwijt zijn.

## Waarom SQLite

SQLite is hier een goede eerste keuze:

- geen aparte server nodig
- makkelijk lokaal te vullen en te inspecteren
- prima voor import, analyse en eerste dashboard-prototypes
- later relatief eenvoudig te migreren naar PostgreSQL als de website daarom vraagt

## Import

```powershell
python database\import_players.py --input-dir downloaddata --db-path database\fantrax_v2.db
```

## Eerste handige queries

Top 20 spelers op score in een seizoen:

```sql
SELECT
  ps.season_slug,
  p.player_name,
  ps.premier_league_team_code,
  ps.position,
  ps.score
FROM player_season ps
JOIN player p ON p.player_id = ps.player_id
WHERE ps.season_slug = '2024-2025'
ORDER BY ps.score DESC
LIMIT 20;
```

Alle beschikbare statcategorieën:

```sql
SELECT stat_key, first_seen_season_slug
FROM stat_category
ORDER BY stat_key;
```

All-time recordhouder per stat:

```sql
SELECT
  leaders.stat_key,
  leaders.player_name,
  leaders.stat_value,
  leaders.season_slug
FROM (
  SELECT
    pss.stat_key,
    p.player_name,
    pss.stat_value,
    pss.season_slug,
    ROW_NUMBER() OVER (
      PARTITION BY pss.stat_key
      ORDER BY pss.stat_value DESC, p.player_name, pss.season_slug
    ) AS rn
  FROM player_season_stat pss
  JOIN player p ON p.player_id = pss.player_id
  WHERE pss.stat_value IS NOT NULL
) leaders
WHERE leaders.rn = 1
ORDER BY leaders.stat_key;
```

Meer voorbeeldqueries staan in:

- [database/queries.sql](C:\Users\kejes\FantraxDatabase\database\queries.sql)

## Matchups

De database bevat nu ook een eerste laag voor historische matchupdata:

- `gameweek`
- `matchup`
- `matchup_stat_result`
- `stat_weight`

Uitgangspunten:

- rijen uit de frontend horen per 2 bij elkaar
- `Pts` is de uiteindelijke matchupscore
- `G` telt altijd voor `2` punten
- alle andere statcategorieën tellen voor `1` punt
- een vaste mapping van fantasy teamnaam naar team-id voegen we later toe

Voorlopig worden matchups daarom opgeslagen met teamnamen als tekst.

Import van geplakte frontend-matchups:

```powershell
python database\import_matchups_text.py --input-file downloaddata\matchups2526_gw01_33.txt --season-slug 2025-2026 --db-path database\fantrax_v2.db
```

Aanname in de importer:

- `DPt2` is de enige categorie waarbij lager beter is
- alle andere categorieën gebruiken hoger is beter
- regels zoals `Gameweek: Playoffs 3` worden automatisch als playoff gemarkeerd
- playoff-rondes worden opgeslagen als `laatste reguliere gameweek + roundnummer`

## Player Gameweeks

De database bevat nu ook weekniveau voor spelers:

- `player_gameweek`
- `player_gameweek_stat`

Daarin bewaren we per speler, per seizoen, per gameweek:

- PL-team in die week
- positie
- rank
- score
- games played
- `Status` als Fantrax momentopname
- `fantrax_team_name` als afgeleide historische owner op basis van transactions
- `Opponent` als snapshot
- alle gameweek-stats als losse rijen

Import:

```powershell
python database\import_player_gameweeks.py --input-root downloaddata\player_gameweeks --db-path database\fantrax_v2.db
```

## Transactions

Voor historische Fantrax-ownership gebruiken we `Transactions*.csv` als bron.

Daarin bewaren we per event:

- seizoen
- gameweek
- spelernaam
- PL-team
- positie
- transactietype (`Claim`, `Drop`, enz.)
- Fantrax-team
- bid
- priority
- datum/tijd als ruwe tekst en als ISO-waarde

Belangrijk uitgangspunt:

- de `Gameweek` uit de transaction export is leidend
- lineup lock modelleren we nu niet apart

Import:

```powershell
python database\import_transactions.py --input-dir downloaddata --db-path database\fantrax_v2.db
```

## Trades

Trades gebruiken we als tweede ownership-bron.

Daarin bewaren we per speler:

- seizoen
- gameweek
- spelernaam
- PL-team
- positie
- `from_fantrax_team_name`
- `to_fantrax_team_name`
- datum/tijd als ruwe tekst en als ISO-waarde

De importer negeert automatisch:

- draft picks
- budget amount regels
- andere niet-spelerrijen zonder PL-team of positie

Import:

```powershell
python database\import_trades.py --input-dir downloaddata --db-path database\fantrax_v2.db
```

Backfill van historische ownership naar `player_gameweek.fantrax_team_name`:

```powershell
python database\backfill_player_gameweek_ownership.py --db-path database\fantrax_v2.db
```

Logica:

- `Claim` maakt de speler eigenaar van het genoemde Fantrax-team vanaf die gameweek
- `Drop` zet de speler vanaf die gameweek op `FA`
- `Trade` verplaatst de speler vanaf die gameweek direct van `from` naar `to`
- als het eerste event in een seizoen een `Drop` is, nemen we aan dat de speler daarvoor
  bij dat team zat
- als het eerste event in een seizoen een `Trade` is, nemen we aan dat de speler daarvoor
  bij het `from`-team zat
- als het eerste event een `Claim` is, nemen we aan dat de speler daarvoor `FA` was
