# Fantrax Database

Startpunt voor een Fantrax-database en later een dashboard met historische statistieken.

## Doel

Deze repo begint met een veilige, praktische eerste stap:

1. standings-data ophalen met een ingelogde browsersessie
2. ruwe JSON per periode opslaan
3. JSON normaliseren naar records die later in een database geladen kunnen worden

## Structuur

- `collector/`: scripts om data uit Fantrax op te halen
- `parser/`: scripts om ruwe JSON om te zetten naar gestructureerde output
- `raw/`: opgeslagen Fantrax responses
- `exports/`: genormaliseerde JSON/CSV output
- `docs/`: technische notities en datamodel

## Eerste flow

1. Installeer Python 3.11+.
2. Maak een virtual environment.
3. Installeer dependencies met `pip install -r requirements.txt`.
4. Installeer Playwright browser binaries met `python -m playwright install chromium`.
5. Run de collector:

```powershell
python collector\capture_standings.py --league-id 4w8wz9damde4avqn --season-slug 2025-2026
```

6. Log handmatig in wanneer de browser opent.
7. Navigeer naar standings en laat de collector per periode JSON opslaan.
8. Parse de ruwe data:

```powershell
python parser\parse_standings.py --input raw\2025-2026\standings --output exports\2025-2026\standings
```

## Belangrijke noot

De huidige setup is gebouwd rond jullie rechtmatige toegang tot de league. De collector automatiseert alleen een bestaande ingelogde sessie.

## Alternatieve Start: Spelersdatabase

Omdat spelers-CSV's makkelijker beschikbaar zijn, is er ook een SQLite-importpad voor seizoenstats van spelers.

Import alle `players*.csv` bestanden:

```powershell
python database\import_players.py --input-dir downloaddata --db-path database\fantrax_v2.db
```

Meer context:

- [docs/player-database.md](C:\Users\kejes\FantraxDatabase\docs\player-database.md)
- [database/schema.sql](C:\Users\kejes\FantraxDatabase\database\schema.sql)
- [database/import_players.py](C:\Users\kejes\FantraxDatabase\database\import_players.py)
- [database/import_player_gameweeks.py](C:\Users\kejes\FantraxDatabase\database\import_player_gameweeks.py)
- [database/queries.sql](C:\Users\kejes\FantraxDatabase\database\queries.sql)
- [database/import_matchups_text.py](C:\Users\kejes\FantraxDatabase\database\import_matchups_text.py)
- [collector/download_player_gameweek_csvs.py](C:\Users\kejes\FantraxDatabase\collector\download_player_gameweek_csvs.py)
- [analysis/fit_score_proxy.py](C:\Users\kejes\FantraxDatabase\analysis\fit_score_proxy.py)

Bulk-download van player gameweek CSVs:

```powershell
python collector\download_player_gameweek_csvs.py --league-id 4w8wz9damde4avqn --season-slug 2025-2026 --season-projection SEASON_925_BY_PERIOD --start-date 2025-08-15 --end-date 2026-04-25 --team-id cjkfcpwzmde4avqx --period-end 38
```

Helper om seizoenparameters van de huidige Fantrax pagina uit te lezen:

```powershell
python collector\extract_player_stats_page_params.py
```

Import van player gameweek CSVs:

```powershell
python database\import_player_gameweeks.py --input-root downloaddata\player_gameweeks --db-path database\fantrax_v2.db
```

Import van transactiegeschiedenis:

```powershell
python database\import_transactions.py --input-dir downloaddata --db-path database\fantrax_v2.db
```

Import van tradegeschiedenis:

```powershell
python database\import_trades.py --input-dir downloaddata --db-path database\fantrax_v2.db
```

Historische Fantrax-owner per speler/gameweek backfillen:

```powershell
python database\backfill_player_gameweek_ownership.py --db-path database\fantrax_v2.db
```

Proxy-formule voor Fantrax Score fitten:

```powershell
python analysis\fit_score_proxy.py --input-dir downloaddata --output-json exports\score_proxy_models.json
```

## Frontend

Er staat nu ook een eerste Streamlit-dashboard bovenop de SQLite database.

Installeer dependencies:

```powershell
pip install -r requirements.txt
```

Start het dashboard:

```powershell
streamlit run frontend\app.py
```

De app gebruikt standaard:

- [database/fantrax_v2.db](C:\Users\kejes\FantraxDatabase\database\fantrax_v2.db)

Eerste schermen:

- overview met kerncijfers
- all-time standings
- top spelers per Fantrax-team
- journeymen
- speler ownership timeline
