# Workflow

## 1. Data ophalen

Gebruik `collector/capture_standings.py` om per periode de ruwe Fantrax-response op te slaan.

## 2. Data normaliseren

Gebruik `parser/parse_standings.py` om uit de ruwe responses:

- teams
- standings per periode
- matchupregels per periode

te exporteren naar JSON-bestanden.

## 3. Database

De volgende stap is een kleine database-loader, bijvoorbeeld SQLite of PostgreSQL.

## 4. Dashboard

Pas daarna bouwen we een API en frontend bovenop de genormaliseerde tabellen.
