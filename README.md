# Borsa Analiz — Updated Setup

This update extends the existing Flask + yfinance + Chart.js architecture with persistent storage, split TRY/USD charts, and a standings system.

## What changed

- Added SQLite database persistence with a schema that can be migrated to PostgreSQL later.
- Added cached `price_history`, `financial_ratios`, and `metadata` storage.
- Added a new USD chart endpoint with optional US market comparisons.
- Added `GET /api/standings?period=1Y&view=gainers|losers|all`.
- Kept the existing endpoints and dashboard structure.

## Files

- `app_updated.py` — updated Flask backend
- `index_updated.html` — updated single-page frontend
- `schema.sql` — database schema
- `requirements_updated.txt` — updated dependencies

## Database schema

### 1) `price_history`
Stores daily historical OHLCV data.

Columns:
- `ticker`
- `datetime`
- `interval`
- `open`, `high`, `low`, `close`, `volume`
- `currency`
- `source`

### 2) `financial_ratios`
Stores quarterly-style ratio snapshots without overwriting older rows.

Columns include:
- `ticker`
- `snapshot_date`
- `pe_ratio`, `forward_pe`, `pb_ratio`, `debt_to_equity`
- `roe`, `roa`, margins
- `ev_ebitda`, `ev_revenue`, and other current ratio fields

### 3) `metadata`
Stores reusable company profile information.

## API summary

Existing endpoints still work:
- `GET /api/stocks/popular`
- `GET /api/stock/<ticker>`
- `GET /api/chart?ticker=&period=&comparisons=&stocks=`
- `GET /api/price?ticker=&period=&currencies=`
- `GET /api/ohlcv?ticker=&period=`

New endpoints:
- `GET /api/usd-chart?ticker=&period=&comparisons=`
- `GET /api/standings?period=1Y&view=gainers`
- `POST /api/admin/backfill`

## Setup

### 1. Install dependencies

```bash
pip install -r requirements_updated.txt
```

### 2. Run the backend

```bash
python app_updated.py
```

The backend runs on `http://localhost:5000`.

### 3. Open the frontend

Open `index_updated.html` directly, or serve it with a local static server:

```bash
python -m http.server 8080
```

Then open `http://localhost:8080/index_updated.html`.

## Optional environment variables

- `BORSA_DB_PATH` — custom SQLite path
- `BORSA_ENABLE_BACKGROUND_SYNC=1` — enable lightweight background refresh
- `BORSA_BACKGROUND_SYNC_SECONDS=43200` — refresh interval, default 12 hours

## Refresh logic

- Prices: daily history is stored in SQLite and refreshed when stale.
- Ratios: refreshed when older than about one quarter.
- Intraday `1D` requests still use live Yahoo data, because Yahoo does not expose full max intraday history in the same way as daily data.

## Suggested first run

To prefill the database for the tracked BIST list:

```bash
curl -X POST http://localhost:5000/api/admin/backfill \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Production notes

- For PostgreSQL later, move SQL access into a repository layer and swap out the SQLite connection factory.
- For heavier workloads, run the backfill as a scheduler or cron job.
- Standings are designed to read from the DB rather than from live yfinance calls.
