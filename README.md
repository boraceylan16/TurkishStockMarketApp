# 📈 Borsa Analiz — BIST Stock Analysis Dashboard

A polished, full-stack stock analysis application focused on Borsa Istanbul (BIST).  
Built with **Python/Flask** (backend) + **Vanilla JS + Chart.js** (frontend).

---

## Features

| Feature | Details |
|---|---|
| 🏦 Turkish Stock Data | 19 pre-loaded BIST stocks + any ticker via search |
| 📊 Financial Ratios | P/E, P/B, D/E, ROE, ROA, margins, EV/EBITDA, and more |
| 📈 Interactive Charts | 1D · 1W · 1M · 6M · 1Y · YTD · Total, normalized base-100 |
| 🔄 Benchmark Comparison | USD/TRY · Gold · Silver · BIST 100 (overlaid, toggleable) |
| 🎨 Modern Dark UI | Syne font, dark theme, smooth micro-interactions |

---

## Quick Start

### 1. Install Python dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Run the Flask backend

```bash
python app.py
# Runs on http://localhost:5000
```

### 3. Open the frontend

Open `frontend/index.html` in your browser.  
> **Tip:** Use a local server to avoid CORS issues with some browsers:
> ```bash
> cd frontend
> python -m http.server 8080
> # Then visit http://localhost:8080
> ```

---

## Project Structure

```
borsa/
├── backend/
│   ├── app.py              # Flask API (main entry point)
│   └── requirements.txt    # Python deps
└── frontend/
    └── index.html          # Complete single-file frontend
```

---

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/stocks/popular` | Returns list of popular BIST stocks |
| `GET /api/stock/<ticker>` | Full overview + ratios for a ticker |
| `GET /api/chart?ticker=&period=&comparisons=` | Normalized time-series data |
| `GET /api/ohlcv?ticker=&period=` | Raw OHLCV data |
| `GET /api/comparison-assets` | Available benchmark assets |

### Period values
`1D` `1W` `1M` `6M` `1Y` `YTD` `Total`

### Comparison values (comma-separated)
`USD/TRY` `Gold` `Silver` `BIST 100`

---

## Adding More Comparison Assets

In `backend/app.py`, edit the `COMPARISON_ASSETS` dict:

```python
COMPARISON_ASSETS = {
    "USD/TRY":  "USDTRY=X",
    "Gold":     "GC=F",
    "Silver":   "SI=F",
    "BIST 100": "XU100.IS",
    "EUR/TRY":  "EURTRY=X",  # <-- add new assets here
    "Oil":      "CL=F",
}
```

Then in `frontend/index.html`, add to `COMPARISON_ASSETS`:

```js
const COMPARISON_ASSETS = {
  ...
  "EUR/TRY": { ticker: "EURTRY=X", color: "#ef8c3d" },
  "Oil":     { ticker: "CL=F",     color: "#9b72ef" },
};
```

---

## Adding More BIST Stocks

In `backend/app.py`, add to `POPULAR_BIST_STOCKS`:

```python
{"ticker": "HALKB.IS", "name": "Halkbank", "sector": "Bankacılık"},
```

In `frontend/index.html`, add to `POPULAR_STOCKS`:

```js
{ ticker: "HALKB.IS", name: "Halkbank", sector: "Bankacılık" },
```

---

## Requirements

- Python 3.9+
- See `backend/requirements.txt`

```
flask>=3.0.0
flask-cors>=4.0.0
yfinance>=0.2.40
pandas>=2.0.0
numpy>=1.26.0
```

---

## Notes

- All stock data is fetched from **Yahoo Finance** via `yfinance`.  
- Turkish stocks use the `.IS` suffix (e.g. `THYAO.IS`).  
- Charts are normalized to **base = 100** so all assets can be compared on the same scale regardless of price.
- Missing data fields are handled gracefully and displayed as `—`.

---

## Deployment

For production, consider:
- Serving the backend with **Gunicorn** + **Nginx**
- Building the frontend with a bundler (Vite) or serving `index.html` statically
- Adding Redis caching for yfinance calls (rate limit friendly)
- Setting `API` constant in `index.html` to the deployed backend URL
