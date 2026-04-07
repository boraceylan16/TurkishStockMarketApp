"""
Borsa Istanbul Stock Analysis API
Backend: Flask + yfinance
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback
from deep_translator import GoogleTranslator

app = Flask(__name__)
CORS(app)

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

COMPARISON_ASSETS = {
"USD/TRY":     "USDTRY=X",
"Altın":       "GC=F",
"Gümüş":       "SI=F",
"Ham Petrol":  "CL=F",
"Doğal Gaz":   "NG=F",
"Bakır":       "HG=F",
"Platin":      "PL=F",
"Paladyum":    "PA=F",
"BIST 100":    "XU100.IS",
"Bitcoin":     "BTC-USD"
}


PERIOD_MAP = {
    "1D":    ("1d",  "5m"),
    "1W":    ("5d",  "30m"),
    "1M":    ("1mo", "1d"),
    "3M":    ("3mo", "1d"),
    "6M":    ("6mo", "1d"),
    "1Y":    ("1y",  "1d"),
    "YTD":   ("ytd", "1d"),
    "3Y":    ("3y",  "1d"),
    "5Y":    ("5y",  "1d"),
    "10Y":   ("10y", "1d"),
    "Total": ("max", "1d")
}

POPULAR_BIST_STOCKS = [
    {"ticker": "AEFES.IS", "name": "Anadolu Efes", "sector": "Tüketim"},
    {"ticker": "AGHOL.IS", "name": "Anadolu Grubu Holding", "sector": "Holding"},
    {"ticker": "AKBNK.IS", "name": "Akbank", "sector": "Bankacılık"},
    {"ticker": "AKSA.IS", "name": "Aksa Akrilik", "sector": "Kimya"},
    {"ticker": "AKSEN.IS", "name": "Aksa Enerji", "sector": "Enerji"},
    {"ticker": "ALARK.IS", "name": "Alarko Holding", "sector": "Holding"},
    {"ticker": "ARCLK.IS", "name": "Arçelik", "sector": "Tüketim"},
    {"ticker": "ASELS.IS", "name": "Aselsan", "sector": "Savunma"},
    {"ticker": "ASTOR.IS", "name": "Astor Enerji", "sector": "Enerji"},
    {"ticker": "BIMAS.IS", "name": "BİM", "sector": "Perakende"},
    {"ticker": "BRYAT.IS", "name": "Borusan Yatırım", "sector": "Holding"},
    {"ticker": "CCOLA.IS", "name": "Coca Cola İçecek", "sector": "Tüketim"},
    {"ticker": "CIMSA.IS", "name": "Çimsa", "sector": "Çimento"},
    {"ticker": "DOHOL.IS", "name": "Doğan Holding", "sector": "Holding"},
    {"ticker": "DOAS.IS", "name": "Doğuş Otomotiv", "sector": "Otomotiv"},
    {"ticker": "ECILC.IS", "name": "Eczacıbaşı İlaç", "sector": "Sağlık"},
    {"ticker": "EGEEN.IS", "name": "Ege Endüstri", "sector": "Otomotiv"},
    {"ticker": "EKGYO.IS", "name": "Emlak Konut GYO", "sector": "GYO"},
    {"ticker": "ENJSA.IS", "name": "Enerjisa Enerji", "sector": "Enerji"},
    {"ticker": "ENKAI.IS", "name": "Enka İnşaat", "sector": "İnşaat"},
    {"ticker": "EREGL.IS", "name": "Ereğli Demir Çelik", "sector": "Endüstri"},
    {"ticker": "FROTO.IS", "name": "Ford Otosan", "sector": "Otomotiv"},
    {"ticker": "GARAN.IS", "name": "Garanti BBVA", "sector": "Bankacılık"},
    {"ticker": "GESAN.IS", "name": "Girişim Elektrik", "sector": "Enerji"},
    {"ticker": "GLYHO.IS", "name": "Global Yatırım Holding", "sector": "Holding"},
    {"ticker": "GUBRF.IS", "name": "Gübre Fabrikaları", "sector": "Tarım"},
    {"ticker": "HALKB.IS", "name": "Halkbank", "sector": "Bankacılık"},
    {"ticker": "HEKTS.IS", "name": "Hektaş", "sector": "Tarım"},
    {"ticker": "ISCTR.IS", "name": "İş Bankası C", "sector": "Bankacılık"},
    {"ticker": "ISDMR.IS", "name": "İskenderun Demir Çelik", "sector": "Endüstri"},
    {"ticker": "KCHOL.IS", "name": "Koç Holding", "sector": "Holding"},
    {"ticker": "KONTR.IS", "name": "Kontrolmatik", "sector": "Teknoloji"},
    {"ticker": "KOZAA.IS", "name": "Koza Anadolu", "sector": "Madencilik"},
    {"ticker": "KOZAL.IS", "name": "Koza Altın", "sector": "Madencilik"},
    {"ticker": "KRDMD.IS", "name": "Kardemir D", "sector": "Endüstri"},
    {"ticker": "MGROS.IS", "name": "Migros", "sector": "Perakende"},
    {"ticker": "ODAS.IS", "name": "Odaş Elektrik", "sector": "Enerji"},
    {"ticker": "OTKAR.IS", "name": "Otokar", "sector": "Savunma"},
    {"ticker": "OYAKC.IS", "name": "Oyak Çimento", "sector": "Çimento"},
    {"ticker": "PGSUS.IS", "name": "Pegasus", "sector": "Ulaşım"},
    {"ticker": "PETKM.IS", "name": "Petkim", "sector": "Petrokimya"},
    {"ticker": "QUAGR.IS", "name": "Qua Granite", "sector": "İnşaat"},
    {"ticker": "SAHOL.IS", "name": "Sabancı Holding", "sector": "Holding"},
    {"ticker": "SASA.IS", "name": "Sasa Polyester", "sector": "Kimya"},
    {"ticker": "SISE.IS", "name": "Şişecam", "sector": "Endüstri"},
    {"ticker": "SMRTG.IS", "name": "Smart Güneş", "sector": "Enerji"},
    {"ticker": "SOKM.IS", "name": "Şok Marketler", "sector": "Perakende"},
    {"ticker": "TAVHL.IS", "name": "TAV Havalimanları", "sector": "Ulaşım"},
    {"ticker": "TCELL.IS", "name": "Turkcell", "sector": "Telekom"},
    {"ticker": "THYAO.IS", "name": "Türk Hava Yolları", "sector": "Ulaşım"},
    {"ticker": "TKFEN.IS", "name": "Tekfen Holding", "sector": "Holding"},
    {"ticker": "TOASO.IS", "name": "Tofaş", "sector": "Otomotiv"},
    {"ticker": "TSKB.IS", "name": "TSKB", "sector": "Bankacılık"},
    {"ticker": "TTRAK.IS", "name": "Türk Traktör", "sector": "Otomotiv"},
    {"ticker": "TUPRS.IS", "name": "Tüpraş", "sector": "Enerji"},
    {"ticker": "ULKER.IS", "name": "Ülker", "sector": "Gıda"},
    {"ticker": "VAKBN.IS", "name": "Vakıfbank", "sector": "Bankacılık"},
    {"ticker": "VESTL.IS", "name": "Vestel", "sector": "Teknoloji"},
    {"ticker": "YKBNK.IS", "name": "Yapı Kredi", "sector": "Bankacılık"},
]

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────


translator = GoogleTranslator(source='en', target='tr')

def translate_text(text):
    if not text:
        return text
    try:
        return translator.translate(text)
    except Exception:
        return text  # fallback if something breaks

def safe_val(val, decimals=2):
    """Return rounded float or None."""
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        return round(float(val), decimals)
    except Exception:
        return None


def fetch_info(ticker_symbol: str) -> dict:
    t = yf.Ticker(ticker_symbol)
    info = t.info or {}
    return info


def build_ratios(info: dict) -> dict:
    raw = {
        # Valuation
        "pe_ratio":            info.get("trailingPE"),
        "forward_pe":          info.get("forwardPE"),
        "pb_ratio":            info.get("priceToBook"),
        "ps_ratio":            info.get("priceToSalesTrailing12Months"),
        "ev_ebitda":           info.get("enterpriseToEbitda"),
        "ev_revenue":          info.get("enterpriseToRevenue"),
        "peg_ratio":           info.get("pegRatio"),
        # Profitability
        "gross_margin":        info.get("grossMargins"),
        "operating_margin":    info.get("operatingMargins"),
        "net_margin":          info.get("profitMargins"),
        "roe":                 info.get("returnOnEquity"),
        "roa":                 info.get("returnOnAssets"),
        # Financial Health
        "debt_to_equity":      info.get("debtToEquity"),
        "current_ratio":       info.get("currentRatio"),
        "quick_ratio":         info.get("quickRatio"),
        # Dividends
        "dividend_yield":      info.get("dividendYield"),
        "payout_ratio":        info.get("payoutRatio"),
        # Growth
        "earnings_growth":     info.get("earningsGrowth"),
        "revenue_growth":      info.get("revenueGrowth"),
        # Per-share
        "eps_trailing":        info.get("trailingEps"),
        "eps_forward":         info.get("forwardEps"),
        "book_value":          info.get("bookValue"),
        # Size
        "market_cap":          info.get("marketCap"),
        "enterprise_value":    info.get("enterpriseValue"),
        "shares_outstanding":  info.get("sharesOutstanding"),
        # 52-week
        "week52_high":         info.get("fiftyTwoWeekHigh"),
        "week52_low":          info.get("fiftyTwoWeekLow"),
        "beta":                info.get("beta"),
        "avg_volume":          info.get("averageVolume"),
    }

    # Percentages → multiply by 100
    pct_keys = ["gross_margin","operating_margin","net_margin","roe","roa",
                 "dividend_yield","payout_ratio","earnings_growth","revenue_growth"]
    result = {}
    for k, v in raw.items():
        sv = safe_val(v, 4)
        if sv is not None and k in pct_keys:
            sv = round(sv * 100, 2)
        result[k] = sv
    return result


def ohlcv_to_list(df: pd.DataFrame) -> list:
    """Convert OHLCV DataFrame to list of dicts."""
    if df is None or df.empty:
        return []
    df = df.reset_index()
    records = []
    for _, row in df.iterrows():
        ts = row.get("Datetime") if "Datetime" in row.index else row.get("Date")
        if ts is None or (not isinstance(ts, str) and pd.isna(ts)):
            continue
        records.append({
            "t": int(pd.Timestamp(ts).timestamp() * 1000),
            "o": safe_val(row.get("Open")),
            "h": safe_val(row.get("High")),
            "l": safe_val(row.get("Low")),
            "c": safe_val(row.get("Close")),
            "v": safe_val(row.get("Volume"), 0),
        })
    return records


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.route("/api/stocks/popular")
def popular_stocks():
    return jsonify(POPULAR_BIST_STOCKS)


@app.route("/api/comparison-assets")
def comparison_assets():
    return jsonify([{"label": k, "ticker": v} for k, v in COMPARISON_ASSETS.items()])


@app.route("/api/stock/<ticker>")
def stock_overview(ticker):
    try:
        info = fetch_info(ticker)
        if not info:
            return jsonify({"error": "Ticker not found"}), 404

        ratios = build_ratios(info)

        overview = {
            "ticker":       ticker,
            "name":         info.get("longName") or info.get("shortName") or ticker,
            "sector":       translate_text(info.get("sector")),
            "industry":     translate_text(info.get("industry")),
            "currency":     info.get("currency", "TRY"),
            "exchange":     info.get("exchange"),
            "website":      info.get("website"),
            "description":  translate_text(info.get("longBusinessSummary")),
            "current_price":safe_val(info.get("currentPrice") or info.get("regularMarketPrice")),
            "previous_close":safe_val(info.get("previousClose")),
            "open":         safe_val(info.get("open")),
            "day_high":     safe_val(info.get("dayHigh")),
            "day_low":      safe_val(info.get("dayLow")),
            "ratios":       ratios,
        }
    
        # Derived: day change
        if overview["current_price"] and overview["previous_close"]:
            chg = overview["current_price"] - overview["previous_close"]
            overview["day_change"]    = safe_val(chg)
            overview["day_change_pct"]= safe_val(chg / overview["previous_close"] * 100)

        return jsonify(overview)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/price")
def price_data():
    """
    Real price chart in TRY and/or USD.
    ?ticker=THYAO.IS&period=1M&currencies=TRY,USD

    Returns:
      {
        "TRY": [ {t, c}, ... ],
        "USD": [ {t, c}, ... ]   (only when requested — TRY price / USDTRY rate)
      }
    """
    ticker     = request.args.get("ticker", "")
    period     = request.args.get("period", "1M")
    currencies = request.args.get("currencies", "TRY")

    if not ticker:
        return jsonify({"error": "ticker required"}), 400

    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", "1d"))
    want_try = "TRY" in currencies
    want_usd = "USD" in currencies

    result = {}
    try:
        # Always fetch the stock
        df_stock = yf.download(ticker, period=yf_period, interval=yf_interval,
                               auto_adjust=True, progress=False)
        if isinstance(df_stock.columns, pd.MultiIndex):
            df_stock.columns = [col[0] for col in df_stock.columns]

        stock_pts = ohlcv_to_list(df_stock)

        if want_try:
            result["TRY"] = [{"t": p["t"], "c": p["c"]} for p in stock_pts]

        if want_usd:
            # Fetch USDTRY for the same period
            df_fx = yf.download("USDTRY=X", period=yf_period, interval=yf_interval,
                                auto_adjust=True, progress=False)
            if isinstance(df_fx.columns, pd.MultiIndex):
                df_fx.columns = [col[0] for col in df_fx.columns]

            fx_pts = ohlcv_to_list(df_fx)

            # Build a timestamp → USDTRY rate lookup
            # Use nearest available rate for each stock timestamp
            fx_map = {p["t"]: p["c"] for p in fx_pts if p["c"] is not None}
            fx_times = sorted(fx_map.keys())

            def nearest_rate(ts):
                if not fx_times:
                    return None
                # Binary search for closest timestamp
                lo, hi = 0, len(fx_times) - 1
                while lo < hi:
                    mid = (lo + hi) // 2
                    if fx_times[mid] < ts:
                        lo = mid + 1
                    else:
                        hi = mid
                # Check both neighbours
                idx = lo
                if idx > 0 and abs(fx_times[idx-1] - ts) < abs(fx_times[idx] - ts):
                    idx = idx - 1
                return fx_map[fx_times[idx]]

            usd_pts = []
            for p in stock_pts:
                if p["c"] is None:
                    usd_pts.append({"t": p["t"], "c": None})
                    continue
                rate = nearest_rate(p["t"])
                usd_pts.append({
                    "t": p["t"],
                    "c": round(p["c"] / rate, 4) if rate else None
                })
            result["USD"] = usd_pts

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    return jsonify(result)


@app.route("/api/chart")
def chart_data():
    """
    Normalized comparison chart (base=100), aligned to the main ticker timeline.
    ?ticker=THYAO.IS&period=1M&comparisons=USD/TRY,Altın&stocks=EREGL.IS,GARAN.IS

    Returns { label: [{t, c, n}, ...], ... }
    """
    ticker       = request.args.get("ticker", "")
    period       = request.args.get("period", "1M")
    compare      = request.args.get("comparisons", "")
    extra_stocks = request.args.get("stocks", "")

    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", "1d"))

    # Build fetch map: display-label -> yfinance symbol
    tickers_to_fetch = {ticker: ticker}

    if compare:
        for label in compare.split(","):
            label = label.strip()
            if label in COMPARISON_ASSETS:
                tickers_to_fetch[label] = COMPARISON_ASSETS[label]

    if extra_stocks:
        for sym in extra_stocks.split(","):
            sym = sym.strip()
            if sym and sym != ticker:
                tickers_to_fetch[sym] = sym

    def download_close_series(sym: str) -> pd.Series:
        df = yf.download(
            sym,
            period=yf_period,
            interval=yf_interval,
            auto_adjust=False,
            progress=False
        )

        if df is None or df.empty:
            return pd.Series(dtype="float64")

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        if "Close" not in df.columns:
            return pd.Series(dtype="float64")

        s = df["Close"].dropna().copy()
        s.index = pd.to_datetime(s.index)

        # Remove timezone if present, keep everything comparable
        try:
            if getattr(s.index, "tz", None) is not None:
                s.index = s.index.tz_convert(None)
        except Exception:
            pass

        s = s.sort_index()
        return s

    def series_to_points(series: pd.Series) -> list:
        out = []
        for ts, val in series.items():
            if pd.isna(val):
                continue
            out.append({
                "t": int(pd.Timestamp(ts).timestamp() * 1000),
                "c": safe_val(val),
            })
        return out

    # Tolerance for aligning nearby timestamps
    tolerance_map = {
        "5m": pd.Timedelta("10min"),
        "30m": pd.Timedelta("1h"),
        "1d": pd.Timedelta("3D"),
    }
    tolerance = tolerance_map.get(yf_interval, pd.Timedelta("3D"))

    result = {}

    try:
        # 1) Main ticker defines the master timeline
        main_series = download_close_series(ticker)
        if main_series.empty:
            return jsonify({"error": f"No data found for {ticker}"}), 404

        main_df = pd.DataFrame({
            "t": main_series.index,
            "c": main_series.values
        }).sort_values("t")

        main_base = float(main_df["c"].iloc[0])
        main_df["n"] = (main_df["c"] / main_base) * 100.0

        result[ticker] = [
            {
                "t": int(pd.Timestamp(row.t).timestamp() * 1000),
                "c": safe_val(row.c),
                "n": safe_val(row.n, 4),
            }
            for row in main_df.itertuples(index=False)
        ]

        master_timeline = main_df[["t"]].copy()

        # 2) Align every other series to the main ticker timestamps
        for label, sym in tickers_to_fetch.items():
            if label == ticker:
                continue

            s = download_close_series(sym)
            if s.empty:
                result[label] = []
                continue

            src = pd.DataFrame({
                "t": s.index,
                "c": s.values
            }).sort_values("t")

            # Align each comparison series to the main stock timeline
            aligned = pd.merge_asof(
                master_timeline,
                src,
                on="t",
                direction="backward",
                tolerance=tolerance
            )

            aligned = aligned.dropna(subset=["c"]).copy()

            if aligned.empty:
                result[label] = []
                continue

            base = float(aligned["c"].iloc[0])
            aligned["n"] = (aligned["c"] / base) * 100.0

            result[label] = [
                {
                    "t": int(pd.Timestamp(row.t).timestamp() * 1000),
                    "c": safe_val(row.c),
                    "n": safe_val(row.n, 4),
                }
                for row in aligned.itertuples(index=False)
            ]

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    return jsonify(result)


@app.route("/api/ohlcv")
def ohlcv():
    """Raw OHLCV for the main stock (candlestick chart)."""
    ticker   = request.args.get("ticker", "")
    period   = request.args.get("period", "1M")
    if not ticker:
        return jsonify({"error": "ticker required"}), 400

    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", "1d"))
    try:
        df = yf.download(ticker, period=yf_period, interval=yf_interval,
                         auto_adjust=False, progress=False)
        return jsonify(ohlcv_to_list(df))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)

