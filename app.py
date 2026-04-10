from __future__ import annotations

"""
Borsa Istanbul Stock Analysis API
Optimized version:
- Incremental daily history refresh instead of full max-history redownloads
- Short TTL in-memory cache for live quote/info and standings
- DB-only standings endpoint
- Batched background sync for daily prices
- Avoids double yfinance info fetch in /api/stock/<ticker>

API compatibility is preserved for existing endpoints.
"""

from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import os
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
import yfinance as yf
from deep_translator import GoogleTranslator


app = Flask(__name__)
CORS(app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("BORSA_DB_PATH", os.path.join(BASE_DIR, "borsa.sqlite3"))
BACKGROUND_SYNC_ENABLED = os.environ.get("BORSA_ENABLE_BACKGROUND_SYNC", "1") == "1"
BACKGROUND_SYNC_INTERVAL_SECONDS = int(os.environ.get("BORSA_BACKGROUND_SYNC_SECONDS", str(60 * 60 * 12)))

COMPARISON_ASSETS = {
    "USD/TRY": "USDTRY=X",
    "Altın": "GC=F",
    "Gümüş": "SI=F",
    "Ham Petrol": "CL=F",
    "Doğal Gaz": "NG=F",
    "Bakır": "HG=F",
    "Platin": "PL=F",
    "Paladyum": "PA=F",
    "BIST 100": "XU100.IS",
    "Bitcoin": "BTC-USD",
}

US_COMPARISON_ASSETS = {
    "S&P 500": "^GSPC",
    "Nasdaq 100": "^NDX",
    "Dow Jones": "^DJI",
    "AAPL": "AAPL",
    "MSFT": "MSFT",
    "NVDA": "NVDA",
    "TSLA": "TSLA",
}

PERIOD_MAP = {
    "1D": ("1d", "5m"),
    "1W": ("5d", "30m"),
    "1M": ("1mo", "1d"),
    "3M": ("3mo", "1d"),
    "6M": ("6mo", "1d"),
    "1Y": ("1y", "1d"),
    "YTD": ("ytd", "1d"),
    "3Y": ("3y", "1d"),
    "5Y": ("5y", "1d"),
    "10Y": ("10y", "1d"),
    "Total": ("max", "1d"),
}

POPULAR_BIST_STOCKS = [
  {"ticker":"AEFES.IS","name":"Anadolu Efes","sector":"Tüketim"},
  {"ticker":"AGHOL.IS","name":"Anadolu Grubu Holding","sector":"Holding"},
  {"ticker":"AHGAZ.IS","name":"Ahlatcı Doğalgaz","sector":"Enerji"},
  {"ticker":"AKBNK.IS","name":"Akbank","sector":"Bankacılık"},
  {"ticker":"AKCNS.IS","name":"Akçansa","sector":"Çimento"},
  {"ticker":"AKFGY.IS","name":"Akfen GYO","sector":"GYO"},
  {"ticker":"AKSA.IS","name":"Aksa Akrilik","sector":"Kimya"},
  {"ticker":"AKSEN.IS","name":"Aksa Enerji","sector":"Enerji"},
  {"ticker":"ALARK.IS","name":"Alarko Holding","sector":"Holding"},
  {"ticker":"ALBRK.IS","name":"Albaraka Türk","sector":"Bankacılık"},
  {"ticker":"ALFAS.IS","name":"Alfa Solar Enerji","sector":"Enerji"},
  {"ticker":"ARCLK.IS","name":"Arçelik","sector":"Tüketim"},
  {"ticker":"ASELS.IS","name":"Aselsan","sector":"Savunma"},
  {"ticker":"ASTOR.IS","name":"Astor Enerji","sector":"Enerji"},
  {"ticker":"BIMAS.IS","name":"BİM","sector":"Perakende"},
  {"ticker":"BOBET.IS","name":"Boğaziçi Beton","sector":"İnşaat"},
  {"ticker":"BRSAN.IS","name":"Borusan Mannesmann","sector":"Sanayi"},
  {"ticker":"BRYAT.IS","name":"Borusan Yatırım","sector":"Holding"},
  {"ticker":"CCOLA.IS","name":"Coca Cola İçecek","sector":"Tüketim"},
  {"ticker":"CIMSA.IS","name":"Çimsa","sector":"Çimento"},
  {"ticker":"DOAS.IS","name":"Doğuş Otomotiv","sector":"Otomotiv"},
  {"ticker":"DOHOL.IS","name":"Doğan Holding","sector":"Holding"},
  {"ticker":"ECILC.IS","name":"Eczacıbaşı İlaç","sector":"Sağlık"},
  {"ticker":"ECZYT.IS","name":"Eczacıbaşı Yatırım","sector":"Holding"},
  {"ticker":"EGEEN.IS","name":"Ege Endüstri","sector":"Otomotiv"},
  {"ticker":"EKGYO.IS","name":"Emlak Konut GYO","sector":"GYO"},
  {"ticker":"ENJSA.IS","name":"Enerjisa Enerji","sector":"Enerji"},
  {"ticker":"ENKAI.IS","name":"Enka İnşaat","sector":"İnşaat"},
  {"ticker":"EREGL.IS","name":"Ereğli Demir Çelik","sector":"Endüstri"},
  {"ticker":"EUPWR.IS","name":"Europower Enerji","sector":"Enerji"},
  {"ticker":"FROTO.IS","name":"Ford Otosan","sector":"Otomotiv"},
  {"ticker":"GARAN.IS","name":"Garanti BBVA","sector":"Bankacılık"},
  {"ticker":"GESAN.IS","name":"Girişim Elektrik","sector":"Enerji"},
  {"ticker":"GLYHO.IS","name":"Global Yatırım Holding","sector":"Holding"},
  {"ticker":"GSDHO.IS","name":"GSD Holding","sector":"Holding"},
  {"ticker":"GUBRF.IS","name":"Gübre Fabrikaları","sector":"Tarım"},
  {"ticker":"HALKB.IS","name":"Halkbank","sector":"Bankacılık"},
  {"ticker":"HEKTS.IS","name":"Hektaş","sector":"Tarım"},
  {"ticker":"TRENJ.IS","name":"TR Doğal Enerji","sector":"Enerji"},
  {"ticker":"ISCTR.IS","name":"İş Bankası C","sector":"Bankacılık"},
  {"ticker":"ISDMR.IS","name":"İskenderun Demir Çelik","sector":"Endüstri"},
  {"ticker":"ISGYO.IS","name":"İş GYO","sector":"GYO"},
  {"ticker":"ISMEN.IS","name":"İş Yatırım","sector":"Finans"},
  {"ticker":"KCAER.IS","name":"Kocaer Çelik","sector":"Sanayi"},
  {"ticker":"KCHOL.IS","name":"Koç Holding","sector":"Holding"},
  {"ticker":"KLSER.IS","name":"Kaleseramik","sector":"Sanayi"},
  {"ticker":"KONTR.IS","name":"Kontrolmatik","sector":"Teknoloji"},
  {"ticker":"KORDS.IS","name":"Kordsa","sector":"Sanayi"},
  {"ticker":"KOZAA.IS","name":"Koza Anadolu","sector":"Madencilik"},
  {"ticker":"KOZAL.IS","name":"Koza Altın","sector":"Madencilik"},
  {"ticker":"KRDMD.IS","name":"Kardemir D","sector":"Endüstri"},
  {"ticker":"MAVI.IS","name":"Mavi Giyim","sector":"Perakende"},
  {"ticker":"MGROS.IS","name":"Migros","sector":"Perakende"},
  {"ticker":"MIATK.IS","name":"Mia Teknoloji","sector":"Teknoloji"},
  {"ticker":"ODAS.IS","name":"Odaş Elektrik","sector":"Enerji"},
  {"ticker":"OTKAR.IS","name":"Otokar","sector":"Savunma"},
  {"ticker":"OYAKC.IS","name":"Oyak Çimento","sector":"Çimento"},
  {"ticker":"PETKM.IS","name":"Petkim","sector":"Petrokimya"},
  {"ticker":"PGSUS.IS","name":"Pegasus","sector":"Ulaşım"},
  {"ticker":"QUAGR.IS","name":"Qua Granite","sector":"İnşaat"},
  {"ticker":"SAHOL.IS","name":"Sabancı Holding","sector":"Holding"},
  {"ticker":"SASA.IS","name":"Sasa Polyester","sector":"Kimya"},
  {"ticker":"SELEC.IS","name":"Selçuk Ecza Deposu","sector":"Sağlık"},
  {"ticker":"SISE.IS","name":"Şişecam","sector":"Endüstri"},
  {"ticker":"SKBNK.IS","name":"Şekerbank","sector":"Bankacılık"},
  {"ticker":"SMRTG.IS","name":"Smart Güneş","sector":"Enerji"},
  {"ticker":"SOKM.IS","name":"Şok Marketler","sector":"Perakende"},
  {"ticker":"TAVHL.IS","name":"TAV Havalimanları","sector":"Ulaşım"},
  {"ticker":"TCELL.IS","name":"Turkcell","sector":"Telekom"},
  {"ticker":"THYAO.IS","name":"Türk Hava Yolları","sector":"Ulaşım"},
  {"ticker":"TKFEN.IS","name":"Tekfen Holding","sector":"Holding"},
  {"ticker":"TOASO.IS","name":"Tofaş","sector":"Otomotiv"},
  {"ticker":"TSKB.IS","name":"TSKB","sector":"Bankacılık"},
  {"ticker":"TTKOM.IS","name":"Türk Telekom","sector":"Telekom"},
  {"ticker":"TTRAK.IS","name":"Türk Traktör","sector":"Otomotiv"},
  {"ticker":"TUPRS.IS","name":"Tüpraş","sector":"Enerji"},
  {"ticker":"ULKER.IS","name":"Ülker","sector":"Gıda"},
  {"ticker":"VAKBN.IS","name":"Vakıfbank","sector":"Bankacılık"},
  {"ticker":"VESBE.IS","name":"Vestel Beyaz Eşya","sector":"Sanayi"},
  {"ticker":"VESTL.IS","name":"Vestel","sector":"Teknoloji"},
  {"ticker":"YKBNK.IS","name":"Yapı Kredi","sector":"Bankacılık"},
  {"ticker":"YYLGD.IS","name":"Yayla Agro","sector":"Gıda"},
  {"ticker":"ZOREN.IS","name":"Zorlu Enerji","sector":"Enerji"}
]

translator = GoogleTranslator(source="en", target="tr")

RATIO_FIELDS = [
    "pe_ratio", "forward_pe", "pb_ratio", "ps_ratio", "ev_ebitda", "ev_revenue", "peg_ratio",
    "gross_margin", "operating_margin", "net_margin", "roe", "roa", "debt_to_equity",
    "current_ratio", "quick_ratio", "dividend_yield", "payout_ratio", "earnings_growth",
    "revenue_growth", "eps_trailing", "eps_forward", "book_value", "market_cap",
    "enterprise_value", "shares_outstanding", "week52_high", "week52_low", "beta", "avg_volume",
]

# Short TTL caches
QUOTE_CACHE_TTL_SECONDS = int(os.environ.get("BORSA_QUOTE_CACHE_TTL_SECONDS", "45"))
STANDINGS_CACHE_TTL_SECONDS = int(os.environ.get("BORSA_STANDINGS_CACHE_TTL_SECONDS", "180"))

_quote_cache: Dict[str, Tuple[float, dict]] = {}
_standings_cache: Dict[str, Tuple[float, dict]] = {}
_cache_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(
        DB_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            ticker TEXT PRIMARY KEY,
            company_name TEXT,
            sector TEXT,
            industry TEXT,
            currency TEXT,
            exchange TEXT,
            website TEXT,
            description TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS price_history (
            ticker TEXT NOT NULL,
            datetime TEXT NOT NULL,
            interval TEXT NOT NULL DEFAULT '1d',
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            currency TEXT,
            source TEXT DEFAULT 'yfinance',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, datetime, interval)
        );

        CREATE INDEX IF NOT EXISTS idx_price_ticker_datetime ON price_history (ticker, datetime);
        CREATE INDEX IF NOT EXISTS idx_price_interval ON price_history (interval, ticker, datetime);

        CREATE TABLE IF NOT EXISTS financial_ratios (
            ticker TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            pe_ratio REAL,
            forward_pe REAL,
            pb_ratio REAL,
            ps_ratio REAL,
            ev_ebitda REAL,
            ev_revenue REAL,
            peg_ratio REAL,
            gross_margin REAL,
            operating_margin REAL,
            net_margin REAL,
            roe REAL,
            roa REAL,
            debt_to_equity REAL,
            current_ratio REAL,
            quick_ratio REAL,
            dividend_yield REAL,
            payout_ratio REAL,
            earnings_growth REAL,
            revenue_growth REAL,
            eps_trailing REAL,
            eps_forward REAL,
            book_value REAL,
            market_cap REAL,
            enterprise_value REAL,
            shares_outstanding REAL,
            week52_high REAL,
            week52_low REAL,
            beta REAL,
            avg_volume REAL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, snapshot_date)
        );

        CREATE INDEX IF NOT EXISTS idx_ratios_ticker_snapshot ON financial_ratios (ticker, snapshot_date DESC);
        """
    )
    conn.commit()
    conn.close()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def is_suspicious(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return True
    if "Close" not in df.columns:
        return True
    if len(df) < 2:
        return True
    if df["Close"].dropna().nunique() <= 1:
        return True
    return False


def iso_dt(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_ts_index(idx):
    idx = pd.to_datetime(idx)
    try:
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_convert(None)
    except Exception:
        try:
            idx = idx.tz_localize(None)
        except Exception:
            pass
    return idx


def safe_val(val, decimals: int = 2):
    try:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        return round(float(val), decimals)
    except Exception:
        return None


def translate_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    try:
        return translator.translate(text)
    except Exception:
        return text


def cache_get(cache: Dict[str, Tuple[float, dict]], key: str, ttl_seconds: int) -> Optional[dict]:
    now_ts = time.time()
    with _cache_lock:
        item = cache.get(key)
        if not item:
            return None
        ts, value = item
        if now_ts - ts > ttl_seconds:
            cache.pop(key, None)
            return None
        return value


def cache_set(cache: Dict[str, Tuple[float, dict]], key: str, value: dict) -> None:
    with _cache_lock:
        cache[key] = (time.time(), value)


def fetch_info(ticker_symbol: str) -> dict:
    t = yf.Ticker(ticker_symbol)
    return t.info or {}


def fetch_info_cached(ticker_symbol: str) -> dict:
    cached = cache_get(_quote_cache, f"info:{ticker_symbol}", QUOTE_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    info = fetch_info(ticker_symbol)
    cache_set(_quote_cache, f"info:{ticker_symbol}", info)
    return info


def build_ratios(info: dict) -> dict:
    raw = {
        "pe_ratio": info.get("trailingPE"),
        "forward_pe": info.get("forwardPE"),
        "pb_ratio": info.get("priceToBook"),
        "ps_ratio": info.get("priceToSalesTrailing12Months"),
        "ev_ebitda": info.get("enterpriseToEbitda"),
        "ev_revenue": info.get("enterpriseToRevenue"),
        "peg_ratio": info.get("pegRatio"),
        "gross_margin": info.get("grossMargins"),
        "operating_margin": info.get("operatingMargins"),
        "net_margin": info.get("profitMargins"),
        "roe": info.get("returnOnEquity"),
        "roa": info.get("returnOnAssets"),
        "debt_to_equity": info.get("debtToEquity"),
        "current_ratio": info.get("currentRatio"),
        "quick_ratio": info.get("quickRatio"),
        "dividend_yield": info.get("dividendYield"),
        "payout_ratio": info.get("payoutRatio"),
        "earnings_growth": info.get("earningsGrowth"),
        "revenue_growth": info.get("revenueGrowth"),
        "eps_trailing": info.get("trailingEps"),
        "eps_forward": info.get("forwardEps"),
        "book_value": info.get("bookValue"),
        "market_cap": info.get("marketCap"),
        "enterprise_value": info.get("enterpriseValue"),
        "shares_outstanding": info.get("sharesOutstanding"),
        "week52_high": info.get("fiftyTwoWeekHigh"),
        "week52_low": info.get("fiftyTwoWeekLow"),
        "beta": info.get("beta"),
        "avg_volume": info.get("averageVolume"),
    }
    pct_keys = {
        "gross_margin", "operating_margin", "net_margin", "roe", "roa",
        "dividend_yield", "payout_ratio", "earnings_growth", "revenue_growth",
    }
    result = {}
    for key, value in raw.items():
        sv = safe_val(value, 4)
        if sv is not None and key in pct_keys:
            sv = round(sv * 100, 2)
        result[key] = sv
    return result


def normalize_yf_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [col[0] for col in out.columns]
    out.index = pd.to_datetime(out.index)
    try:
        if getattr(out.index, "tz", None) is not None:
            out.index = out.index.tz_convert(None)
    except Exception:
        pass
    out = out.sort_index()
    return out


def ohlcv_to_list(df: pd.DataFrame) -> list:
    if df is None or df.empty:
        return []

    df = df.reset_index()
    records = []

    possible_ts_cols = ["Datetime", "Date", "datetime", "date", "index"]
    ts_col = next((col for col in possible_ts_cols if col in df.columns), None)
    if ts_col is None and len(df.columns) > 0:
        ts_col = df.columns[0]

    for _, row in df.iterrows():
        ts = row.get(ts_col)
        if ts is None or pd.isna(ts):
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


def persist_metadata(ticker: str, info: dict) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO metadata (
            ticker, company_name, sector, industry, currency, exchange, website, description, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            company_name=excluded.company_name,
            sector=excluded.sector,
            industry=excluded.industry,
            currency=excluded.currency,
            exchange=excluded.exchange,
            website=excluded.website,
            description=excluded.description,
            updated_at=excluded.updated_at
        """,
        (
            ticker,
            info.get("longName") or info.get("shortName") or ticker,
            translate_text(info.get("sector")),
            translate_text(info.get("industry")),
            info.get("currency", "TRY"),
            info.get("exchange"),
            info.get("website"),
            translate_text(info.get("longBusinessSummary")),
            iso_dt(utcnow()),
        ),
    )
    conn.commit()
    conn.close()


def persist_ratios_snapshot(ticker: str, ratios: dict, snapshot_date: Optional[str] = None) -> None:
    snapshot_date = snapshot_date or utcnow().date().isoformat()
    columns = ["ticker", "snapshot_date"] + RATIO_FIELDS
    placeholders = ",".join(["?"] * len(columns))
    values = [ticker, snapshot_date] + [ratios.get(col) for col in RATIO_FIELDS]
    conn = get_conn()
    conn.execute(
        f"INSERT OR REPLACE INTO financial_ratios ({','.join(columns)}) VALUES ({placeholders})",
        values,
    )
    conn.commit()
    conn.close()


def persist_price_history(ticker: str, df: pd.DataFrame, interval: str = "1d", currency: str = "TRY") -> None:
    if df is None or df.empty:
        return
    df = normalize_yf_df(df)
    records = []
    for ts, row in df.iterrows():
        records.append(
            (
                ticker,
                iso_dt(pd.Timestamp(ts).to_pydatetime().replace(tzinfo=timezone.utc)),
                interval,
                safe_val(row.get("Open"), 4),
                safe_val(row.get("High"), 4),
                safe_val(row.get("Low"), 4),
                safe_val(row.get("Close"), 4),
                safe_val(row.get("Volume"), 0),
                currency,
            )
        )
    conn = get_conn()
    conn.executemany(
        """
        INSERT OR REPLACE INTO price_history (
            ticker, datetime, interval, open, high, low, close, volume, currency
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        records,
    )
    conn.commit()
    conn.close()


def get_latest_price_timestamp(ticker: str, interval: str = "1d") -> Optional[datetime]:
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(datetime) AS dt FROM price_history WHERE ticker = ? AND interval = ?",
        (ticker, interval),
    ).fetchone()
    conn.close()
    if not row or not row["dt"]:
        return None
    return datetime.strptime(row["dt"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def get_latest_ratio_date(ticker: str) -> Optional[datetime]:
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(snapshot_date) AS d FROM financial_ratios WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    conn.close()
    if not row or not row["d"]:
        return None
    return datetime.strptime(row["d"], "%Y-%m-%d")


def should_refresh_daily_prices(ticker: str) -> bool:
    latest = get_latest_price_timestamp(ticker, "1d")
    if latest is None:
        return True
    now = utcnow()
    return (now - latest) > timedelta(hours=18)


def should_refresh_ratios(ticker: str) -> bool:
    latest = get_latest_ratio_date(ticker)
    if latest is None:
        return True
    return datetime.utcnow() - latest > timedelta(days=85)


def download_history(symbol: str, period: str, interval: str, auto_adjust: bool = False) -> pd.DataFrame:
    df = yf.download(symbol, period=period, interval=interval, auto_adjust=auto_adjust, progress=False, threads=True)
    return normalize_yf_df(df)


def download_history_batch(symbols: List[str], period: str, interval: str, auto_adjust: bool = False) -> Dict[str, pd.DataFrame]:
    symbols = [s for s in symbols if s]
    if not symbols:
        return {}

    raw = yf.download(
        tickers=" ".join(symbols),
        period=period,
        interval=interval,
        auto_adjust=auto_adjust,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    out: Dict[str, pd.DataFrame] = {}

    if raw is None or raw.empty:
        return out

    if isinstance(raw.columns, pd.MultiIndex):
        lvl0 = set(raw.columns.get_level_values(0))
        for sym in symbols:
            if sym in lvl0:
                sub = raw[sym].copy()
                sub = normalize_yf_df(sub)
                if not sub.empty:
                    out[sym] = sub
    else:
        # Single symbol case
        if len(symbols) == 1:
            out[symbols[0]] = normalize_yf_df(raw)

    return out


def ensure_daily_price_history(ticker: str) -> None:
    latest = get_latest_price_timestamp(ticker, "1d")

    if latest is not None and not should_refresh_daily_prices(ticker):
        return

    if latest is None:
        df = download_history(ticker, period="max", interval="1d", auto_adjust=False)
    else:
        age = utcnow() - latest
        if age <= timedelta(hours=18):
            return

        # Fetch only a recent buffer window, then upsert
        # This avoids re-downloading full history.
        if age <= timedelta(days=7):
            fetch_period = "1mo"
        elif age <= timedelta(days=31):
            fetch_period = "3mo"
        elif age <= timedelta(days=93):
            fetch_period = "6mo"
        else:
            fetch_period = "1y"

        df = download_history(ticker, period=fetch_period, interval="1d", auto_adjust=False)

    if is_suspicious(df):
        print(f"⚠️ Skipping bad data for {ticker}")
        return

    persist_price_history(ticker, df, interval="1d", currency="TRY")


def ensure_fx_history() -> None:
    latest = get_latest_price_timestamp("USDTRY=X", "1d")
    if latest is not None and not should_refresh_daily_prices("USDTRY=X"):
        return

    if latest is None:
        df = download_history("USDTRY=X", period="max", interval="1d", auto_adjust=False)
    else:
        age = utcnow() - latest
        if age <= timedelta(hours=18):
            return
        fetch_period = "3mo" if age <= timedelta(days=31) else "1y"
        df = download_history("USDTRY=X", period=fetch_period, interval="1d", auto_adjust=False)

    if not df.empty:
        persist_price_history("USDTRY=X", df, interval="1d", currency="TRY")


def batch_refresh_daily_price_history(tickers: List[str]) -> None:
    stale = [t for t in tickers if should_refresh_daily_prices(t)]
    if not stale:
        return

    # Split into new/no-data vs existing cache
    no_data = []
    incremental = []
    for ticker in stale:
        latest = get_latest_price_timestamp(ticker, "1d")
        if latest is None:
            no_data.append(ticker)
        else:
            incremental.append(ticker)

    # Full history only for first-time load
    if no_data:
        batch = download_history_batch(no_data, period="max", interval="1d", auto_adjust=False)
        for ticker, df in batch.items():
            if is_suspicious(df):
                print(f"⚠️ Skipping bad data for {ticker}")
                continue
            persist_price_history(ticker, df, interval="1d", currency="TRY")

    # Incremental recent-window refresh for existing symbols
    if incremental:
        batch = download_history_batch(incremental, period="6mo", interval="1d", auto_adjust=False)
        for ticker, df in batch.items():
            if is_suspicious(df):
                print(f"⚠️ Skipping bad data for {ticker}")
                continue
            persist_price_history(ticker, df, interval="1d", currency="TRY")


def ensure_stock_info(ticker: str) -> dict:
    need_ratios = should_refresh_ratios(ticker)

    conn = get_conn()
    meta = conn.execute("SELECT * FROM metadata WHERE ticker = ?", (ticker,)).fetchone()
    conn.close()

    if meta is not None and not need_ratios:
        info = dict(meta)
        latest_ratios = get_latest_ratios_from_db(ticker)
        if latest_ratios:
            info["ratios"] = latest_ratios
        info["_live_info"] = None
        return info

    live_info = fetch_info_cached(ticker)
    if not live_info:
        raise ValueError("Ticker not found")

    persist_metadata(ticker, live_info)
    ratios = build_ratios(live_info)
    persist_ratios_snapshot(ticker, ratios)

    merged = dict(meta) if meta else {}
    merged.update({
        "company_name": live_info.get("longName") or live_info.get("shortName") or ticker,
        "sector": translate_text(live_info.get("sector")),
        "industry": translate_text(live_info.get("industry")),
        "currency": live_info.get("currency", "TRY"),
        "exchange": live_info.get("exchange"),
        "website": live_info.get("website"),
        "description": translate_text(live_info.get("longBusinessSummary")),
        "ratios": ratios,
        "_live_info": live_info,
    })
    return merged


def get_latest_ratios_from_db(ticker: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM financial_ratios WHERE ticker = ? ORDER BY snapshot_date DESC LIMIT 1",
        (ticker,),
    ).fetchone()
    conn.close()
    if row is None:
        return None
    data = dict(row)
    return {k: data.get(k) for k in RATIO_FIELDS}


def get_db_series(ticker: str, start_dt: Optional[datetime] = None) -> pd.Series:
    conn = get_conn()
    if start_dt is not None:
        rows = conn.execute(
            "SELECT datetime, close FROM price_history WHERE ticker = ? AND interval = '1d' AND datetime >= ? ORDER BY datetime ASC",
            (ticker, iso_dt(start_dt)),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT datetime, close FROM price_history WHERE ticker = ? AND interval = '1d' ORDER BY datetime ASC",
            (ticker,),
        ).fetchall()
    conn.close()

    if not rows:
        return pd.Series(dtype="float64")

    idx = normalize_ts_index([r["datetime"] for r in rows])
    vals = [r["close"] for r in rows]
    return pd.Series(vals, index=idx, dtype="float64").sort_index()


def get_db_ohlcv(ticker: str, start_dt: Optional[datetime] = None) -> pd.DataFrame:
    conn = get_conn()
    if start_dt is not None:
        rows = conn.execute(
            """
            SELECT datetime, open, high, low, close, volume
            FROM price_history
            WHERE ticker = ? AND interval = '1d' AND datetime >= ?
            ORDER BY datetime ASC
            """,
            (ticker, iso_dt(start_dt)),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT datetime, open, high, low, close, volume
            FROM price_history
            WHERE ticker = ? AND interval = '1d'
            ORDER BY datetime ASC
            """,
            (ticker,),
        ).fetchall()
    conn.close()

    if not rows:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    df = pd.DataFrame([dict(r) for r in rows])
    df["datetime"] = normalize_ts_index(df["datetime"])
    df = df.set_index("datetime")
    return df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    }).sort_index()


def period_start(period: str) -> Optional[datetime]:
    now = datetime.utcnow()
    if period == "1M":
        return now - timedelta(days=31)
    if period == "3M":
        return now - timedelta(days=92)
    if period == "6M":
        return now - timedelta(days=183)
    if period == "1Y":
        return now - timedelta(days=366)
    if period == "YTD":
        return datetime(now.year, 1, 1)
    if period == "3Y":
        return now - timedelta(days=365 * 3 + 2)
    if period == "5Y":
        return now - timedelta(days=365 * 5 + 2)
    if period == "10Y":
        return now - timedelta(days=365 * 10 + 3)
    if period == "1W":
        return now - timedelta(days=8)
    if period == "Total":
        return None
    return now - timedelta(days=31)


def get_cached_or_live_series(symbol: str, period: str, interval: str, auto_adjust: bool = False) -> pd.Series:
    if interval == "1d":
        if symbol == "USDTRY=X":
            ensure_fx_history()
        else:
            ensure_daily_price_history(symbol)
        start = period_start(period)
        s = get_db_series(symbol, start)
        if not s.empty:
            return s

    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", interval))
    df = download_history(symbol, yf_period, yf_interval, auto_adjust=auto_adjust)
    if df.empty or "Close" not in df.columns:
        return pd.Series(dtype="float64")
    return df["Close"].dropna()


def get_cached_or_live_ohlcv(ticker: str, period: str) -> pd.DataFrame:
    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", "1d"))
    if yf_interval == "1d":
        ensure_daily_price_history(ticker)
        df = get_db_ohlcv(ticker, period_start(period))
        if not df.empty:
            return df
    return download_history(ticker, period=yf_period, interval=yf_interval, auto_adjust=False)


def series_to_points(series: pd.Series, normalized: bool = False) -> list:
    if series.empty:
        return []
    series = series.dropna()
    if series.empty:
        return []
    base = float(series.iloc[0])
    out = []
    for ts, val in series.items():
        item = {"t": int(pd.Timestamp(ts).timestamp() * 1000), "c": safe_val(val, 4)}
        if normalized and base:
            item["n"] = safe_val((float(val) / base) * 100.0, 4)
        out.append(item)
    return out


def nearest_rate_lookup(fx_series: pd.Series, ts: pd.Timestamp) -> Optional[float]:
    if fx_series.empty:
        return None

    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    else:
        try:
            ts = ts.tz_localize(None)
        except Exception:
            pass

    fx_index = normalize_ts_index(fx_series.index)
    fx_series = pd.Series(fx_series.values, index=fx_index).sort_index()

    idx = fx_series.index.get_indexer([ts], method="nearest")
    if len(idx) == 0 or idx[0] == -1:
        return None
    return float(fx_series.iloc[idx[0]])


def get_usd_series_for_stock(ticker: str, period: str) -> pd.Series:
    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", "1d"))
    stock_series = get_cached_or_live_series(ticker, period, yf_interval, auto_adjust=True)
    if stock_series.empty:
        return pd.Series(dtype="float64")

    if yf_interval == "1d":
        ensure_fx_history()
        fx_series = get_db_series("USDTRY=X", period_start(period))
        if fx_series.empty:
            fx_series = get_cached_or_live_series("USDTRY=X", period, "1d", auto_adjust=True)
    else:
        fx_df = download_history("USDTRY=X", yf_period, yf_interval, auto_adjust=True)
        fx_series = fx_df["Close"].dropna() if not fx_df.empty else pd.Series(dtype="float64")

    if fx_series.empty:
        return pd.Series(dtype="float64")

    aligned_values = []
    aligned_index = []
    for ts, val in stock_series.dropna().items():
        rate = nearest_rate_lookup(fx_series, pd.Timestamp(ts))
        if rate:
            aligned_values.append(float(val) / rate)
            aligned_index.append(pd.Timestamp(ts))
    if not aligned_values:
        return pd.Series(dtype="float64")
    return pd.Series(aligned_values, index=pd.to_datetime(aligned_index), dtype="float64").sort_index()


def align_to_master(master: pd.DatetimeIndex, series: pd.Series, tolerance: pd.Timedelta) -> pd.Series:
    if series.empty:
        return pd.Series(dtype="float64")

    master = normalize_ts_index(master)
    src_index = normalize_ts_index(series.index)

    src = pd.DataFrame({
        "t": src_index,
        "c": series.values
    }).sort_values("t")

    mdf = pd.DataFrame({
        "t": master
    }).sort_values("t")

    aligned = pd.merge_asof(
        mdf,
        src,
        on="t",
        direction="backward",
        tolerance=tolerance
    )

    aligned = aligned.dropna(subset=["c"])
    if aligned.empty:
        return pd.Series(dtype="float64")

    return pd.Series(
        aligned["c"].values,
        index=pd.to_datetime(aligned["t"]),
        dtype="float64"
    ).sort_index()


def compute_standings_from_db(period: str, view: str) -> dict:
    if period == "1D":
        start = datetime.utcnow() - timedelta(days=2)
    elif period == "1W":
        start = datetime.utcnow() - timedelta(days=8)
    else:
        start = period_start(period)

    rows = []
    for stock in POPULAR_BIST_STOCKS:
        ticker = stock["ticker"]
        s = get_db_series(ticker, start)
        s = s.dropna()
        if len(s) < 2:
            continue

        start_price = float(s.iloc[0])
        end_price = float(s.iloc[-1])
        if start_price == 0:
            continue

        change_pct = ((end_price - start_price) / start_price) * 100.0
        rows.append({
            "ticker": ticker,
            "name": stock["name"],
            "sector": stock["sector"],
            "start_price": safe_val(start_price, 4),
            "end_price": safe_val(end_price, 4),
            "change_pct": safe_val(change_pct, 4),
        })

    rows = sorted(rows, key=lambda x: x["change_pct"], reverse=True)
    gainers = rows[:10]
    losers = list(reversed(rows[-10:]))

    selected = rows
    if view == "gainers":
        selected = gainers
    elif view == "losers":
        selected = losers

    return {
        "period": period,
        "view": view,
        "updated_at": iso_dt(utcnow()),
        "count": len(rows),
        "rows": selected,
        "top_gainers": gainers,
        "top_losers": losers,
    }


def get_standings_cached(period: str, view: str) -> dict:
    key = f"{period}:{view}"
    cached = cache_get(_standings_cache, key, STANDINGS_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached

    data = compute_standings_from_db(period, view)
    cache_set(_standings_cache, key, data)
    return data


def invalidate_standings_cache() -> None:
    with _cache_lock:
        _standings_cache.clear()


def start_background_sync() -> None:
    if not BACKGROUND_SYNC_ENABLED:
        return

    def worker() -> None:
        while True:
            try:
                tickers = [stock["ticker"] for stock in POPULAR_BIST_STOCKS]
                batch_refresh_daily_price_history(tickers)
                ensure_fx_history()
                invalidate_standings_cache()
            except Exception:
                traceback.print_exc()
            time.sleep(BACKGROUND_SYNC_INTERVAL_SECONDS)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/stocks/popular")
def popular_stocks():
    return jsonify(POPULAR_BIST_STOCKS)


@app.route("/api/comparison-assets")
def comparison_assets():
    return jsonify({
        "local": [{"label": k, "ticker": v} for k, v in COMPARISON_ASSETS.items()],
        "us": [{"label": k, "ticker": v} for k, v in US_COMPARISON_ASSETS.items()],
    })


@app.route("/api/stock/<ticker>")
def stock_overview(ticker):
    try:
        ensure_daily_price_history(ticker)

        info = ensure_stock_info(ticker)
        ratios = info.get("ratios") or get_latest_ratios_from_db(ticker) or {}

        # Avoid second uncached info fetch. Use live info if ensure_stock_info fetched it,
        # otherwise short-TTL cached info fetch.
        raw_info = info.get("_live_info") or fetch_info_cached(ticker)

        current_price = safe_val(raw_info.get("currentPrice") or raw_info.get("regularMarketPrice"))
        previous_close = safe_val(raw_info.get("previousClose"))
        open_price = safe_val(raw_info.get("open"))
        day_high = safe_val(raw_info.get("dayHigh"))
        day_low = safe_val(raw_info.get("dayLow"))

        if current_price is None:
            series = get_db_series(ticker)
            if not series.empty:
                current_price = safe_val(series.iloc[-1])
                if len(series) > 1:
                    previous_close = safe_val(series.iloc[-2])

        overview = {
            "ticker": ticker,
            "name": raw_info.get("longName") or raw_info.get("shortName") or info.get("company_name") or ticker,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "currency": raw_info.get("currency", info.get("currency", "TRY")),
            "exchange": raw_info.get("exchange") or info.get("exchange"),
            "website": raw_info.get("website") or info.get("website"),
            "description": info.get("description"),
            "current_price": current_price,
            "previous_close": previous_close,
            "open": open_price,
            "day_high": day_high,
            "day_low": day_low,
            "ratios": ratios,
        }
        if overview["current_price"] is not None and overview["previous_close"]:
            chg = overview["current_price"] - overview["previous_close"]
            overview["day_change"] = safe_val(chg)
            overview["day_change_pct"] = safe_val(chg / overview["previous_close"] * 100)
        return jsonify(overview)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/reset-bad-data", methods=["POST"])
def reset_bad_data():
    bad = ["ODAS.IS", "HALKB.IS", "TSKB.IS"]

    conn = get_conn()
    for t in bad:
        conn.execute("DELETE FROM price_history WHERE ticker = ?", (t,))
    conn.commit()
    conn.close()

    invalidate_standings_cache()
    return jsonify({"status": "cleaned", "tickers": bad})


@app.route("/api/price")
def price_data():
    ticker = request.args.get("ticker", "")
    period = request.args.get("period", "1M")
    currencies = request.args.get("currencies", "TRY")
    if not ticker:
        return jsonify({"error": "ticker required"}), 400

    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", "1d"))
    want_try = "TRY" in currencies
    want_usd = "USD" in currencies
    result = {}

    try:
        if yf_interval == "1d":
            ensure_daily_price_history(ticker)
            stock_df = get_db_ohlcv(ticker, period_start(period))
            if stock_df.empty:
                stock_df = download_history(ticker, yf_period, yf_interval, auto_adjust=True)
        else:
            stock_df = download_history(ticker, yf_period, yf_interval, auto_adjust=True)

        if stock_df.empty:
            return jsonify({"TRY": [], "USD": []})

        if want_try:
            try_series = stock_df["Close"].dropna()
            result["TRY"] = [
                {"t": int(pd.Timestamp(ts).timestamp() * 1000), "c": safe_val(val, 4)}
                for ts, val in try_series.items()
            ]

        if want_usd:
            if yf_interval == "1d":
                ensure_fx_history()
                fx_series = get_db_series("USDTRY=X", period_start(period))
            else:
                fx_df = download_history("USDTRY=X", yf_period, yf_interval, auto_adjust=True)
                fx_series = fx_df["Close"].dropna() if not fx_df.empty else pd.Series(dtype="float64")

            usd_points = []
            if "Close" in stock_df.columns:
                for ts, val in stock_df["Close"].dropna().items():
                    rate = nearest_rate_lookup(fx_series, pd.Timestamp(ts))
                    usd_points.append({
                        "t": int(pd.Timestamp(ts).timestamp() * 1000),
                        "c": round(float(val) / rate, 4) if (rate and val is not None) else None
                    })
            result["USD"] = usd_points

        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/usd-chart")
def usd_chart_data():
    ticker = request.args.get("ticker", "")
    period = request.args.get("period", "1M")
    compare = request.args.get("comparisons", "")
    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", "1d"))
    tolerance = {"5m": pd.Timedelta("10min"), "30m": pd.Timedelta("1h"), "1d": pd.Timedelta("3D")}.get(yf_interval, pd.Timedelta("3D"))

    try:
        main_usd = get_usd_series_for_stock(ticker, period)
        if main_usd.empty:
            return jsonify({"error": f"No data found for {ticker}"}), 404

        result = {ticker: series_to_points(main_usd, normalized=True)}
        master = pd.to_datetime(main_usd.index)

        for label in [s.strip() for s in compare.split(",") if s.strip()]:
            sym = US_COMPARISON_ASSETS.get(label)
            if not sym:
                continue
            s = get_cached_or_live_series(sym, period, yf_interval, auto_adjust=False)
            aligned = align_to_master(master, s, tolerance)
            result[label] = series_to_points(aligned, normalized=True)

        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/chart")
def chart_data():
    ticker = request.args.get("ticker", "")
    period = request.args.get("period", "1M")
    compare = request.args.get("comparisons", "")
    extra_stocks = request.args.get("stocks", "")

    if not ticker:
        return jsonify({"error": "ticker is required"}), 400

    yf_period, yf_interval = PERIOD_MAP.get(period, ("1mo", "1d"))
    tolerance = {"5m": pd.Timedelta("10min"), "30m": pd.Timedelta("1h"), "1d": pd.Timedelta("3D")}.get(yf_interval, pd.Timedelta("3D"))

    try:
        main_series = get_cached_or_live_series(ticker, period, yf_interval, auto_adjust=False)
        if main_series.empty:
            return jsonify({"error": f"No data found for {ticker}"}), 404

        result = {ticker: series_to_points(main_series, normalized=True)}
        master = pd.to_datetime(main_series.index)

        for label in [s.strip() for s in compare.split(",") if s.strip()]:
            sym = COMPARISON_ASSETS.get(label)
            if not sym:
                continue
            s = get_cached_or_live_series(sym, period, yf_interval, auto_adjust=False)
            aligned = align_to_master(master, s, tolerance)
            result[label] = series_to_points(aligned, normalized=True)

        for sym in [s.strip() for s in extra_stocks.split(",") if s.strip() and s.strip() != ticker]:
            s = get_cached_or_live_series(sym, period, yf_interval, auto_adjust=False)
            aligned = align_to_master(master, s, tolerance)
            result[sym] = series_to_points(aligned, normalized=True)

        return jsonify(result)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/ohlcv")
def ohlcv():
    ticker = request.args.get("ticker", "")
    period = request.args.get("period", "1M")
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    try:
        df = get_cached_or_live_ohlcv(ticker, period)
        return jsonify(ohlcv_to_list(df))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/standings")
def standings():
    period = request.args.get("period", "1Y")
    view = request.args.get("view", "all")

    try:
        # DB-only. No network fetch on request path.
        data = get_standings_cached(period, view)
        return jsonify(data)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/backfill", methods=["POST"])
def admin_backfill():
    payload = request.get_json(silent=True) or {}
    tickers = payload.get("tickers") or [s["ticker"] for s in POPULAR_BIST_STOCKS]
    done = []

    try:
        batch_refresh_daily_price_history(tickers)
        for ticker in tickers:
            try:
                ensure_stock_info(ticker)
                done.append(ticker)
            except Exception:
                traceback.print_exc()

        ensure_fx_history()
        invalidate_standings_cache()
        return jsonify({"ok": True, "processed": done, "db_path": DB_PATH})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


init_db()
start_background_sync()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)