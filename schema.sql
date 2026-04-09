PRAGMA journal_mode=WAL;

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
