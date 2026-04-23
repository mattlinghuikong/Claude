import sqlite3
import os
from contextlib import contextmanager
from datetime import date
from config import DB_PATH


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def batch_conn():
    """Context manager: open once, commit once, close once. Use for bulk writes."""
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS analyst_calls (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            market      TEXT NOT NULL,
            analyst     TEXT NOT NULL,
            firm        TEXT,
            ticker      TEXT NOT NULL,
            rating      TEXT,
            price_target REAL,
            price_at_call REAL,
            call_date   TEXT NOT NULL,
            recorded_at TEXT NOT NULL DEFAULT (date('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_calls_ticker ON analyst_calls(ticker);
        CREATE INDEX IF NOT EXISTS idx_calls_analyst ON analyst_calls(market, analyst);

        CREATE TABLE IF NOT EXISTS price_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            market      TEXT NOT NULL,
            price       REAL,
            snapshot_date TEXT NOT NULL,
            UNIQUE(ticker, snapshot_date)
        );

        CREATE TABLE IF NOT EXISTS analyst_winrates (
            market      TEXT NOT NULL,
            analyst     TEXT NOT NULL,
            firm        TEXT,
            total_calls INTEGER DEFAULT 0,
            winning_calls INTEGER DEFAULT 0,
            win_rate    REAL DEFAULT 0.0,
            avg_return  REAL DEFAULT 0.0,
            last_updated TEXT,
            PRIMARY KEY (market, analyst)
        );

        -- Tier 1/2 picks published by the tool itself — used to measure
        -- realised performance (pick made money or not) over time.
        CREATE TABLE IF NOT EXISTS published_picks (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date   TEXT NOT NULL,
            market        TEXT NOT NULL,
            ticker        TEXT NOT NULL,
            name          TEXT,
            tier          INTEGER,
            priority_score REAL,
            price_at_pub  REAL,
            target_price  REAL,
            upside_pct    REAL,
            recommender_count INTEGER,
            UNIQUE(report_date, ticker)
        );
        CREATE INDEX IF NOT EXISTS idx_picks_ticker ON published_picks(ticker);
        CREATE INDEX IF NOT EXISTS idx_picks_date   ON published_picks(report_date);
    """)
    conn.commit()
    conn.close()


def record_published_picks(report_date, picks):
    """Write today's tier1+tier2 picks into published_picks (single transaction).
    `picks` is a list of stock dicts from the aggregator."""
    if not picks:
        return 0
    rows = []
    for s in picks:
        ticker = str(s.get("ticker") or "").strip()
        if not ticker:
            continue
        rows.append((
            report_date,
            s.get("market", ""),
            ticker,
            s.get("name", ""),
            int(s.get("tier", 3)),
            float(s.get("priority_score") or 0.0),
            float(s.get("current_price") or 0.0),
            float(s.get("price_target") or s.get("target_mean") or 0.0),
            float(s.get("upside_pct") or 0.0),
            int(s.get("recommender_count") or 0),
        ))
    if not rows:
        return 0
    with batch_conn() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO published_picks
                (report_date, market, ticker, name, tier, priority_score,
                 price_at_pub, target_price, upside_pct, recommender_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
    return len(rows)


def get_realised_performance(lookback_days=180, min_age_days=7):
    """Return realised performance rows: past picks ≥7 days old with
    current prices from price_snapshots. Used to show 'which past picks
    actually made money' on the HTML report."""
    sql = """
        SELECT p.report_date, p.market, p.ticker, p.name, p.tier,
               p.price_at_pub, p.target_price, p.upside_pct,
               (SELECT price FROM price_snapshots
                 WHERE ticker = p.ticker
                 ORDER BY snapshot_date DESC LIMIT 1) AS current_price
          FROM published_picks p
         WHERE p.report_date <= date('now', ?)
           AND p.report_date >= date('now', ?)
           AND p.tier IN (1, 2)
         ORDER BY p.report_date DESC
    """
    conn = get_conn()
    try:
        rows = conn.execute(sql, (
            f"-{min_age_days} days", f"-{lookback_days} days",
        )).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def snapshot_prices(stocks, snapshot_date=None):
    """Bulk-insert today's prices for each stock. Accepts list of aggregator dicts."""
    snap = snapshot_date or date.today().isoformat()
    rows = []
    for s in stocks:
        price = s.get("current_price")
        ticker = s.get("ticker")
        market = s.get("market", "")
        if not ticker or price in (None, 0):
            continue
        try:
            rows.append((str(ticker), str(market), float(price), snap))
        except (TypeError, ValueError):
            continue
    if not rows:
        return 0
    with batch_conn() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO price_snapshots (ticker, market, price, snapshot_date)
            VALUES (?, ?, ?, ?)
        """, rows)
    return len(rows)


def upsert_analyst_call(market, analyst, firm, ticker, rating, price_target, price_at_call, call_date):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO analyst_calls (market, analyst, firm, ticker, rating, price_target, price_at_call, call_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (market, analyst, firm, ticker, rating, price_target, price_at_call, call_date))
    conn.commit()
    conn.close()


def upsert_price_snapshot(ticker, market, price, snapshot_date):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO price_snapshots (ticker, market, price, snapshot_date)
        VALUES (?, ?, ?, ?)
    """, (ticker, market, price, snapshot_date))
    conn.commit()
    conn.close()


def upsert_winrate(market, analyst, firm, total, winning, win_rate, avg_return):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO analyst_winrates (market, analyst, firm, total_calls, winning_calls, win_rate, avg_return, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, date('now'))
        ON CONFLICT(market, analyst) DO UPDATE SET
            firm=excluded.firm,
            total_calls=excluded.total_calls,
            winning_calls=excluded.winning_calls,
            win_rate=excluded.win_rate,
            avg_return=excluded.avg_return,
            last_updated=excluded.last_updated
    """, (market, analyst, firm, total, winning, win_rate, avg_return))
    conn.commit()
    conn.close()


def get_top_analysts(market, limit=20):
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("""
        SELECT * FROM analyst_winrates
        WHERE market = ? AND total_calls >= 3
        ORDER BY win_rate DESC, avg_return DESC
        LIMIT ?
    """, (market, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_analyst_recent_calls(market, analyst, days=90):
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("""
        SELECT * FROM analyst_calls
        WHERE market = ? AND analyst = ?
          AND call_date >= date('now', ?)
        ORDER BY call_date DESC
    """, (market, analyst, f"-{days} days")).fetchall()
    conn.close()
    return [dict(r) for r in rows]
