import sqlite3
import os
from datetime import date
from config import DB_PATH


def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
    """)
    conn.commit()
    conn.close()


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
