# Stock Tracker — Project Overview

Daily analyst report generator covering US (NYSE/NASDAQ), Hong Kong (HKEX), and China A-shares. Fetches free public data from multiple sources, ranks stocks by analyst conviction, and outputs a self-contained Apple-style HTML report.

## Running the project

```bash
# Full run (all markets + extended sources)
python3 run.py

# Fast run — skip China and extended sources
python3 run.py --no-cn --no-ext

# Also save a plain-text version alongside the HTML
python3 run.py --text

# Backfill a specific date
python3 run.py --date 2026-04-20

# Open the latest report
open reports/$(date +%Y-%m-%d).html
```

Reports are saved to `reports/YYYY-MM-DD.html`. The cron job runs daily at 7:00 AM local time (added to `crontab -l`), with stdout/stderr logged to `cron.log`.

## File structure

```
stock_tracker/
├── run.py                    # Entry point — orchestrates all fetchers and generators
├── config.py                 # Watchlists, thresholds, optional API keys
├── aggregator.py             # Inverts/merges data → priority scores → tier assignment
├── db.py                     # SQLite helpers (analyst_calls, price_snapshots, winrates)
├── report_html_generator.py  # Apple-style self-contained HTML output
├── report_generator.py       # Plain-text output (kept as fallback / --text flag)
├── requirements.txt
├── fetchers/
│   ├── us_market.py          # yfinance — US stocks, analyst consensus, upgrades
│   ├── hk_market.py          # yfinance — HK stocks (same logic as US)
│   ├── cn_market.py          # akshare — Eastmoney top-20 analysts + their picks
│   ├── cn_extended.py        # akshare — sector fund flows, hot stocks, research reports
│   ├── finviz_fetcher.py     # Finviz scraping — US analyst upgrade feed (no key needed)
│   └── fmp_fetcher.py        # FMP API — US/HK upgrade grades + EPS estimates (optional)
├── data/
│   └── analyst_tracker.db    # SQLite — tracks calls over time to self-compute win rates
└── reports/                  # Generated HTML (and optional .txt) reports
```

## Data sources (all free, no credit card)

| Source | Markets | What it provides | Key |
|--------|---------|-----------------|-----|
| Yahoo Finance (`yfinance`) | US, HK | Price, analyst consensus, price targets, upgrades history | None |
| Eastmoney 东方财富 (`akshare`) | CN | Top-20 analysts ranked by win rate, their picks, fund flows, research reports | None |
| Xueqiu 雪球 (`akshare`) | CN, HK | Hot stocks by social follower attention | None |
| Finviz (scraping) | US | Analyst upgrade/downgrade feed, last 30 days | None |
| FMP free tier | US, HK | Upgrade grades with firm names, EPS estimates | `FMP_API_KEY` env var |
| Tushare free tier | CN | A-share financials, analyst consensus | `TUSHARE_TOKEN` env var |

Optional keys are read from environment variables — set them in your shell profile or pass them before the command:
```bash
export FMP_API_KEY=your_key   # https://financialmodelingprep.com/register
export TUSHARE_TOKEN=your_tok # https://tushare.pro/register
```

## Priority / tier system (`aggregator.py`)

All stocks from all three markets flow through `aggregate_all()`, which:

1. **Inverts CN data** — `cn_data` comes in as `analyst → picks`; aggregator flips it to `ticker → {stock, recommenders[]}` so stocks recommended by multiple analysts are identified.
2. **Collects US/HK recommenders** — harvests unique firm names from `recent_upgrades`, `finviz_upgrades`, and `fmp_upgrades` fields.
3. **Scores** each stock: `recommender_count × 20 + min(upside_pct, 80) + (5 − rec_mean) × 8` (US/HK) or `+ avg_win_rate / 10` (CN).
4. **Assigns tier**:
   - **Tier 1 — High Conviction**: ≥ 3 recommenders, or ≥ 2 recommenders with ≥ 25% upside
   - **Tier 2 — Strong Picks**: ≥ 2 recommenders, or 1 recommender with ≥ 20% upside + strong consensus
   - **Tier 3 — Watch List**: everything else
5. Within each tier, stocks are sorted by priority score descending.

## Report date filtering

All fetchers enforce a **30-day lookback** — reports or analyst calls older than 30 days are silently skipped. This applies to:
- `cn_market.py:get_analyst_picks()` — filters by `report_date`
- `cn_extended.py` — both `get_research_reports_for_stock()` and `get_latest_reports_for_universe()`
- `finviz_fetcher.py` — 30-day window on the upgrade feed
- `fmp_fetcher.py` — filters `publishedDate` before returning results

Unparseable or missing dates pass through (treated as recent) to avoid silently discarding data.

## Sector fund flows note

`cn_extended.py:get_sector_fund_flows()` and `get_top_inflow_stocks()` pull from Eastmoney's real-time push server. This data is **only available during and shortly after A-share trading hours (9:30 am – ~3:30 pm CST)**. Outside those hours the calls time out gracefully; the report section is omitted rather than showing an error.

## Key config knobs (`config.py`)

| Variable | Default | Purpose |
|----------|---------|---------|
| `US_WATCHLIST` | 40 tickers | Stocks checked for US analyst consensus |
| `HK_WATCHLIST` | 20 tickers | Stocks checked for HK analyst consensus |
| `TOP_ANALYST_COUNT` | 20 | Number of Eastmoney analysts retrieved per year |
| `MIN_UPSIDE_PCT` | 10.0 | Minimum analyst target upside to include a stock |
| `MIN_BUY_RATINGS` | 3 | Minimum analyst count required for US/HK inclusion |

## SQLite database (`db.py`)

Three tables in `data/analyst_tracker.db`:

- `analyst_calls` — every analyst recommendation logged with market, firm, ticker, rating, target price, and call date. Used to self-compute win rates over time (after ~1–3 months of data accumulates).
- `price_snapshots` — daily price records per ticker for outcome tracking.
- `analyst_winrates` — aggregated win rate and average return per analyst, updated incrementally.

## HTML report design

`report_html_generator.py` produces a self-contained file (no external CSS/JS dependencies). Design tokens mirror Apple.com:
- Font: `-apple-system, BlinkMacSystemFont, "SF Pro Display", "Helvetica Neue"`
- Background: `#f5f5f7`, cards: `#ffffff`, accent: `#0071e3`
- Card hover: `translateY(-3px)` with cubic-bezier transition
- Tier accents: gold top-border (Tier 1), blue (Tier 2), gray (Tier 3)
- Analyst chips display firm + grade (US/HK) or analyst name + win rate (CN)
- Fundamentals panel is collapsible via inline JS (`toggleFundamentals`)

## Dependencies

Install with:
```bash
pip3 install --break-system-packages -r requirements.txt
```

Core packages: `yfinance`, `akshare`, `pandas`, `requests`, `beautifulsoup4`, `lxml`
