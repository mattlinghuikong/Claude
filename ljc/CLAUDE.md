# CLAUDE.md — Stock Research Report Generator

## Project Overview

This project generates a professional stock research report covering A-shares (CN), Hong Kong (HK), and US equities. It identifies the top 20 analysts by win rate in each market, aggregates their buy recommendations, scores stocks across all three markets, and outputs a ranked list of the top 10 most recommended stocks with an optional Claude AI investment thesis for each.

## Directory Structure

```
ljc/
├── CLAUDE.md
└── stock_research/
    ├── main.py                        # Entry point and orchestrator
    ├── config.py                      # All tuneable constants
    ├── requirements.txt               # Python dependencies
    ├── .env.example                   # API key template
    ├── .venv/                         # Python virtual environment
    ├── fetchers/
    │   ├── cn_fetcher.py              # A-shares via akshare → Eastmoney
    │   ├── hk_fetcher.py              # HK via AASTOCKS (fallback: curated list)
    │   └── us_fetcher.py              # US via TipRanks (fallback: curated list)
    ├── analyzers/
    │   └── stock_scorer.py            # Composite scoring + cross-market ranking
    ├── ai/
    │   └── claude_analyzer.py         # Claude API integration for investment theses
    └── report/
        ├── generator.py               # Jinja2 HTML report builder
        └── templates/report.html      # Styled report template
```

## How to Run

```bash
cd stock_research
source .venv/bin/activate

# Data + scoring only (no API key needed, ~20s)
python3 main.py --no-ai

# Full run with Claude AI analysis (requires ANTHROPIC_API_KEY)
cp .env.example .env   # add key to .env
python3 main.py

# Single market
python3 main.py --market CN    # or HK, US
```

Output is written to `stock_research/output/stock_report_<timestamp>.html` and opened automatically in the browser on macOS.

## Data Sources

| Market | Analyst Rankings | Stock Recommendations |
|--------|-----------------|----------------------|
| **A股 (CN)** | `akshare.stock_analyst_rank_em(year="2024")` — Eastmoney annual analyst ranking | `akshare.stock_analyst_detail_em(analyst_id=..., indicator="最新跟踪成分股")` |
| **港股 (HK)** | Curated list of top 20 HK brokers (Goldman, Morgan Stanley, CICC, CLSA, etc.) | AASTOCKS consensus page; falls back to curated 15-stock list |
| **美股 (US)** | TipRanks top analysts API; falls back to curated 20-analyst list (TipRanks 2024 rankings) | TipRanks top-stocks API; falls back to curated 15-stock list |

TipRanks and AASTOCKS return 403/timeout in automated contexts — the fallbacks are based on their publicly published annual rankings and Wall Street consensus data.

## Scoring Model

Composite score per stock (normalised 0–1 within each market, then merged globally):

```
composite = 0.4 × win_rate_norm
          + 0.3 × analyst_count_norm
          + 0.2 × excess_return_norm
          + 0.1 × recency_norm
```

Weights are in `config.py` (`WEIGHT_WIN_RATE`, etc.).

**CN note:** A-share analysts are sector specialists with little stock overlap, so the minimum analyst coverage threshold is set to 1 (vs 2 for HK/US). The "win rate" metric for CN is the analyst's 2024 annual return (e.g. 118%) rather than a traditional win/loss percentage, because that is what akshare exposes.

## akshare API Notes

The akshare column names changed from what documentation suggested. Confirmed working schema:

`stock_analyst_rank_em(year="2024")` returns:
- `分析师名称`, `分析师单位`, `年度指数`, `2024年收益率`, `12个月收益率`, `成分股个数`, `分析师ID`, `行业`

`stock_analyst_detail_em(analyst_id=<str>, indicator="最新跟踪成分股")` returns:
- `股票代码`, `股票名称`, `调入日期`, `最新评级日期`, `当前评级名称`, `成交价格(前复权)`, `最新价格`, `阶段涨跌幅`
- The parameter is `analyst_id` (not `analyst_name`).

## Claude AI Integration

`ai/claude_analyzer.py` uses `claude-opus-4-7` and makes three calls per run:

1. **Analyst landscape summary** — 200-word overview of the three markets' analyst quality
2. **Per-stock investment thesis** — ~4-paragraph thesis (logic, catalyst, risk, rating) for each of the top 10 stocks
3. **Portfolio summary** — 300-word cross-market portfolio commentary

All prompts and the system message are in Chinese. Requires `ANTHROPIC_API_KEY` in `.env`.

## Configuration (`config.py`)

| Constant | Default | Purpose |
|----------|---------|---------|
| `TOP_ANALYSTS_PER_MARKET` | 20 | Analysts fetched per market |
| `TOP_STOCKS_FINAL` | 10 | Final stocks in the report |
| `ANALYST_RANK_YEAR` | `"2024"` | Year passed to akshare |
| `MIN_ANALYST_COVERAGE` | 2 | Min analysts for HK/US stocks to qualify |
| `ANTHROPIC_MODEL` | `claude-opus-4-7` | Model used for AI analysis |

## Dependencies

Install inside the venv:
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install akshare yfinance pandas requests beautifulsoup4 anthropic jinja2 lxml python-dotenv tqdm plotly
```

Python 3.14 confirmed working. Homebrew Python requires a venv (PEP 668 — no `--break-system-packages` needed).
