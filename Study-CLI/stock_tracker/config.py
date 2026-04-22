import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "analyst_tracker.db")

TOP_ANALYST_COUNT = 20

# US watchlist — broad coverage across sectors
US_WATCHLIST = [
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA", "AMD",
    # Semiconductors
    "AVGO", "TSM", "QCOM", "INTC", "MU",
    # Finance
    "JPM", "GS", "MS", "BAC", "V", "MA", "BRK-B",
    # Healthcare
    "LLY", "UNH", "JNJ", "ABBV", "MRK", "PFE",
    # Energy
    "XOM", "CVX", "COP",
    # Consumer
    "WMT", "COST", "MCD", "NKE",
    # Industrial / Defense
    "CAT", "BA", "HON", "RTX",
    # Cloud / Enterprise
    "CRM", "NOW", "SNOW", "PLTR",
    # Biotech
    "MRNA", "REGN", "VRTX",
]

# HK watchlist — major constituents
HK_WATCHLIST = [
    "0700.HK",  # Tencent
    "9988.HK",  # Alibaba
    "3690.HK",  # Meituan
    "9618.HK",  # JD.com
    "1810.HK",  # Xiaomi
    "0005.HK",  # HSBC
    "1299.HK",  # AIA
    "0388.HK",  # HKEX
    "2318.HK",  # Ping An
    "0941.HK",  # China Mobile
    "3988.HK",  # Bank of China
    "0016.HK",  # Sun Hung Kai
    "1177.HK",  # Sino Biopharm
    "2269.HK",  # Wuxi Biologics
    "0883.HK",  # CNOOC
    "0027.HK",  # Galaxy Entertainment
    "0175.HK",  # Geely Auto
    "9999.HK",  # NetEase
    "6690.HK",  # Haier Smart Home
    "9868.HK",  # Xpeng
]

# Min upside threshold to include a stock in the report
MIN_UPSIDE_PCT = 10.0

# Analyst consensus: require at least this many buy/strong-buy ratings
MIN_BUY_RATINGS = 3

# ── Optional API keys (all free, no credit card required) ──────────────────
#
# FMP (Financial Modeling Prep) — free tier, 250 req/day
#   Register at: https://financialmodelingprep.com/register
#   Provides: analyst upgrades/downgrades with firm names, EPS estimates,
#             price target consensus for US + HK stocks.
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
#
# Tushare — free tier, ~120 req/min
#   Register at: https://tushare.pro/register
#   Provides: A-share financial statements, analyst consensus, fund holdings.
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "")
# ──────────────────────────────────────────────────────────────────────────
