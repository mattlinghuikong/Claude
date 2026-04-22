import os
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-opus-4-7"

TOP_ANALYSTS_PER_MARKET = 20
TOP_STOCKS_FINAL = 10
ANALYST_RANK_YEAR = "2024"

# Min recommendations from top analysts to be included
MIN_ANALYST_COVERAGE = 2

# Score weights
WEIGHT_WIN_RATE = 0.4
WEIGHT_ANALYST_COUNT = 0.3
WEIGHT_EXCESS_RETURN = 0.2
WEIGHT_RECENCY = 0.1

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
