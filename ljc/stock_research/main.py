#!/usr/bin/env python3
"""
Stock Research Report Generator
Covers A-shares (CN), Hong Kong (HK), and US equities.

Usage:
    python main.py                  # full run with AI analysis
    python main.py --no-ai          # skip Claude AI (data + scoring only)
    python main.py --market CN      # single market
"""

import sys
import argparse
import subprocess
import os
import pandas as pd
from tqdm import tqdm

# ── bootstrap path ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

from fetchers   import CNAnalystFetcher, HKAnalystFetcher, USAnalystFetcher
from analyzers  import StockScorer
from report     import ReportGenerator
import config


def run(markets: list[str] = None, use_ai: bool = True) -> str:
    if markets is None:
        markets = ["CN", "HK", "US"]

    print("=" * 60)
    print("  Stock Research Report Generator")
    print(f"  Markets : {', '.join(markets)}")
    print(f"  AI mode : {'enabled' if use_ai else 'disabled'}")
    print("=" * 60)

    # ── 1. Fetch analysts ────────────────────────────────────────────────────
    cn_analysts = pd.DataFrame()
    hk_analysts = pd.DataFrame()
    us_analysts = pd.DataFrame()
    cn_recs = hk_recs = us_recs = pd.DataFrame()

    if "CN" in markets:
        fetcher = CNAnalystFetcher()
        cn_analysts = fetcher.get_top_analysts()
        if not cn_analysts.empty:
            cn_recs = fetcher.collect_recommendations(cn_analysts)
            print(f"  [CN] {len(cn_recs)} recommendations collected")

    if "HK" in markets:
        fetcher = HKAnalystFetcher()
        hk_analysts = fetcher.get_top_analysts()
        if not hk_analysts.empty:
            hk_recs = fetcher.collect_recommendations(hk_analysts)
            print(f"  [HK] {len(hk_recs)} recommendations collected")

    if "US" in markets:
        fetcher = USAnalystFetcher()
        us_analysts = fetcher.get_top_analysts()
        if not us_analysts.empty:
            us_recs = fetcher.collect_recommendations(us_analysts)
            print(f"  [US] {len(us_recs)} recommendations collected")

    # ── 2. Score stocks ──────────────────────────────────────────────────────
    print("\n[Scoring] Computing composite scores across markets...")
    scorer = StockScorer()

    cn_scored = scorer.score_cn(cn_recs)
    hk_scored = scorer.score_hk(hk_recs)
    us_scored = scorer.score_us(us_recs)

    top_stocks = scorer.select_top_stocks(cn_scored, hk_scored, us_scored)

    if top_stocks.empty:
        print("ERROR: No stocks scored. Check data fetching above.")
        sys.exit(1)

    print(f"\n[Result] Top {len(top_stocks)} stocks selected:")
    for i, (_, row) in enumerate(top_stocks.iterrows(), 1):
        market = row.get("market", row.get("market_label", "?"))
        print(f"  {i:2d}. {row.get('name','?'):20s} {row.get('ticker','?'):8s} [{market}]")

    # ── 3. AI analysis ───────────────────────────────────────────────────────
    analyst_summary = ""
    portfolio_summary = ""
    stock_theses: dict = {}

    if use_ai:
        try:
            from ai import ClaudeAnalyzer
            analyzer = ClaudeAnalyzer()

            print("\n[AI] Generating analyst landscape summary...")
            analyst_summary = analyzer.analyze_top_analysts(
                cn_analysts, hk_analysts, us_analysts
            )

            print("[AI] Generating investment theses for each stock...")
            for i, (_, row) in enumerate(
                tqdm(top_stocks.iterrows(), total=len(top_stocks), desc="Theses"),
                start=1,
            ):
                result = analyzer.generate_stock_thesis(row, rank=i)
                stock_theses[result["ticker"]] = result["thesis"]

            print("[AI] Generating portfolio summary...")
            portfolio_summary = analyzer.generate_portfolio_summary(top_stocks)

        except Exception as e:
            print(f"\n[AI] Skipped — {e}")

    # ── 4. Generate report ───────────────────────────────────────────────────
    print("\n[Report] Building HTML report...")
    generator = ReportGenerator()
    out_path = generator.build(
        cn_analysts=cn_analysts,
        hk_analysts=hk_analysts,
        us_analysts=us_analysts,
        top_stocks=top_stocks,
        analyst_summary=analyst_summary,
        portfolio_summary=portfolio_summary,
        stock_theses=stock_theses,
    )

    print(f"\n✅  Report saved to: {out_path}")

    # Try to open in default browser (macOS)
    if sys.platform == "darwin":
        subprocess.run(["open", out_path], check=False)

    return out_path


def main():
    parser = argparse.ArgumentParser(description="Global Stock Research Report Generator")
    parser.add_argument(
        "--market", choices=["CN", "HK", "US"],
        help="Run for a single market only"
    )
    parser.add_argument(
        "--no-ai", action="store_true",
        help="Skip Claude AI analysis (faster, no API key needed)"
    )
    args = parser.parse_args()

    markets = [args.market] if args.market else ["CN", "HK", "US"]
    run(markets=markets, use_ai=not args.no_ai)


if __name__ == "__main__":
    main()
