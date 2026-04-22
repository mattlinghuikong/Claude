#!/usr/bin/env python3
"""
Daily stock analyst report runner.
Usage:  python run.py
        python run.py --date 2025-01-15   (backfill a specific date)
        python run.py --no-cn             (skip China, faster)
        python run.py --text              (also save plain-text version)
"""

import sys
import os
import argparse
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import FMP_API_KEY
from db import init_db
from fetchers.us_market import build_us_report_data
from fetchers.hk_market import build_hk_report_data
from fetchers.cn_market import build_cn_report_data
from fetchers.cn_extended import build_market_context
from fetchers.finviz_fetcher import enrich_with_finviz
from fetchers.fmp_fetcher import enrich_with_fmp
from aggregator import aggregate_all
from report_html_generator import generate_html, save_html
from report_generator import generate_report, save_report  # kept for --text fallback


def main():
    parser = argparse.ArgumentParser(description="Generate daily analyst stock report")
    parser.add_argument("--date",   default=None,        help="Report date (YYYY-MM-DD)")
    parser.add_argument("--no-cn",  action="store_true", help="Skip China market")
    parser.add_argument("--no-us",  action="store_true", help="Skip US market")
    parser.add_argument("--no-hk",  action="store_true", help="Skip HK market")
    parser.add_argument("--no-ext", action="store_true", help="Skip extended sources (Finviz, FMP, fund flows)")
    parser.add_argument("--text",   action="store_true", help="Also save plain-text version alongside HTML")
    args = parser.parse_args()

    report_date = args.date or date.today().isoformat()
    print(f"\n[Stock Tracker] Generating report for {report_date}")
    print("=" * 60)

    init_db()

    # ── US Market ──────────────────────────────────────────────────
    us_stocks = []
    if not args.no_us:
        print("[1/5] Fetching US market data (Yahoo Finance)...")
        us_stocks, err = build_us_report_data()
        if err:
            print(f"      Warning: {err}")
        else:
            print(f"      {len(us_stocks)} qualifying US stocks.")

        if not args.no_ext:
            print("      Enriching with Finviz analyst upgrades...")
            us_stocks, err = enrich_with_finviz(us_stocks)
            if err:
                print(f"      Finviz warning: {err}")

            if FMP_API_KEY:
                print("      Enriching with FMP analyst grades...")
                us_stocks, err = enrich_with_fmp(us_stocks, FMP_API_KEY)
                if err:
                    print(f"      FMP warning: {err}")
            else:
                print("      FMP skipped (set FMP_API_KEY in config.py to enable)")

    # ── HK Market ─────────────────────────────────────────────────
    hk_stocks = []
    if not args.no_hk:
        print("[2/5] Fetching HK market data (Yahoo Finance)...")
        hk_stocks, err = build_hk_report_data()
        if err:
            print(f"      Warning: {err}")
        else:
            print(f"      {len(hk_stocks)} qualifying HK stocks.")

        if not args.no_ext and FMP_API_KEY:
            print("      Enriching HK with FMP analyst grades...")
            hk_stocks, err = enrich_with_fmp(hk_stocks, FMP_API_KEY)

    # ── China A-shares ─────────────────────────────────────────────
    cn_data = []
    if not args.no_cn:
        print("[3/5] Fetching CN analyst rankings (Eastmoney)...")
        cn_data, err = build_cn_report_data(top_n_analysts=20, picks_per_analyst=3)
        if err:
            print(f"      Warning: {err}")
        else:
            print(f"      {len(cn_data)} analyst profiles retrieved.")

    # ── Extended CN/HK Market Context ─────────────────────────────
    market_ctx = {}
    if not args.no_cn and not args.no_ext:
        print("[4/5] Fetching extended market context...")
        print("      → Sector fund flows | Hot stocks | Research reports (Eastmoney + Xueqiu)")
        market_ctx = build_market_context()
        n_flows = len(market_ctx.get("sector_flows_today", []))
        n_reports = len(market_ctx.get("latest_reports", []))
        print(f"      Sector flows: {n_flows} | Reports: {n_reports}")
    else:
        print("[4/5] Extended context skipped.")

    # ── Aggregate & Prioritise ─────────────────────────────────────
    print("[5/5] Aggregating, ranking, and generating HTML report...")
    aggregated = aggregate_all(us_stocks, hk_stocks, cn_data)
    stats = aggregated.get("stats", {})
    print(f"      Tier 1 (High Conviction): {len(aggregated.get('tier1', []))} stocks")
    print(f"      Tier 2 (Strong Picks):    {len(aggregated.get('tier2', []))} stocks")
    print(f"      Tier 3 (Watch List):      {len(aggregated.get('tier3', []))} stocks")

    # ── HTML Report ────────────────────────────────────────────────
    html = generate_html(aggregated, market_ctx, report_date=report_date)
    html_path = save_html(html, report_date=report_date)
    print(f"\n[Done] HTML report: {html_path}")

    # ── Optional plain-text ────────────────────────────────────────
    if args.text:
        txt = generate_report(us_stocks, hk_stocks, cn_data, market_ctx, report_date=report_date)
        txt_path = save_report(txt, report_date=report_date)
        print(f"[Done] Text report: {txt_path}")

    print("=" * 60)
    print(f"      Open in browser: open \"{html_path}\"")


if __name__ == "__main__":
    main()
