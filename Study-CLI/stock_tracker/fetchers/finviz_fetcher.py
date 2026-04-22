"""
Finviz analyst ratings feed — free, no API key required.
Scrapes https://finviz.com/ratings.ashx for recent US analyst
upgrade/downgrade actions with firm name, rating, and price target.
"""

import traceback
from datetime import date, timedelta

try:
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
except ImportError:
    requests = None
    BeautifulSoup = None
    pd = None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finviz.com/",
}

BUY_ACTIONS = {"upgrade", "initiated", "reiterated", "resumed", "maintains"}
BUY_GRADES  = {"buy", "strong buy", "outperform", "overweight", "positive",
               "accumulate", "market outperform", "sector outperform", "add"}


def fetch_ratings(pages=3):
    """
    Fetch recent analyst ratings from Finviz.
    Returns a list of dicts: ticker, company, action, brokerage,
    rating_from, rating_to, price_target, date.
    """
    if requests is None or BeautifulSoup is None:
        return [], "requests/bs4 not installed"

    all_rows = []
    for page in range(1, pages + 1):
        url = f"https://finviz.com/ratings.ashx?v=3&p={page}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            # Finviz ratings table
            table = soup.find("table", class_="ratings-table")
            if table is None:
                # Try alternate selector
                tables = soup.find_all("table")
                table = next(
                    (t for t in tables if "Analyst" in t.get_text()[:200]), None
                )
            if table is None:
                continue

            rows = table.find_all("tr")
            for row in rows[1:]:  # skip header
                cols = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cols) < 6:
                    continue
                # Typical columns: Date | Action | Company | Analyst | Rating Change | Price Target
                entry = {
                    "date": cols[0] if cols[0] else "",
                    "action": cols[1].lower() if len(cols) > 1 else "",
                    "ticker": "",
                    "company": cols[2] if len(cols) > 2 else "",
                    "brokerage": cols[3] if len(cols) > 3 else "",
                    "rating_change": cols[4] if len(cols) > 4 else "",
                    "price_target": cols[5] if len(cols) > 5 else "",
                }
                # Extract ticker from link inside row
                link = row.find("a", class_="tab-link")
                if link and link.get("href"):
                    href = link["href"]
                    if "quote.ashx?t=" in href:
                        entry["ticker"] = href.split("t=")[-1].split("&")[0].upper()
                    elif "/quote/" in href:
                        entry["ticker"] = href.split("/quote/")[-1].split("?")[0].upper()
                all_rows.append(entry)
        except Exception as e:
            continue

    return all_rows, None


def get_bullish_upgrades(days=7, min_pages=3):
    """
    Return only bullish actions (upgrades, initiations with buy) from the
    last N days. Keyed by ticker for easy lookup.
    """
    rows, err = fetch_ratings(pages=min_pages)
    if err and not rows:
        return {}, err

    cutoff = date.today() - timedelta(days=days)
    result = {}

    for row in rows:
        action = row.get("action", "").lower()
        change = row.get("rating_change", "").lower()
        ticker = row.get("ticker", "")

        if not ticker:
            continue

        # Keep upgrades and strong-buy initiations
        is_bullish = (
            action in BUY_ACTIONS
            and any(g in change for g in BUY_GRADES)
        ) or action == "upgrade"

        if not is_bullish:
            continue

        if ticker not in result:
            result[ticker] = []
        result[ticker].append({
            "date": row["date"],
            "action": row["action"],
            "brokerage": row["brokerage"],
            "rating_change": row["rating_change"],
            "price_target": row["price_target"],
        })

    return result, None


def enrich_with_finviz(stock_list):
    """
    Add Finviz upgrade entries to each stock dict in stock_list.
    Mutates in place; returns the same list.
    """
    upgrades_map, err = get_bullish_upgrades(days=30)
    if err or not upgrades_map:
        return stock_list, err

    for stock in stock_list:
        ticker = stock.get("ticker", "")
        fv_data = upgrades_map.get(ticker, [])
        if fv_data:
            existing = stock.get("finviz_upgrades", [])
            stock["finviz_upgrades"] = existing + fv_data

    return stock_list, None
