"""
Generates a plain-text daily report from fetched analyst data.
"""

import os
from datetime import date
from config import REPORTS_DIR


def _fmt_price(val, currency="$"):
    if not val or val == 0:
        return "N/A"
    return f"{currency}{val:,.2f}"


def _fmt_pct(val):
    if val is None:
        return "N/A"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.1f}%"


def _fmt_cap(val):
    if not val:
        return "N/A"
    b = val / 1e9
    if b >= 1:
        return f"${b:.1f}B"
    return f"${val/1e6:.0f}M"


def _rec_label(rec_mean):
    if rec_mean is None:
        return "N/A"
    if rec_mean <= 1.5:
        return "Strong Buy"
    if rec_mean <= 2.5:
        return "Buy"
    if rec_mean <= 3.5:
        return "Hold"
    return "Sell"


def _build_us_hk_section(title, stocks, currency="$", max_stocks=15):
    lines = []
    lines.append("=" * 70)
    lines.append(f"  {title}")
    lines.append("=" * 70)

    if not stocks:
        lines.append("  No qualifying stocks found today.")
        lines.append("")
        return lines

    for i, s in enumerate(stocks[:max_stocks], 1):
        ticker = s.get("ticker", "?")
        name = s.get("name", ticker)
        sector = s.get("sector", "")
        industry = s.get("industry", "")
        price = s.get("current_price", 0)
        target = s.get("target_mean", 0)
        target_high = s.get("target_high", 0)
        upside = s.get("upside_pct")
        n_analysts = s.get("n_analysts", 0)
        rec = _rec_label(s.get("rec_mean"))
        upgrades = s.get("recent_upgrades", [])

        lines.append("")
        lines.append(f"#{i}  {ticker}  —  {name}")
        lines.append(f"    Sector / Industry : {sector} / {industry}")
        lines.append(f"    Current Price     : {_fmt_price(price, currency)}")
        lines.append(f"    Analyst Target    : {_fmt_price(target, currency)}  (high: {_fmt_price(target_high, currency)})")
        lines.append(f"    Upside to Target  : {_fmt_pct(upside)}")
        lines.append(f"    Consensus         : {rec}  ({n_analysts} analysts)")
        lines.append(f"    Market Cap        : {_fmt_cap(s.get('market_cap'))}")

        # Fundamentals
        pe = s.get("pe_ratio")
        fpe = s.get("forward_pe")
        rev_g = s.get("revenue_growth")
        earn_g = s.get("earnings_growth")
        margin = s.get("profit_margin")
        roe = s.get("roe")
        d_e = s.get("debt_to_equity")
        eps_f = s.get("eps_forward")
        w52h = s.get("52w_high")
        w52l = s.get("52w_low")

        lines.append("")
        lines.append("    FUNDAMENTALS")
        lines.append(f"      Trailing P/E      : {pe:.1f}x" if pe else "      Trailing P/E      : N/A")
        lines.append(f"      Forward P/E       : {fpe:.1f}x" if fpe else "      Forward P/E       : N/A")
        lines.append(f"      Forward EPS       : {_fmt_price(eps_f, currency)}")
        lines.append(f"      Revenue Growth    : {_fmt_pct(rev_g * 100 if rev_g else None)}")
        lines.append(f"      Earnings Growth   : {_fmt_pct(earn_g * 100 if earn_g else None)}")
        lines.append(f"      Profit Margin     : {_fmt_pct(margin * 100 if margin else None)}")
        lines.append(f"      Return on Equity  : {_fmt_pct(roe * 100 if roe else None)}")
        lines.append(f"      Debt / Equity     : {d_e:.1f}" if d_e else "      Debt / Equity     : N/A")
        lines.append(f"      52-Week Range     : {_fmt_price(w52l, currency)} — {_fmt_price(w52h, currency)}")

        # Recent upgrades
        if upgrades:
            lines.append("")
            lines.append("    RECENT ANALYST ACTIVITY (last 30 days)")
            for u in upgrades[:5]:
                action_word = {"up": "Upgraded", "init": "Initiated", "down": "Downgraded", "main": "Maintained"}.get(
                    u.get("action", "").lower(), u.get("action", "").title()
                )
                from_g = f" from {u['from_grade']}" if u.get("from_grade") else ""
                lines.append(f"      {u.get('date','?')}  {u.get('firm','?')}  →  {action_word}{from_g} to {u.get('to_grade','?')}")

        # Why worth buying
        lines.append("")
        lines.append("    WHY THIS STOCK IS WORTH WATCHING")
        reasons = _build_reasons_us_hk(s)
        for r in reasons:
            lines.append(f"      • {r}")

        # Business description snippet
        summary = s.get("business_summary", "").strip()
        if summary:
            lines.append("")
            lines.append("    BUSINESS")
            # wrap at 65 chars
            words = summary.split()
            line_buf = "      "
            for w in words:
                if len(line_buf) + len(w) + 1 > 67:
                    lines.append(line_buf.rstrip())
                    line_buf = "      " + w + " "
                else:
                    line_buf += w + " "
            if line_buf.strip():
                lines.append(line_buf.rstrip())

        lines.append("")
        lines.append("    " + "-" * 60)

    return lines


def _build_reasons_us_hk(s):
    reasons = []
    upside = s.get("upside_pct") or 0
    if upside >= 30:
        reasons.append(f"Analysts see {upside:.0f}% upside to consensus target — significant re-rating potential.")
    elif upside >= 15:
        reasons.append(f"Analyst consensus target implies {upside:.0f}% upside from current price.")

    rec_mean = s.get("rec_mean") or 3
    n = s.get("n_analysts", 0)
    if rec_mean <= 1.5 and n >= 5:
        reasons.append(f"Strong Buy consensus across {n} analysts — high conviction across the street.")
    elif rec_mean <= 2.0:
        reasons.append(f"Buy consensus from {n} analysts with aligned bullish price targets.")

    upgrades = s.get("recent_upgrades", [])
    if len(upgrades) >= 3:
        firms = ", ".join(set(u["firm"] for u in upgrades[:3] if u.get("firm")))
        reasons.append(f"Multiple recent upgrades ({firms}) — positive momentum shift.")
    elif upgrades:
        u = upgrades[0]
        reasons.append(f"Recent upgrade by {u.get('firm','?')} to {u.get('to_grade','?')} ({u.get('date','?')}).")

    rev_g = s.get("revenue_growth") or 0
    if rev_g >= 0.20:
        reasons.append(f"Revenue growing at {rev_g*100:.0f}% YoY — above-market top-line expansion.")
    elif rev_g >= 0.10:
        reasons.append(f"Solid {rev_g*100:.0f}% revenue growth supports earnings trajectory.")

    earn_g = s.get("earnings_growth") or 0
    if earn_g >= 0.20:
        reasons.append(f"Earnings growing at {earn_g*100:.0f}% YoY — strong EPS leverage.")

    margin = s.get("profit_margin") or 0
    if margin >= 0.25:
        reasons.append(f"High-quality business: {margin*100:.0f}% net profit margin.")

    roe = s.get("roe") or 0
    if roe >= 0.20:
        reasons.append(f"Return on equity of {roe*100:.0f}% — efficient capital allocation.")

    fpe = s.get("forward_pe") or 0
    pe = s.get("pe_ratio") or 0
    if 0 < fpe < pe * 0.8 and fpe > 0:
        reasons.append(f"Forward P/E of {fpe:.1f}x is significantly below trailing P/E — earnings acceleration expected.")
    elif 0 < fpe < 20:
        reasons.append(f"Reasonable valuation at {fpe:.1f}x forward earnings.")

    w52h = s.get("52w_high") or 0
    price = s.get("current_price") or 0
    if w52h > 0 and price > 0 and price < w52h * 0.85:
        pct_below = (w52h - price) / w52h * 100
        reasons.append(f"Trading {pct_below:.0f}% below 52-week high — potential mean-reversion opportunity.")

    if not reasons:
        reasons.append("Qualifies on analyst consensus and price target upside criteria.")
    return reasons


def _build_cn_section(cn_data, max_analysts=10):
    lines = []
    lines.append("=" * 70)
    lines.append("  CHINA A-SHARES  —  Top Analyst Picks (Eastmoney / 东方财富)")
    lines.append("=" * 70)

    if not cn_data:
        lines.append("  No data available from Eastmoney today.")
        lines.append("")
        return lines

    for i, analyst_entry in enumerate(cn_data[:max_analysts], 1):
        name = analyst_entry.get("analyst", "?")
        firm = analyst_entry.get("firm", "?")
        win_rate = analyst_entry.get("win_rate", 0)
        avg_return = analyst_entry.get("avg_return", 0)
        total_calls = analyst_entry.get("total_calls", 0)
        picks = analyst_entry.get("picks", [])

        lines.append("")
        lines.append(f"ANALYST #{i}  {name}  —  {firm}")
        lines.append(f"    Win Rate: {win_rate:.1f}%   |   Avg Return: {_fmt_pct(avg_return)}   |   Total Calls: {total_calls}")

        if not picks:
            lines.append("    (No current picks available)")
            lines.append("")
            continue

        for j, pick in enumerate(picks, 1):
            ticker = pick.get("ticker", "?")
            pname = pick.get("name", ticker)
            rating = pick.get("rating", "")
            target = pick.get("price_target", 0)
            current = pick.get("current_price")
            upside = pick.get("upside_pct")
            report_title = pick.get("report_title", "")
            report_date = pick.get("report_date", "")
            fundamentals = pick.get("fundamentals", {})

            lines.append("")
            lines.append(f"    Pick {j}: [{ticker}] {pname}")
            lines.append(f"      Rating          : {rating}")
            lines.append(f"      Current Price   : {_fmt_price(current, '¥') if current else 'N/A'}")
            lines.append(f"      Target Price    : {_fmt_price(target, '¥') if target else 'N/A'}")
            lines.append(f"      Upside          : {_fmt_pct(upside)}")
            if report_title:
                lines.append(f"      Latest Report   : {report_title} ({report_date})")

            # Key fundamentals from Eastmoney
            if fundamentals:
                lines.append("      Fundamentals:")
                key_fields = ["总市值", "市盈率(TTM)", "市净率", "ROE", "营收增速", "净利润增速"]
                for field in key_fields:
                    val = fundamentals.get(field)
                    if val and val not in ("--", "None", ""):
                        lines.append(f"        {field}: {val}")

            # Why worth buying
            lines.append("      Why Worth Buying:")
            cn_reasons = _build_reasons_cn(pick, analyst_entry)
            for r in cn_reasons:
                lines.append(f"        • {r}")

        lines.append("")
        lines.append("    " + "-" * 60)

    return lines


def _build_reasons_cn(pick, analyst_entry):
    reasons = []
    win_rate = analyst_entry.get("win_rate", 0)
    firm = analyst_entry.get("firm", "")
    name = analyst_entry.get("analyst", "")

    if win_rate >= 70:
        reasons.append(f"Recommended by {name} ({firm}) with a {win_rate:.0f}% historical accuracy rate.")
    elif win_rate >= 50:
        reasons.append(f"Analyst {name} at {firm} has a {win_rate:.0f}% win rate — above-average track record.")

    upside = pick.get("upside_pct")
    if upside and upside >= 20:
        reasons.append(f"Analyst target implies {upside:.0f}% upside from current price.")
    elif upside and upside >= 10:
        reasons.append(f"Price target offers {upside:.0f}% upside — positive risk/reward.")

    report_title = pick.get("report_title", "")
    if report_title:
        reasons.append(f"Latest research: \"{report_title}\"")

    if not reasons:
        reasons.append("Meets analyst conviction and rating criteria for this market.")
    return reasons


def _build_sector_flows_section(market_ctx):
    lines = []
    flows = market_ctx.get("sector_flows_today", [])
    flows_5d = market_ctx.get("sector_flows_5d", [])
    inflow = market_ctx.get("top_inflow_stocks", [])
    hot_em = market_ctx.get("hot_em", [])
    hot_xq = market_ctx.get("hot_xq", [])
    hk_hot = market_ctx.get("hk_hot", [])
    latest_reports = market_ctx.get("latest_reports", [])
    market_flow = market_ctx.get("market_fund_flow", {})

    lines.append("=" * 70)
    lines.append("  MARKET INTELLIGENCE  —  Fund Flows & Sentiment (CN/HK)")
    lines.append("  Sources: Eastmoney 东方财富 | Xueqiu 雪球")
    lines.append("=" * 70)

    # Overall market fund flow
    if market_flow:
        lines.append("")
        lines.append("  OVERALL A-SHARE MARKET FUND FLOW")
        for k, v in list(market_flow.items())[:6]:
            lines.append(f"    {k}: {v}")

    # Sector fund flows today
    if flows:
        lines.append("")
        lines.append("  SECTOR FUND FLOWS — TODAY (主力资金行业排行)")
        lines.append("  Shows where institutional 'main force' money is flowing in/out.")
        lines.append("")
        lines.append(f"  {'Rank':<4} {'Sector':<20} {'Net Inflow (亿)':<18} {'Net %':<10} {'Leading Stock'}")
        lines.append("  " + "-" * 65)
        for i, s in enumerate(flows[:8], 1):
            inflow_val = s.get("main_net_inflow", 0)
            sign = "▲" if inflow_val >= 0 else "▼"
            lines.append(
                f"  {i:<4} {s.get('sector',''):<20} "
                f"{sign} {abs(inflow_val):>8.2f}亿      "
                f"{_fmt_pct(s.get('main_net_pct',0)):<10} "
                f"{s.get('leading_stock','')} ({_fmt_pct(s.get('leading_pct',0))})"
            )

    # Sector fund flows 5-day trend
    if flows_5d:
        lines.append("")
        lines.append("  SECTOR FUND FLOWS — 5-DAY TREND")
        lines.append("")
        for i, s in enumerate(flows_5d[:5], 1):
            inflow_val = s.get("main_net_inflow", 0)
            sign = "▲" if inflow_val >= 0 else "▼"
            lines.append(f"  {i}. {s.get('sector',''):<20} {sign} {abs(inflow_val):.2f}亿  ({_fmt_pct(s.get('main_net_pct',0))})")

    # Top individual stock inflows
    if inflow:
        lines.append("")
        lines.append("  TOP STOCKS BY MAIN-FORCE NET INFLOW — TODAY")
        lines.append("  Stocks receiving largest institutional buying pressure right now.")
        lines.append("")
        for i, s in enumerate(inflow[:10], 1):
            inflow_val = s.get("main_net_inflow", 0)
            sign = "▲" if inflow_val >= 0 else "▼"
            lines.append(
                f"  {i:<3} [{s.get('ticker','')}] {s.get('name',''):<12} "
                f"Price: {s.get('latest_price',0):.2f}  "
                f"Chg: {_fmt_pct(s.get('change_pct',0))}  "
                f"Inflow: {sign}{abs(inflow_val):.2f}亿"
            )

    # Hot stocks — social attention
    if hot_em or hot_xq:
        lines.append("")
        lines.append("  HOT STOCKS BY RETAIL ATTENTION")
        if hot_em:
            lines.append("  Eastmoney热榜 (search popularity):")
            for s in hot_em[:8]:
                lines.append(
                    f"    #{s.get('rank','?'):<3} [{s.get('ticker','')}] {s.get('name',''):<12} "
                    f"{_fmt_pct(s.get('change_pct',0))}"
                )
        if hot_xq:
            lines.append("  Xueqiu雪球 (follower attention):")
            for s in hot_xq[:8]:
                lines.append(
                    f"    #{s.get('rank','?'):<3} [{s.get('ticker','')}] {s.get('name',''):<12} "
                    f"Followers: {s.get('follow_count','')}  {_fmt_pct(s.get('change_pct',0))}"
                )

    # Latest CN research reports
    if latest_reports:
        lines.append("")
        lines.append("  LATEST ANALYST RESEARCH REPORTS — CN A-SHARES (Eastmoney)")
        lines.append("  Most recently published buy-rated research notes:")
        lines.append("")
        for r in latest_reports[:15]:
            ticker = r.get("ticker", "")
            name = r.get("name", "")
            firm = r.get("firm", "")
            analyst = r.get("analyst", "")
            rating = r.get("rating", "")
            target = r.get("price_target", 0)
            dt = r.get("date", "")
            title = r.get("title", "")[:55]
            target_str = f"Target: ¥{target:.2f}" if target else ""
            lines.append(f"  {dt}  [{ticker}] {name:<10} | {firm} ({analyst}) | {rating} | {target_str}")
            if title:
                lines.append(f"           \"{title}\"")

    # HK hot stocks from Eastmoney
    if hk_hot:
        lines.append("")
        lines.append("  HOT HK STOCKS — Eastmoney热榜 (Hong Kong)")
        for s in hk_hot[:10]:
            lines.append(
                f"    #{s.get('rank','?'):<3} [{s.get('ticker','')}] {s.get('name',''):<14} "
                f"Price: {s.get('latest_price',0):.2f}  Chg: {_fmt_pct(s.get('change_pct',0))}"
            )

    return lines


def _build_finviz_summary(us_stocks):
    """Append a Finviz upgrade summary to the US section header."""
    lines = []
    all_upgrades = []
    for s in us_stocks:
        for u in s.get("finviz_upgrades", []):
            all_upgrades.append(f"  {u.get('date','?')}  [{s.get('ticker','')}]  "
                                f"{u.get('brokerage','?')}  →  {u.get('rating_change','?')}  "
                                f"PT: {u.get('price_target','N/A')}")
    if all_upgrades:
        lines.append("")
        lines.append("  FINVIZ ANALYST UPGRADES (last 14 days — bullish actions only)")
        lines.extend(all_upgrades[:20])
    return lines


def generate_report(us_stocks, hk_stocks, cn_data, market_ctx=None, report_date=None):
    """Assemble and return the full plain-text report as a string."""
    today = report_date or date.today().isoformat()
    market_ctx = market_ctx or {}
    lines = []

    lines.append("")
    lines.append("*" * 70)
    lines.append(f"  DAILY ANALYST STOCK REPORT  —  {today}")
    lines.append("*" * 70)
    lines.append("")
    lines.append("  FREE DATA SOURCES USED:")
    lines.append("    • Yahoo Finance (yfinance)     — US & HK price/consensus/targets")
    lines.append("    • Eastmoney 东方财富 (akshare)  — CN analyst rankings, fund flows,")
    lines.append("                                      research reports, HK coverage")
    lines.append("    • Xueqiu 雪球 (akshare)         — CN/HK social sentiment & hot stocks")
    lines.append("    • Finviz                        — US analyst upgrade/downgrade feed")
    lines.append("    • FMP (if key set)              — US/HK analyst grades & estimates")
    lines.append("    • CNINF 巨潮资讯 (akshare)      — Official SSE/SZSE company filings")
    lines.append("")
    lines.append("  DISCLAIMER: Auto-generated from public data. Not investment advice.")
    lines.append("")

    # Market Intelligence — Sector Flows & Sentiment
    if market_ctx:
        lines += _build_sector_flows_section(market_ctx)
        lines.append("")

    # US Section
    lines += _build_us_hk_section("US EQUITIES  —  Top Analyst Picks (NYSE / NASDAQ)", us_stocks, currency="$")
    if us_stocks:
        lines += _build_finviz_summary(us_stocks)
    lines.append("")

    # HK Section
    lines += _build_us_hk_section("HONG KONG  —  Top Analyst Picks (HKEX)", hk_stocks, currency="HK$")
    lines.append("")

    # CN Section
    lines += _build_cn_section(cn_data)

    lines.append("")
    lines.append("=" * 70)
    lines.append(f"  END OF REPORT  —  Generated {today}")
    lines.append("=" * 70)
    lines.append("")

    return "\n".join(lines)


def save_report(report_text, report_date=None):
    """Save report to reports/<date>.txt and return the file path."""
    os.makedirs(REPORTS_DIR, exist_ok=True)
    today = report_date or date.today().isoformat()
    path = os.path.join(REPORTS_DIR, f"{today}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(report_text)
    return path
