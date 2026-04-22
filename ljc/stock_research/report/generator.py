"""HTML report generator using Jinja2."""

import os
import json
import pandas as pd
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
from config import OUTPUT_DIR, TOP_STOCKS_FINAL

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")

MARKET_NAMES = {"CN": "A股", "HK": "港股", "US": "美股"}


class ReportGenerator:
    def __init__(self):
        self.env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))

    def _df_to_records(self, df: pd.DataFrame, cols: list) -> list:
        if df.empty:
            return []
        cols = [c for c in cols if c in df.columns]
        subset = df[cols].fillna(0)
        for col in subset.select_dtypes(include="number").columns:
            subset[col] = subset[col].round(2)
        return subset.to_dict("records")

    def build(
        self,
        cn_analysts: pd.DataFrame,
        hk_analysts: pd.DataFrame,
        us_analysts: pd.DataFrame,
        top_stocks: pd.DataFrame,
        analyst_summary: str = "",
        portfolio_summary: str = "",
        stock_theses: dict = None,
    ) -> str:
        report_date = datetime.now().strftime("%Y-%m-%d %H:%M")
        total_analysts = len(cn_analysts) + len(hk_analysts) + len(us_analysts)

        analyst_cols = ["analyst_name", "broker", "win_rate", "excess_return"]
        analyst_tables = {
            "CN": self._df_to_records(cn_analysts, analyst_cols),
            "HK": self._df_to_records(hk_analysts, analyst_cols),
            "US": self._df_to_records(us_analysts, analyst_cols),
        }

        # Build top-stocks list for the template
        stock_records = []
        for i, (_, row) in enumerate(top_stocks.iterrows(), start=1):
            rec = row.to_dict()
            rec["market_label"] = rec.get("market", rec.get("market_label", "US"))
            rec.setdefault("global_score", rec.get("composite_score", 0))
            # Attach thesis if available
            ticker = rec.get("ticker", "")
            if stock_theses and ticker in stock_theses:
                rec["thesis"] = stock_theses[ticker]
            else:
                rec["thesis"] = ""
            stock_records.append(rec)

        template = self.env.get_template("report.html")
        html = template.render(
            report_date=report_date,
            total_analysts=total_analysts,
            top_n=TOP_STOCKS_FINAL,
            analyst_tables=analyst_tables,
            market_names=MARKET_NAMES,
            analyst_summary=analyst_summary,
            top_stocks=stock_records,
            portfolio_summary=portfolio_summary,
        )

        out_path = os.path.join(
            OUTPUT_DIR,
            f"stock_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
        )
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)

        # Also save raw data as JSON
        json_path = out_path.replace(".html", "_data.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "report_date": report_date,
                    "top_stocks": [
                        {k: (v if not isinstance(v, float) or v == v else None)
                         for k, v in s.items()}
                        for s in stock_records
                    ],
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

        return out_path
