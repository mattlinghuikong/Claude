"""Use Claude API to generate investment thesis for each top stock."""

import json
import pandas as pd
import anthropic
from config import ANTHROPIC_API_KEY, ANTHROPIC_MODEL


SYSTEM_PROMPT = """你是一位资深的股票研究分析师，擅长综合多位顶级分析师的观点，
为投资者生成深度研究报告。你的分析需要客观、专业，涵盖基本面、催化剂和风险因素。
请用中文回答，报告需条理清晰、逻辑严谨。"""


class ClaudeAnalyzer:
    def __init__(self):
        if not ANTHROPIC_API_KEY:
            raise ValueError(
                "ANTHROPIC_API_KEY not set. "
                "Please set it in .env or environment variables."
            )
        self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    def analyze_top_analysts(
        self,
        cn_analysts: pd.DataFrame,
        hk_analysts: pd.DataFrame,
        us_analysts: pd.DataFrame,
    ) -> str:
        """Generate a summary of the analyst landscape across markets."""
        summary = {}
        for label, df in [("A股", cn_analysts), ("港股", hk_analysts), ("美股", us_analysts)]:
            if df.empty:
                summary[label] = []
                continue
            cols = ["analyst_name", "broker", "win_rate", "excess_return"]
            cols = [c for c in cols if c in df.columns]
            summary[label] = df[cols].head(5).to_dict("records")

        prompt = f"""以下是三大市场胜率最高的分析师概况（仅展示各市场前5位）：

{json.dumps(summary, ensure_ascii=False, indent=2)}

请基于以上数据，撰写一段200字左右的"分析师市场概况"，说明：
1. 各市场顶级分析师的胜率水平和超额收益特征
2. 各市场最具代表性的券商/投行
3. 对本次选股方法论的简要说明"""

        resp = self.client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=600,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text

    def generate_stock_thesis(self, stock_row: pd.Series, rank: int) -> dict:
        """Generate investment thesis for a single stock."""
        stock_info = stock_row.to_dict()
        market_name = {"CN": "A股", "HK": "港股", "US": "美股"}.get(
            stock_info.get("market", stock_info.get("market_label", "")), "未知市场"
        )

        prompt = f"""请为以下股票生成一份专业的投资研究摘要：

股票信息：
- 代码：{stock_info.get('ticker', 'N/A')}
- 名称：{stock_info.get('name', 'N/A')}
- 市场：{market_name}
- 所属行业：{stock_info.get('sector', '未知')}
- 覆盖分析师数：{stock_info.get('analyst_count', 'N/A')}
- 分析师平均胜率：{stock_info.get('analyst_win_rate', 'N/A'):.1f}%
- 分析师平均超额收益：{stock_info.get('analyst_excess_return', 'N/A'):.1f}%
- 目标价：{stock_info.get('target_price', 'N/A')}
- 综合评分（0-1）：{stock_info.get('global_score', stock_info.get('composite_score', 'N/A')):.3f}
- 本次推荐排名：第{rank}位

请按以下结构输出（每部分2-3句话）：

**投资逻辑**：[核心买入理由]

**核心催化剂**：[近期可能推动股价上涨的事件]

**主要风险**：[需关注的下行风险]

**评级建议**：[买入/增持，以及目标价合理性分析]"""

        resp = self.client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=500,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return {
            "ticker": stock_info.get("ticker", ""),
            "thesis": resp.content[0].text,
        }

    def generate_portfolio_summary(self, top_stocks: pd.DataFrame) -> str:
        """Generate overall portfolio commentary."""
        stocks_list = []
        for _, row in top_stocks.iterrows():
            market = row.get("market", row.get("market_label", ""))
            stocks_list.append(
                f"- {row.get('name','?')} ({row.get('ticker','?')}) "
                f"[{market}] 行业:{row.get('sector','?')}"
            )

        prompt = f"""以下10只股票是经过A股、港股、美股三大市场胜率最高分析师筛选后的最优推荐组合：

{chr(10).join(stocks_list)}

请生成一段300字左右的"组合投资逻辑总结"，包括：
1. 组合的行业分布和地域分布特征
2. 当前宏观环境下该组合的合理性
3. 组合的风险收益特征分析
4. 适合什么类型的投资者"""

        resp = self.client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=700,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text
