import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib.parse
import matplotlib.pyplot as plt
from typing import List, Tuple, Dict
from datetime import datetime, timedelta
import argparse
import json

# ========== 配置加载 ==========
def load_config(path: str = None) -> dict:
    default_config = {
        "date_range": ("2025-04-01", "2025-04-03"),
        "per_layer_sample": 30,
        "layers": [(0, 10), (10, 30), (30, 70), (70, 90), (90, 100)],
        "indicators": [
            "click_rate", "chat_start_rate", "avg_chat_rounds",
            "sticky_score", "avg_chat_rounds_user", "avg_retention_days"
        ],
        "weights": {
            "click_rate_click_rate": 0.1,
            "chat_start_rate_chat_start_rate": 0.1,
            "avg_chat_rounds_avg_chat_rounds": 0.2,
            "sticky_score_sticky_score": 0.2,
            "avg_chat_rounds_user_avg_chat_rounds_per_user": 0.2,
            "avg_retention_days_avg_retention_days": 0.2
        },
        "output": {
            "csv_market": "market_bot_scores.csv",
            "csv_ai_template": "ai_bot_scores_{date}.csv",
            "plot_market": "market_layer_score_trend.png",
            "enable_plot": True
        }
    }
    if path:
        with open(path, 'r', encoding='utf-8') as f:
            user_config = json.load(f)
        default_config.update(user_config)
    return default_config

# ========== 数据库连接 ==========
def get_engine() -> create_engine:
    pwd = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{pwd}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# ========== 分层采样 ==========
def get_stratified_bot_sample(event_date: str, layers: List[Tuple[int, int]], per_sample: int) -> pd.DataFrame:
    q = f"""
    SELECT prompt_id, COUNT(DISTINCT user_id) AS chat_users,
           COUNT(*) AS total_chat, COUNT(DISTINCT event_id) AS event_cnt
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{event_date}'
    GROUP BY prompt_id
    HAVING chat_users >= 3 OR event_cnt >= 5
    """
    df = pd.read_sql(q, get_engine())
    df["percentile"] = df["event_cnt"].rank(pct=True) * 100
    sampled = []
    for low, high in layers:
        seg = df[(df["percentile"] >= low) & (df["percentile"] < high)]
        s = seg.sample(n=min(per_sample, len(seg)), random_state=42)
        s["layer"] = f"{low}-{high}%"
        sampled.append(s)
    return pd.concat(sampled, ignore_index=True)

# ========== AI Bot 列表获取 ==========
def get_ai_bot_ids(event_date: str, top_n: int = 100) -> List[str]:
    q = f"""
    SELECT prompt_id FROM tbl_top50_sora_bots_daily
    WHERE event_date = '{event_date}' ORDER BY event_cnt DESC LIMIT {top_n}
    """
    df = pd.read_sql(q, get_engine())
    return df['prompt_id'].tolist()

# ========== 指标 SQL 生成器 ==========
def generate_bot_quality_sql(bot_id: str, date: str,
                             sticky_start: str, sticky_end: str,
                             indicators: List[str]) -> Dict[str, str]:
    sql = {}
    # click_rate, chat_start_rate, avg_chat_rounds, sticky_score,...
    # （此处保留原 implementation）
    return sql

# ========== SQL 执行与指标分析 ==========
def run_sql(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def analyze_bot_indicators(bot_id: str, date: str,
                            sticky_start: str, sticky_end: str,
                            indicators: List[str]) -> pd.DataFrame:
    engine = get_engine()
    sqls = generate_bot_quality_sql(bot_id, date, sticky_start, sticky_end, indicators)
    res = {}
    for name, s in sqls.items():
        try:
            df = run_sql(engine, s)
            if not df.empty:
                for c in df.columns:
                    res[f"{name}_{c}"] = df.iloc[0][c]
        except Exception as e:
            res[f"{name}_error"] = str(e)
    res["prompt_id"] = bot_id
    return pd.DataFrame([res])

# ========== 归一化与打分（市场 Bot） ==========
def normalize_and_score_market(df: pd.DataFrame, weights: Dict[str, float]) -> pd.DataFrame:
    def min_max(s: pd.Series):
        return (s - s.min()) / (s.max() - s.min()) if s.nunique() > 1 else pd.Series([0.5]*len(s))
    for m in weights:
        if m in df.columns:
            df[f"{m}_norm"] = min_max(df[m])
    df["final_score"] = sum(df.get(f"{m}_norm",0)*w for m,w in weights.items())
    df["final_score"] = df["final_score"].fillna(0).clip(lower=0.01)
    return df

# ========== AI Bot 打分（基于市场 MinMax） ==========
def score_ai_bots(ai_df: pd.DataFrame,
                  market_min_max: Dict[str, Tuple[float, float]],
                  weights: Dict[str, float]) -> pd.DataFrame:
    for m, (mn, mx) in market_min_max.items():
        if m in ai_df.columns:
            if mx>mn:
                ai_df[f"{m}_norm"] = (ai_df[m] - mn)/(mx-mn)
            else:
                ai_df[f"{m}_norm"] = 0.5
    ai_df["final_score"] = sum(ai_df.get(f"{m}_norm",0)*w for m,w in weights.items())
    return ai_df

# ========== 主执行 ==========
def run_bot_analysis(config: dict):
    all_market = []
    all_results = []
    layers = config["layers"]
    weights = config["weights"]
    for d in pd.date_range(*config["date_range"]):
        date = d.strftime("%Y-%m-%d")
        start = (d - timedelta(days=14)).strftime("%Y-%m-%d")
        end = (d + timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Processing {date}")
        market_df = get_stratified_bot_sample(date, layers, config["per_layer_sample"])
        m_res = pd.concat([
            analyze_bot_indicators(row.prompt_id, date, start, end, config["indicators"])
            for _,row in market_df.iterrows()
        ], ignore_index=True)
        m_scored = normalize_and_score_market(m_res, weights)
        m_scored["layer"] = market_df["layer"].values
        m_scored.to_csv(config["output"]["csv_market"], index=False)

        # 计算市场分层均值与 MinMax
        layer_avg = m_scored.groupby("layer")["final_score"].mean().to_dict()
        min_max_map = {m:(m_scored[m].min(), m_scored[m].max()) for m in weights}

        # AI Bot 分析
        ai_ids = get_ai_bot_ids(date)
        ai_res = pd.concat([
            analyze_bot_indicators(b, date, start, end, config["indicators"])
            for b in ai_ids
        ], ignore_index=True)
        ai_scored = score_ai_bots(ai_res, min_max_map, weights)
        # 匹配最接近的分层
        ai_scored["matched_layer"] = ai_scored["final_score"].apply(
            lambda x: min(layer_avg.items(), key=lambda t: abs(t[1]-x))[0]
        )
        ai_scored.to_csv(config["output"]["csv_ai_template"].format(date=date), index=False)

        all_market.append(m_scored)
        all_results.append(ai_scored)

    # 绘制市场分层趋势
    summary = pd.concat(all_market)
    if config["output"]["enable_plot"]:
        plt.figure(figsize=(10,6))
        for lyr, grp in summary.groupby("layer"):
            plt.plot(grp["event_date"], grp["final_score"], marker='o', label=lyr)
        plt.legend(title="Layer")
        plt.tight_layout()
        plt.savefig(config["output"]["plot_market"])
        plt.show()

# ========== CLI ==========
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument('--config', type=str)
    args = p.parse_args()
    cfg = load_config(args.config)
    run_bot_analysis(cfg)
