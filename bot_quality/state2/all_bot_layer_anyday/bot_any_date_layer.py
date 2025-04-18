import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from typing import Dict
import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np


# ========== Step 1: Connect to Database ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# ========== Step 1.5: 获取前 N 个符合条件的 bot（含分层） ==========
def get_stratified_bot_sample(event_date: str, ranges=[(99, 100), (95, 99), (90, 95), (70, 90), (30, 70), (10, 30), (0, 10)], per_range_sample=50) -> pd.DataFrame:
    query = f"""
    SELECT prompt_id,
           COUNT(DISTINCT user_id) AS chat_users,
           COUNT(*) AS total_chat,
           COUNT(DISTINCT event_id) AS event_cnt
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{event_date}'
    GROUP BY prompt_id
    HAVING chat_users >= 3 OR event_cnt >= 5
    """
    df = pd.read_sql(query, get_engine())
    df["percentile"] = df["event_cnt"].rank(pct=True) * 100

    sampled_df = []
    for low, high in ranges:
        segment = df[(df["percentile"] >= low) & (df["percentile"] < high)]
        sampled = segment.sample(n=min(per_range_sample, len(segment)), random_state=42)
        sampled["layer"] = f"{low}-{high}%"
        sampled_df.append(sampled)

    return pd.concat(sampled_df, ignore_index=True)

# ========== Step 2: 生成 SQL 查询 ==========
def generate_bot_quality_sql(bot_id: str, date='2025-03-15', sticky_start='2025-02-01', sticky_end='2025-02-15') -> Dict[str, str]:
    sql = {}

    sql['click_rate'] = f"""
    WITH view_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_bot_view
      WHERE event_date = '{date}' AND bot_id = '{bot_id}'
    ),
    show_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_show_prompt_card
      WHERE event_date = '{date}' AND prompt_id = '{bot_id}'
    )
    SELECT COUNT(v.user_id) AS click_user_cnt,
           COUNT(s.user_id) AS show_user_cnt,
           ROUND(COUNT(v.user_id) / NULLIF(COUNT(s.user_id), 0), 4) AS click_rate
    FROM view_users v FULL OUTER JOIN show_users s ON v.user_id = s.user_id;
    """

    sql['chat_start_rate'] = f"""
    WITH chat_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_chat_send
      WHERE event_date = '{date}' AND prompt_id = '{bot_id}'
    ),
    click_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_bot_view
      WHERE event_date = '{date}' AND bot_id = '{bot_id}'
    )
    SELECT COUNT(c.user_id) AS chat_user_cnt,
           COUNT(v.user_id) AS click_user_cnt,
           ROUND(COUNT(c.user_id) / NULLIF(COUNT(v.user_id), 0), 4) AS chat_start_rate
    FROM chat_users c FULL OUTER JOIN click_users v ON c.user_id = v.user_id;
    """

    sql['avg_chat_rounds'] = f"""
    SELECT COUNT(*) AS total_chat_rounds,
           COUNT(DISTINCT user_id) AS chat_user_cnt,
           ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{date}' AND prompt_id = '{bot_id}';
    """

    sql['sticky_score'] = f"""
    WITH base_chat AS (
      SELECT user_id, event_date
      FROM flow_event_info.tbl_app_event_chat_send
      WHERE prompt_id = '{bot_id}' AND event_date >= '{sticky_start}' AND event_date < '{sticky_end}'
    ),
    first_chat AS (
      SELECT user_id, MIN(event_date) AS first_chat_date
      FROM base_chat GROUP BY user_id
    ),
    chat_with_gap AS (
      SELECT b.user_id, DATEDIFF(b.event_date, f.first_chat_date) AS day_gap
      FROM base_chat b JOIN first_chat f ON b.user_id = f.user_id
      WHERE b.event_date > f.first_chat_date AND b.event_date <= DATE_ADD(f.first_chat_date, INTERVAL 15 DAY)
    )
    SELECT COUNT(DISTINCT user_id) AS total_return_users,
           SUM(16 - day_gap) AS sticky_score
    FROM chat_with_gap
    WHERE day_gap BETWEEN 1 AND 15;
    """

    sql['avg_chat_rounds_user'] = f"""
    SELECT COUNT(*) AS total_chat_rounds,
           COUNT(DISTINCT user_id) AS total_user_cnt,
           ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds_per_user
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE prompt_id = '{bot_id}' AND event_date >= '{sticky_start}' AND event_date < '{sticky_end}';
    """

    sql['avg_retention_days'] = f"""
    WITH d0_users AS (
      SELECT user_id, MIN(event_date) AS first_date
      FROM flow_event_info.tbl_app_event_bot_view
      WHERE bot_id = '{bot_id}' AND event_date >= '{sticky_start}' AND event_date < DATE_ADD('{sticky_start}', INTERVAL 4 DAY)
      GROUP BY user_id
    ),
    future_activity AS (
      SELECT d.user_id, DATEDIFF(v.event_date, d.first_date) AS day_gap
      FROM d0_users d
      JOIN flow_event_info.tbl_app_event_bot_view v
        ON d.user_id = v.user_id AND v.bot_id = '{bot_id}'
      WHERE v.event_date > d.first_date AND v.event_date <= DATE_ADD(d.first_date, INTERVAL 14 DAY)
    ),
    user_retention_days AS (
      SELECT user_id, COUNT(DISTINCT day_gap) AS retained_days
      FROM future_activity
      GROUP BY user_id
    )
    SELECT COUNT(*) AS user_cnt,
           ROUND(AVG(retained_days), 2) AS avg_retention_days
    FROM user_retention_days;
    """

    return sql

# ========== Step 3: 执行分析 ==========
def run_sql(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

def analyze_bot_indicators(bot_id: str, date: str, sticky_start: str, sticky_end: str) -> pd.DataFrame:
    engine = get_engine()
    sqls = generate_bot_quality_sql(bot_id, date, sticky_start, sticky_end)
    results = {}

    for name, sql in sqls.items():
        try:
            df = run_sql(engine, sql)
            if not df.empty:
                for col in df.columns:
                    results[f"{name}_{col}"] = df.iloc[0][col]
        except Exception as e:
            results[f"{name}_error"] = str(e)

    results["prompt_id"] = bot_id
    return pd.DataFrame([results])

# ========== Step 4: 归一化 + 评分 ==========
def normalize_and_score(df: pd.DataFrame, weights: dict = None) -> pd.DataFrame:
    if weights is None:
        weights = {
            "click_rate_click_rate": 0.1,
            "chat_start_rate_chat_start_rate": 0.1,
            "avg_chat_rounds_avg_chat_rounds": 0.2,
            "sticky_score_sticky_score": 0.2,
            "avg_chat_rounds_user_avg_chat_rounds_per_user": 0.2,
            "avg_retention_days_avg_retention_days": 0.2
        }

    def min_max(series):
        return (series - series.min()) / (series.max() - series.min()) if series.nunique() > 1 else pd.Series([0.5] * len(series))

    for metric in weights:
        if metric in df.columns:
            df[f"{metric}_norm"] = min_max(df[metric])

    df["final_score"] = sum(df[f"{m}_norm"] * w for m, w in weights.items() if f"{m}_norm" in df.columns)
    df["final_score"] = df["final_score"].fillna(0).clip(lower=0.01)
    df["rank"] = df["final_score"].rank(ascending=False)
    return df

# ========== Step 5: 主程序入口 ==========
if __name__ == "__main__":
    all_score_by_layer = []
    date_range = pd.date_range("2025-04-13", "2025-04-17")  # 按需改动时间范围
    per_range_sample = 10

    for target_date in date_range:
        target_str = target_date.strftime("%Y-%m-%d")
        sticky_start = (target_date - pd.Timedelta(days=14)).strftime("%Y-%m-%d")
        sticky_end = (target_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"\n📅 正在处理日期：{target_str}")
        print(f"👉 粘性统计范围：{sticky_start} ~ {sticky_end}")

        bot_df = get_stratified_bot_sample(target_str, per_range_sample=per_range_sample)
        print(f"🔎 获取到 Bot 数量：{len(bot_df)}")
        print(bot_df[["prompt_id", "layer"]].head())

        all_results = []

        for i, row in bot_df.iterrows():
            bot_id = row["prompt_id"]
            layer = row["layer"]
            print(f"  ➤ ({i+1}/{len(bot_df)}) 分析 Bot: {bot_id} | 分层: {layer}")
            try:
                df = analyze_bot_indicators(bot_id, target_str, sticky_start, sticky_end)
                df["layer"] = layer
                all_results.append(df)
            except Exception as e:
                print(f"❌ 分析失败: {e}")

        if not all_results:
            print(f"⚠️ 日期 {target_str} 没有可用分析结果，跳过")
            continue

        result_df = pd.concat(all_results, ignore_index=True)
        scored_df = normalize_and_score(result_df)
        print("✅ 已完成归一化与评分")

        # 输出分层平均得分
        score_by_layer = scored_df.groupby("layer")["final_score"].mean().reset_index()
        score_by_layer["date"] = target_str
        print(f"📊 当天各分层平均得分：\n{score_by_layer}")
        all_score_by_layer.append(score_by_layer)

    # 合并所有天的结果
    if all_score_by_layer:
        summary_df = pd.concat(all_score_by_layer, ignore_index=True)
        summary_df = summary_df[["date", "layer", "final_score"]]
        summary_df.to_csv("bot_layer_score_trend.csv", index=False)
        print("\n📁 已保存 CSV：bot_layer_score_trend.csv")

        # 折线图绘制
        plt.figure(figsize=(10, 6))
        for layer, group in summary_df.groupby("layer"):
            plt.plot(group["date"], group["final_score"], label=layer, marker='o')

        plt.title("各分层每日平均得分变化趋势")
        plt.xlabel("日期")
        plt.ylabel("平均得分")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.legend(title="分层")
        plt.tight_layout()
        plt.savefig("bot_layer_score_trend.png")
        plt.show()
        print("🖼️ 折线图已保存：bot_layer_score_trend.png")
    else:
        print("❌ 未生成任何得分数据，任务失败")
