import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from typing import Dict
import re
from datetime import datetime

# ========== Step 1: Connect to Database ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    engine = create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )
    return engine

# ========== Step 2: SQL Generator ==========
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

# ========== Step 3: 执行分析并整合结果 ==========
def run_sql(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

def is_valid_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def is_safe_identifier(text: str) -> bool:
    return bool(re.match(r'^[a-zA-Z0-9_\-]+$', text))

def analyze_bot_indicators(bot_id: str, date='2025-02-15', sticky_start='2025-02-01', sticky_end='2025-02-15') -> pd.DataFrame:
    print(f"\n🔍 正在分析 bot: {bot_id}...")

    for label, val in {'date': date, 'sticky_start': sticky_start, 'sticky_end': sticky_end}.items():
        if not is_valid_date(val):
            raise ValueError(f"❌ 参数错误: {label} 格式应为 YYYY-MM-DD，但收到: {val}")
    if not is_safe_identifier(bot_id):
        raise ValueError(f"❌ bot_id 包含非法字符: {bot_id}")

    engine = get_engine()
    sqls = generate_bot_quality_sql(bot_id, date, sticky_start, sticky_end)
    results = {}

    for name, sql in sqls.items():
        try:
            print(f"➡️ 运行指标 [{name}] 的 SQL...")
            df = run_sql(engine, sql)
            print(f"✅ {name} 查询成功，结果行数: {len(df)}")
            if not df.empty:
                for col in df.columns:
                    results[f"{name}_{col}"] = df.iloc[0][col]
            else:
                print(f"⚠️ {name} 查询返回空结果。")
        except Exception as e:
            print(f"❌ {name} 查询失败: {e}")
            results[f"{name}_error"] = str(e)

    results['prompt_id'] = bot_id
    return pd.DataFrame([results])

# ========== Step 4: 归一化并融合评分 ==========
def normalize_and_score(df: pd.DataFrame, weights: dict = None) -> pd.DataFrame:
    if weights is None:
        weights = {
            "click_rate_click_rate": 0.2,
            "chat_start_rate_chat_start_rate": 0.2,
            "avg_chat_rounds_avg_chat_rounds": 0.2,
            "sticky_score_sticky_score": 0.2,
            "avg_chat_rounds_user_avg_chat_rounds_per_user": 0.1,
            "avg_retention_days_avg_retention_days": 0.1
        }

    def min_max(series):
        if series.nunique() <= 1:
            return pd.Series([0.5] * len(series), index=series.index)
        return (series - series.min()) / (series.max() - series.min())

    print("\n📊 开始归一化处理并计算加权得分...")
    for metric in weights:
        if metric in df.columns:
            df[f"{metric}_norm"] = min_max(df[metric])

    df["final_score"] = sum(
        df[f"{metric}_norm"] * weight
        for metric, weight in weights.items()
        if f"{metric}_norm" in df.columns
    )

    df["final_score"] = df["final_score"].fillna(0).clip(lower=0.01)
    df["rank"] = df["final_score"].rank(ascending=False, method="min")
    print("✅ 得分计算完成。\n")
    return df

# ========== Step 5: 单个 bot 质量分析 ==========
if __name__ == "__main__":
    # ✅ 你要分析的 bot 列表
    bot_list = [
        "60LJPpsh-Y1xrEm8HLf11",
        "TK6prskkeMBwNzF5o1WLr",
        "6GaY6yFp35TpMVXCEZZVV",
        "YNPHLWWXZ38amS3NhZHxr",
        "LLQVXSOBx0G0qXg_uP_XO",
        "Sd4Yl-TJp6SNIplWMZapH",
        "4CXDeRD-u9U72xC6c6WRz"
    ]

    # ✅ 设置分析日期
    target_date = "2025-04-15"
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    sticky_start = (target_dt - pd.Timedelta(days=14)).strftime("%Y-%m-%d")
    sticky_end = (target_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    output_prefix = "custom_bot_batch"  # 输出前缀

    all_results = []

    for bot_id in bot_list:
        try:
            df = analyze_bot_indicators(bot_id, date=target_date, sticky_start=sticky_start, sticky_end=sticky_end)
            all_results.append(df)
        except Exception as e:
            print(f"❌ 分析 bot {bot_id} 失败: {e}")

    if all_results:
        result_df = pd.concat(all_results, ignore_index=True)
        scored_df = normalize_and_score(result_df)
        scored_df.to_csv(f"{output_prefix}_quality.csv", index=False)

        print("\n📊 批量分析结果（排名+得分）：")
        print(scored_df[["prompt_id", "final_score", "rank"]])
    else:
        print("⚠️ 所有分析都失败，没有生成评分结果。")
