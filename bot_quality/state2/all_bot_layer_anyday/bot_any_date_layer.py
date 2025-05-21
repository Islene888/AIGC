import pandas as pd
import pymysql
import urllib.parse
from sqlalchemy import create_engine
from typing import Dict
import matplotlib.pyplot as plt

# ========== Step 1: Connect to Database ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

def get_connection():
    # 用于 pandas.read_sql with pymysql
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return pymysql.connect(
        host="3.135.224.186", port=9030,
        user="flowgptzmy", password=password,
        db="flow_ab_test", charset="utf8mb4"
    )

# ========== Step 2: Sample Bots (unchanged) ==========
def get_stratified_bot_sample(event_date: str, per_range_sample=100) -> pd.DataFrame:
    engine = get_engine()
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
    df = pd.read_sql(query, engine)
    df["percentile"] = df["event_cnt"].rank(pct=True) * 100

    layers = [(i, i+10) for i in range(0, 100, 10)]
    sampled_df = []
    for low, high in layers:
        segment = df[(df["percentile"] >= low) & (df["percentile"] < high)]
        sampled = segment.sample(n=min(per_range_sample, len(segment)), random_state=42)
        sampled["layer"] = f"{low}-{high}%"
        sampled_df.append(sampled)

    return pd.concat(sampled_df, ignore_index=True)

# ========== Step 3: Analyze a Single Bot ==========
def analyze_bot_indicators(bot_id: str, cohort_date: str) -> pd.DataFrame:
    engine = get_engine()
    # --- A. 基础指标：click_rate, chat_start_rate, avg_chat_rounds, avg_chat_rounds_user ---
    sqls = {
        'click_rate': f"""
            WITH view_users AS (
              SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_bot_view
              WHERE event_date = '{cohort_date}' AND bot_id = '{bot_id}'
            ),
            show_users AS (
              SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_show_prompt_card
              WHERE event_date = '{cohort_date}' AND prompt_id = '{bot_id}'
            )
            SELECT
              COUNT(v.user_id) AS click_user_cnt,
              COUNT(s.user_id) AS show_user_cnt,
              ROUND(COUNT(v.user_id) / NULLIF(COUNT(s.user_id), 0), 4) AS click_rate
            FROM view_users v LEFT JOIN show_users s ON v.user_id = s.user_id;
        """,
        'chat_start_rate': f"""
            WITH chat_users AS (
              SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_chat_send
              WHERE event_date = '{cohort_date}' AND prompt_id = '{bot_id}'
            ),
            click_users AS (
              SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_bot_view
              WHERE event_date = '{cohort_date}' AND bot_id = '{bot_id}'
            )
            SELECT
              COUNT(c.user_id) AS chat_user_cnt,
              COUNT(v.user_id) AS click_user_cnt,
              ROUND(COUNT(c.user_id) / NULLIF(COUNT(v.user_id), 0), 4) AS chat_start_rate
            FROM chat_users c LEFT JOIN click_users v ON c.user_id = v.user_id;
        """,
        'avg_chat_rounds': f"""
            SELECT
              COUNT(*) AS total_chat_rounds,
              COUNT(DISTINCT user_id) AS chat_user_cnt,
              ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds
            FROM flow_event_info.tbl_app_event_chat_send
            WHERE event_date = '{cohort_date}' AND prompt_id = '{bot_id}';
        """,
        'avg_chat_rounds_user': f"""
            SELECT
              COUNT(*) AS total_chat_rounds,
              COUNT(DISTINCT user_id) AS total_user_cnt,
              ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds_per_user
            FROM flow_event_info.tbl_app_event_chat_send
            WHERE prompt_id = '{bot_id}'
              AND event_date BETWEEN '{cohort_date}'
                               AND DATE_ADD('{cohort_date}', INTERVAL 15 DAY);
        """
    }
    results = {}
    # 执行基础 SQL 指标
    for name, sql in sqls.items():
        try:
            df = pd.read_sql(sql, engine)
            if not df.empty:
                for col in df.columns:
                    results[f"{name}_{col}"] = df.iloc[0][col]
        except Exception as e:
            results[f"{name}_error"] = str(e)

    # --- B. Python 计算 1–15 天留存 & sticky_score ---
    conn = get_connection()
    try:
        events = pd.read_sql(
            f"""
            SELECT DISTINCT user_id, event_date
            FROM flow_event_info.tbl_app_event_chat_send
            WHERE prompt_id = '{bot_id}'
              AND event_date BETWEEN '{cohort_date}'
                               AND DATE_ADD('{cohort_date}', INTERVAL 15 DAY)
            """,
            conn,
            parse_dates=['event_date']
        )
    finally:
        conn.close()

    if events.empty:
        return pd.DataFrame([results])
    # 首次聊天
    fc = (
        events.groupby('user_id')['event_date']
        .min()
        .reset_index(name='first_chat_date')
    )
    # 仅保留 cohort_date 首聊用户
    new_users = fc[fc['first_chat_date'] == pd.to_datetime(cohort_date)]['user_id']
    total_new = len(new_users)
    if total_new == 0:
        return pd.DataFrame([results])

    # 逐日留存
    for n in range(1, 16):
        day = pd.to_datetime(cohort_date) + pd.Timedelta(days=n)
        retained = (
            events[(events['user_id'].isin(new_users)) &
                   (events['event_date'] == day)]['user_id']
            .nunique()
        )
        results[f'd{n}_retention'] = round(retained / total_new, 4)

    # 计算 avg_active_days
    active_days = (
        events[events['user_id'].isin(new_users)]
        .groupby('user_id')['event_date']
        .nunique()
    )
    avg_act = round(active_days.mean(), 2)
    results['avg_active_days'] = avg_act

    # 计算加权 sticky_score
    weights = {1: 70, 3: 5, 7: 5, 14: 5}
    score = (
          weights[1] * results.get('d1_retention', 0)
        + weights[3] * results.get('d3_retention', 0)
        + weights[7] * results.get('d7_retention', 0)
        + weights[14] * results.get('d14_retention', 0)
        + 15 * (avg_act / 15)
    )
    results['sticky_score'] = round(score, 2)

    results['prompt_id'] = bot_id
    return pd.DataFrame([results])

# ========== Step 4: 主程序示例 ==========
if __name__ == '__main__':
    cohort = '2025-04-01'
    bots = get_stratified_bot_sample(cohort, per_range_sample=10)
    all_res = []
    for _, row in bots.iterrows():
        bot_id = row['prompt_id']
        layer  = row['layer']
        df = analyze_bot_indicators(bot_id, cohort)
        df['layer'] = layer
        all_res.append(df)
    result_df = pd.concat(all_res, ignore_index=True)
    print(result_df.head())
