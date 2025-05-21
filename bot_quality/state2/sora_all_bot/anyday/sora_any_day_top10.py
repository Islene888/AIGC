import pandas as pd
from matplotlib import pyplot as plt
from sqlalchemy import create_engine
import urllib.parse
from typing import Dict
import re
from datetime import datetime, timedelta
from tqdm import tqdm

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'Microsoft YaHei', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False

# ========== 数据库连接 ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    engine = create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4",
        connect_args={"connect_timeout": 3000}
    )
    return engine

# ========== 获取活跃 bot ==========
def get_top_bot_ids_by_type(event_date: str, top_n: int = 10) -> Dict[str, list]:
    engine = get_engine()
    sora_query = """
    SELECT DISTINCT p.`"id"` AS prompt_id
    FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
    INNER JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
    INNER JOIN flow_rds_prod.tbl_wide_rds_prompt p ON ptl.`"PromptId"` = p.`"id"`
    WHERE pt.`"name"` = 'Sora'
    """
    sora_df = pd.read_sql(sora_query, engine)
    sora_ids = tuple(sora_df["prompt_id"].tolist()) or ("",)

    top_sora_query = f"""
    SELECT prompt_id FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{event_date}' AND prompt_id IN {sora_ids}
    GROUP BY prompt_id ORDER BY COUNT(DISTINCT event_id) DESC LIMIT {top_n}
    """
    top_non_sora_query = f"""
    SELECT prompt_id FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{event_date}' AND prompt_id NOT IN {sora_ids}
    GROUP BY prompt_id ORDER BY COUNT(DISTINCT event_id) DESC LIMIT {top_n}
    """

    top_sora_df = pd.read_sql(top_sora_query, engine)
    top_non_sora_df = pd.read_sql(top_non_sora_query, engine)

    return {
        "sora": top_sora_df["prompt_id"].tolist(),
        "non_sora": top_non_sora_df["prompt_id"].tolist()
    }

# ========== 工具函数 ==========
def compute_final_score(row):
    score_items = []
    for key in [
        "click_rate_click_rate",
        "chat_start_rate_chat_start_rate",
        "avg_chat_rounds_avg_chat_rounds",
        "sticky_score_sticky_score",
        "avg_chat_rounds_user_avg_chat_rounds_per_user",
        "avg_retention_days_avg_retention_days"
    ]:
        val = row.get(key)
        if val is not None:
            score_items.append(val)
    if score_items:
        return round(sum(score_items) / len(score_items), 4)
    return None

# ========== SQL生成器 ==========
def generate_bot_quality_sql(bot_id: str, date: str, sticky_start: str, sticky_end: str) -> Dict[str, str]:
    sql = {}
    sql["click_rate"] = f"""
    WITH view_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_bot_view
      WHERE event_date = '{date}' AND bot_id = '{bot_id}'
    ),
    show_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_show_prompt_card
      WHERE event_date = '{date}' AND prompt_id = '{bot_id}'
    )
    SELECT COUNT(v.user_id) AS click_user_cnt, COUNT(s.user_id) AS show_user_cnt,
           ROUND(COUNT(v.user_id) / NULLIF(COUNT(s.user_id), 0), 4) AS click_rate
    FROM view_users v FULL OUTER JOIN show_users s ON v.user_id = s.user_id;
    """

    sql["chat_start_rate"] = f"""
    WITH chat_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_chat_send
      WHERE event_date = '{date}' AND prompt_id = '{bot_id}'
    ),
    click_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_bot_view
      WHERE event_date = '{date}' AND bot_id = '{bot_id}'
    )
    SELECT COUNT(c.user_id) AS chat_user_cnt, COUNT(v.user_id) AS click_user_cnt,
           ROUND(COUNT(c.user_id) / NULLIF(COUNT(v.user_id), 0), 4) AS chat_start_rate
    FROM chat_users c FULL OUTER JOIN click_users v ON c.user_id = v.user_id;
    """

    sql["avg_chat_rounds"] = f"""
    SELECT COUNT(*) AS total_chat_rounds, COUNT(DISTINCT user_id) AS chat_user_cnt,
           ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{date}' AND prompt_id = '{bot_id}';
    """

    sql["sticky_score"] = f"""
    WITH base_chat AS (
      SELECT user_id, prompt_id, event_date
      FROM flow_event_info.tbl_app_event_chat_send
      WHERE prompt_id = '{bot_id}' AND event_date >= '{sticky_start}' AND event_date < '{sticky_end}'
    ),
    first_chat AS (
      SELECT user_id, MIN(event_date) AS first_chat_date FROM base_chat GROUP BY user_id
    ),
    chat_with_gap AS (
      SELECT b.user_id, DATEDIFF(b.event_date, f.first_chat_date) AS day_gap
      FROM base_chat b JOIN first_chat f ON b.user_id = f.user_id
      WHERE b.event_date > f.first_chat_date AND b.event_date <= DATE_ADD(f.first_chat_date, INTERVAL 15 DAY)
    )
    SELECT COUNT(DISTINCT user_id) AS total_return_users,
           SUM(16 - day_gap) AS sticky_score
    FROM chat_with_gap WHERE day_gap BETWEEN 1 AND 15;
    """

    sql["avg_chat_rounds_user"] = f"""
    SELECT COUNT(*) AS total_chat_rounds, COUNT(DISTINCT user_id) AS total_user_cnt,
           ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds_per_user
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE prompt_id = '{bot_id}' AND event_date >= '{sticky_start}' AND event_date < '{sticky_end}';
    """

    sql["avg_retention_days"] = f"""
        WITH d0_users AS (
          SELECT user_id, MIN(event_date) AS first_date
          FROM flow_event_info.tbl_app_event_bot_view
          WHERE bot_id = '{bot_id}' AND event_date BETWEEN '{sticky_start}' AND DATE_ADD('{sticky_start}', INTERVAL 3 DAY)
          GROUP BY user_id
        ),
        future_views AS (
          SELECT d.user_id, DATEDIFF(v.event_date, d.first_date) AS day_gap
          FROM d0_users d
          JOIN flow_event_info.tbl_app_event_bot_view v
            ON d.user_id = v.user_id AND v.bot_id = '{bot_id}'
          WHERE v.event_date > d.first_date AND v.event_date <= DATE_ADD(d.first_date, INTERVAL 14 DAY)
        ),
        user_retention_days AS (
          SELECT user_id, COUNT(DISTINCT day_gap) AS retained_days
          FROM future_views
          GROUP BY user_id
        )
        SELECT COUNT(*) AS user_cnt,
               ROUND(AVG(retained_days), 2) AS avg_retention_days
        FROM user_retention_days;
        """
    return sql

# ========== 指标分析 ==========
def analyze_bot_indicators(bot_id: str, date: str, sticky_start: str, sticky_end: str) -> pd.DataFrame:
    print(f"分析 bot: {bot_id}")
    engine = get_engine()
    sqls = generate_bot_quality_sql(bot_id, date, sticky_start, sticky_end)
    results = {}

    for name, sql in sqls.items():
        try:
            df = pd.read_sql(sql, engine)
            if not df.empty:
                for col in df.columns:
                    results[f"{name}_{col}"] = df.iloc[0][col]
            else:
                results[f"{name}_empty"] = True
        except Exception as e:
            print(f"❌ {name} 查询失败: {e}")
            results[f"{name}_error"] = str(e)

    results['prompt_id'] = bot_id
    results['final_score'] = compute_final_score(results)
    return pd.DataFrame([results])

# ========== 主程序入口 ==========
if __name__ == "__main__":
    start_date = datetime.strptime("2025-04-01", "%Y-%m-%d")
    end_date = datetime.strptime("2025-04-17", "%Y-%m-%d")
    top_n = 10
    all_daily = []

    while start_date <= end_date:
        date_str = start_date.strftime("%Y-%m-%d")
        sticky_start = (start_date - timedelta(days=14)).strftime("%Y-%m-%d")
        sticky_end = (start_date + timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            bot_groups = get_top_bot_ids_by_type(date_str, top_n)
            for group_type, bot_list in bot_groups.items():
                for bot_id in bot_list:
                    df = analyze_bot_indicators(bot_id, date_str, sticky_start, sticky_end)
                    df['bot_type'] = group_type
                    df['event_date'] = date_str
                    all_daily.append(df)
        except Exception as e:
            print(f"❌ {date_str} 处理失败: {e}")

        start_date += timedelta(days=1)

    if all_daily:
        result_df = pd.concat(all_daily, ignore_index=True)
        result_df.to_csv("all_bot_scores_daily.csv", index=False)

        # 折线图绘制
        plt.figure(figsize=(10, 6))
        for btype in ['sora', 'non_sora']:
            tmp = result_df[result_df['bot_type'] == btype].groupby("event_date")["final_score"].mean()
            if not tmp.empty:
                plt.plot(tmp.index, tmp.values, label=btype)
        plt.xticks(rotation=45)
        plt.title("Sora vs 非 Sora Bot 每日平均得分趋势")
        plt.ylabel("平均得分")
        plt.tight_layout()
        plt.legend()
        plt.savefig("bot_quality_trend.png")
        plt.show()
    else:
        print("⚠️ 没有分析结果。")