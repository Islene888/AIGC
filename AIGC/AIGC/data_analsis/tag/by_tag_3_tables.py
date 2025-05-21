import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse

# 创建数据库连接
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(DATABASE_URL)

# 插入函数
def insert_metric_by_tag(event_date: str, metric_type: str):
    engine = get_db_connection()
    if metric_type == "click_rate":
        sql = f"""
        INSERT INTO tbl_report_AIGC_metrics_by_tag
        WITH agg_prompt_metrics AS (
          SELECT
            prompt_id,
            SUM(click_event_cnt) AS click_events,
            SUM(show_event_cnt) AS show_users
          FROM tbl_report_AIGC_click_rate
          WHERE event_date = '{event_date}'
          GROUP BY prompt_id
        )
        SELECT
          '{event_date}' AS event_date,
          'click_rate' AS metric_type,
          tag,
          SUM(click_events) AS total_events,
          SUM(show_users) AS total_users,
          COUNT(DISTINCT p.prompt_id) AS prompt_count,
          ROUND(SUM(click_events) * 1.0 / NULLIF(SUM(show_users), 0), 4) AS metric_value
        FROM agg_prompt_metrics p
        JOIN tbl_aigc_prompt_tags_exploded t ON p.prompt_id = t.prompt_id
        GROUP BY tag;
        """
    elif metric_type == "chat_start_rate":
        sql = f"""
        INSERT INTO tbl_report_AIGC_metrics_by_tag
        WITH agg_prompt_metrics AS (
          SELECT
            prompt_id,
            SUM(click_user_cnt) AS click_users,
            SUM(chat_user_cnt) AS chat_users
          FROM tbl_report_AIGC_chat_start_rate
          WHERE event_date = '{event_date}'
          GROUP BY prompt_id
        )
        SELECT
          '{event_date}' AS event_date,
          'chat_start_rate' AS metric_type,
          tag,
          SUM(click_users) AS total_events,
          SUM(chat_users) AS total_users,
          COUNT(DISTINCT p.prompt_id) AS prompt_count,
          ROUND(SUM(chat_users) * 1.0 / NULLIF(SUM(click_users), 0), 4) AS metric_value
        FROM agg_prompt_metrics p
        JOIN tbl_aigc_prompt_tags_exploded t ON p.prompt_id = t.prompt_id
        GROUP BY tag;
        """
    elif metric_type == "chat_depth_user":
        sql = f"""
        INSERT INTO tbl_report_AIGC_metrics_by_tag
        WITH agg_prompt_metrics AS (
          SELECT
            prompt_id,
            SUM(total_chat_events) AS chat_events,
            SUM(unique_users) AS users
          FROM tbl_report_AIGC_chat_rounds
          WHERE event_date = '{event_date}'
          GROUP BY prompt_id
        )
        SELECT
          '{event_date}' AS event_date,
          'chat_depth_user' AS metric_type,
          tag,
          SUM(chat_events) AS total_events,
          SUM(users) AS total_users,
          COUNT(DISTINCT p.prompt_id) AS prompt_count,
          ROUND(SUM(chat_events) * 1.0 / NULLIF(SUM(users), 0), 2) AS metric_value
        FROM agg_prompt_metrics p
        JOIN tbl_aigc_prompt_tags_exploded t ON p.prompt_id = t.prompt_id
        GROUP BY tag;
        """
    else:
        raise ValueError("Invalid metric_type")

    with engine.begin() as conn:
        conn.execute(text(sql))
        print(f"✅ {event_date} {metric_type} 插入完成")

# 插入从 2025-04-16 到 2025-05-07 的数据
start_date = datetime.strptime("2025-04-16", "%Y-%m-%d")
for i in range((datetime.strptime("2025-05-16", "%Y-%m-%d") - start_date).days + 1):
    d = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
    for m in ["click_rate", "chat_start_rate", "chat_depth_user"]:
        insert_metric_by_tag(d, m)