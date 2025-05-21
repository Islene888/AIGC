import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse

# 数据库连接函数
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(DATABASE_URL)

# 插入某天的某个指标（优化版：先聚合后 JOIN）
def insert_metric_by_workflow(event_date: str, metric_type: str):
    engine = get_db_connection()

    if metric_type == "click_rate":
        sql = f"""
        INSERT INTO tbl_report_AIGC_metrics_by_workflow
        WITH click_count AS (
            SELECT bot_id AS prompt_id, COUNT(DISTINCT user_id) AS click_users
            FROM flow_event_info.tbl_app_event_bot_view
            WHERE event_date = '{event_date}'
            GROUP BY bot_id
        ),
        show_count AS (
            SELECT prompt_id, COUNT(DISTINCT user_id) AS show_users
            FROM flow_event_info.tbl_app_event_show_prompt_card
            WHERE event_date = '{event_date}'
            GROUP BY prompt_id
        )
        SELECT
            '{event_date}' AS event_date,
            'click_rate' AS metric_type,
            p.workflow,
            IFNULL(SUM(c.click_users), 0) AS total_events,
            IFNULL(SUM(s.show_users), 0) AS total_users,
            COUNT(DISTINCT p.prompt_id) AS prompt_count,
            ROUND(SUM(IFNULL(c.click_users, 0)) * 1.0 / NULLIF(SUM(IFNULL(s.show_users, 0)), 0), 4) AS metric_value
        FROM AIGC_prompt_tag_with_v5 p
        LEFT JOIN click_count c ON p.prompt_id = c.prompt_id
        LEFT JOIN show_count s ON p.prompt_id = s.prompt_id
        GROUP BY p.workflow;
        """

    elif metric_type == "chat_start_rate":
        sql = f"""
        INSERT INTO tbl_report_AIGC_metrics_by_workflow
        WITH valid_prompts AS (
            SELECT DISTINCT prompt_id, workflow
            FROM AIGC_prompt_tag_with_v5
        ),
        view_users AS (
            SELECT bot_id, user_id
            FROM flow_event_info.tbl_app_event_bot_view
            WHERE event_date = '{event_date}'
        ),
        chat_users AS (
            SELECT prompt_id, user_id
            FROM flow_event_info.tbl_app_event_chat_send
            WHERE event_date = '{event_date}'
        ),
        click_with_chat_flag AS (
            SELECT 
                v.bot_id,
                v.user_id,
                IF(c.user_id IS NOT NULL, 1, 0) AS has_chat
            FROM view_users v
            LEFT JOIN chat_users c
            ON v.user_id = c.user_id AND v.bot_id = c.prompt_id
        ),
        agg AS (
            SELECT 
                bot_id,
                COUNT(*) AS click_users,
                SUM(has_chat) AS chat_users
            FROM click_with_chat_flag
            GROUP BY bot_id
        )
        SELECT
            '{event_date}' AS event_date,
            'chat_start_rate' AS metric_type,
            p.workflow,
            SUM(IFNULL(a.click_users, 0)) AS total_events,
            SUM(IFNULL(a.chat_users, 0)) AS total_users,
            COUNT(DISTINCT p.prompt_id) AS prompt_count,
            ROUND(SUM(IFNULL(a.chat_users, 0)) * 1.0 / NULLIF(SUM(IFNULL(a.click_users, 0)), 0), 4) AS metric_value
        FROM AIGC_prompt_tag_with_v5 p
        LEFT JOIN agg a ON p.prompt_id = a.bot_id
        GROUP BY p.workflow;
        """

    elif metric_type == "chat_depth_user":
        sql = f"""
        INSERT INTO tbl_report_AIGC_metrics_by_workflow
        WITH valid_prompts AS (
            SELECT DISTINCT prompt_id, workflow
            FROM AIGC_prompt_tag_with_v5
        ),
        chat_data AS (
            SELECT prompt_id, user_id, event_id
            FROM flow_event_info.tbl_app_event_chat_send
            WHERE event_date = '{event_date}'
        ),
        agg AS (
            SELECT 
                prompt_id,
                COUNT(event_id) AS total_chat_events,
                COUNT(DISTINCT user_id) AS unique_users
            FROM chat_data
            GROUP BY prompt_id
        )
        SELECT
            '{event_date}' AS event_date,
            'chat_depth_user' AS metric_type,
            v.workflow,
            SUM(IFNULL(a.total_chat_events, 0)) AS total_events,
            SUM(IFNULL(a.unique_users, 0)) AS total_users,
            COUNT(DISTINCT v.prompt_id) AS prompt_count,
            ROUND(SUM(IFNULL(a.total_chat_events, 0)) * 1.0 / NULLIF(SUM(IFNULL(a.unique_users, 0)), 0), 2) AS metric_value
        FROM valid_prompts v
        LEFT JOIN agg a ON v.prompt_id = a.prompt_id
        GROUP BY v.workflow;
        """
    else:
        raise ValueError("\u274c \u65e0\u6548\u7684 metric_type")

    with engine.begin() as conn:
        conn.execute(text(sql))
        print(f"\u2705 插入完成：{event_date} {metric_type}")

# 主函数：定义时间范围并批量执行
def run_insert(start_date: str, end_date: str):
    date_start = datetime.strptime(start_date, "%Y-%m-%d")
    date_end = datetime.strptime(end_date, "%Y-%m-%d")
    days = (date_end - date_start).days + 1

    for i in range(days):
        date_str = (date_start + timedelta(days=i)).strftime("%Y-%m-%d")
        for metric in ["click_rate", "chat_start_rate", "chat_depth_user"]:
            try:
                insert_metric_by_workflow(date_str, metric)
            except Exception as e:
                print(f"\u274c 插入失败：{date_str} {metric}，错误：{e}")

# 定义时间范围执行
if __name__ == "__main__":
    run_insert(start_date="2025-04-16", end_date="2025-05-12")