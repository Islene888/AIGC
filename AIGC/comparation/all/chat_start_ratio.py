import urllib.parse
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

def get_engine():
    password = urllib.parse.quote_plus("flowgpt@2024.com")  # 真实密码
    return create_engine(
        f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

def insert_chat_start_rate(start_date_str: str, end_date_str: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    engine = get_engine()

    with engine.connect() as conn:
        for i in range((end_date - start_date).days + 1):
            event_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
            sql = f"""
            INSERT INTO tbl_report_chat_start_rate_compare_daily
            SELECT * FROM (
              SELECT
                '{event_date}' AS event_date,
                'AIGC' AS workflow,
                'AIGC' AS bot_type,
                SUM(a.click_user_cnt) AS click_user_cnt,
                SUM(a.chat_user_cnt) AS chat_user_cnt,
                ROUND(SUM(a.chat_user_cnt) * 1.0 / NULLIF(SUM(a.click_user_cnt), 0), 4) AS chat_start_rate
              FROM tbl_report_AIGC_chat_start_rate a
              WHERE a.event_date = '{event_date}'

              UNION ALL

              SELECT
                '{event_date}' AS event_date,
                'ALL' AS workflow,
                'AllBots' AS bot_type,
                COUNT(DISTINCT v.user_id) AS click_user_cnt,
                COUNT(DISTINCT c.user_id) AS chat_user_cnt,
                ROUND(COUNT(DISTINCT c.user_id) * 1.0 / NULLIF(COUNT(DISTINCT v.user_id), 0), 4) AS chat_start_rate
              FROM (
                SELECT DISTINCT user_id
                FROM flow_event_info.tbl_app_event_bot_view
                WHERE event_date = '{event_date}'
              ) v
              LEFT JOIN (
                SELECT DISTINCT user_id
                FROM flow_event_info.tbl_app_event_chat_send
                WHERE event_date = '{event_date}'
              ) c
              ON v.user_id = c.user_id
            ) t;
            """
            conn.execute(text(sql))
            print(f"✅ 插入完成: {event_date}")

# 主函数入口
if __name__ == "__main__":
    insert_chat_start_rate(start_date_str="2025-04-16", end_date_str="2025-05-12")
