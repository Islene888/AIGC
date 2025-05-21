from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import urllib.parse

def get_engine():
    password = urllib.parse.quote_plus("flowgpt@2024.com")  # 真实密码
    return create_engine(
        f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

def insert_chat_depth_compare(start_date_str: str, end_date_str: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    engine = get_engine()

    with engine.connect() as conn:
        for i in range((end_date - start_date).days + 1):
            date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
            sql = f"""
            INSERT INTO tbl_report_chat_depth_compare_daily
            SELECT
              event_date,
              bot_type,
              SUM(total_msgs) AS total_msgs,
              SUM(user_count) AS user_count,
              CASE
                WHEN SUM(user_count) = 0 THEN 0
                ELSE ROUND(SUM(total_msgs) * 1.0 / NULLIF(SUM(user_count), 0), 4)
              END AS chat_depth_user
            FROM (
              SELECT
                '{date_str}' AS event_date,
                c.prompt_id AS bot_id,
                CASE 
                  WHEN a.prompt_id IS NOT NULL THEN 'AIGC' 
                  ELSE 'AllBots' 
                END AS bot_type,
                COUNT(*) AS total_msgs,
                COUNT(DISTINCT c.user_id) AS user_count
              FROM flow_event_info.tbl_app_event_chat_send c
              LEFT JOIN AIGC_prompt_tag_with_v5 a 
                ON c.prompt_id = a.prompt_id
              WHERE c.event_date = '{date_str}'
              GROUP BY c.prompt_id, bot_type
            ) t
            GROUP BY event_date, bot_type;
            """
            conn.execute(text(sql))
            print(f"✅ 插入完成: {date_str}")

# 主程序入口
if __name__ == "__main__":
    insert_chat_depth_compare(start_date_str="2025-04-16", end_date_str="2025-05-12")
