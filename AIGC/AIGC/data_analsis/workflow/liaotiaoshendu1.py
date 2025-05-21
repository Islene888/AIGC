import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse
import logging

# 初始化日志
logging.basicConfig(level=logging.INFO)

# 数据库连接
def get_db_engine():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    url = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(url)

# 插入指定日期的人均聊天轮次
def insert_chat_rounds_for_date(event_date: str):
    engine = get_db_engine()
    sql = f"""
    INSERT INTO tbl_report_AIGC_chat_rounds
    WITH valid_data AS (
      SELECT
        u.event_date,
        u.prompt_id,
        t.workflow,
        t.tags,
        u.user_id,
        u.chats
      FROM flow_wide_info.tbl_wide_daily_bot_user_info u
      JOIN AIGC_prompt_tag_with_v5 t
        ON u.prompt_id = t.prompt_id
      WHERE u.event_date = '{event_date}'
        AND u.chats > 0
    )
    SELECT
      event_date,
      prompt_id,
      workflow,
      tags,
      SUM(chats) AS total_chat_events,
      COUNT(DISTINCT user_id) AS unique_users,
      ROUND(SUM(chats) * 1.0 / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds_per_user
    FROM valid_data
    GROUP BY event_date, prompt_id, workflow, tags;
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
            logging.info(f"✅ 已插入 {event_date} 的聊天轮次数据")
    except Exception as e:
        logging.error(f"❌ 插入失败: {event_date}，错误信息：{e}")

if __name__ == "__main__":
    # 设置批量插入的起止日期
    start_date = datetime.strptime("2025-04-16", "%Y-%m-%d")
    end_date = datetime.strptime("2025-05-20", "%Y-%m-%d")

    curr_date = start_date
    while curr_date <= end_date:
        date_str = curr_date.strftime("%Y-%m-%d")
        insert_chat_rounds_for_date(date_str)
        curr_date += timedelta(days=1)
