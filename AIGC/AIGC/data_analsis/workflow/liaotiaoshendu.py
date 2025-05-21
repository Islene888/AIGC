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
    WITH valid_prompts AS (
      SELECT DISTINCT prompt_id, workflow, tags
      FROM AIGC_prompt_tag_with_v5
    ),
    chat_filtered AS (
      SELECT *
      FROM flow_event_info.tbl_app_event_chat_send
      WHERE event_date = '{event_date}'
        AND prompt_id IN (SELECT prompt_id FROM valid_prompts)
    )
    SELECT
      '{event_date}' AS event_date,
      a.prompt_id,
      a.workflow,
      a.tags,
      COUNT(c.event_id) AS total_chat_events,
      COUNT(DISTINCT c.user_id) AS unique_users,
      ROUND(COUNT(c.event_id) * 1.0 / NULLIF(COUNT(DISTINCT c.user_id), 0), 2) AS avg_chat_rounds_per_user
    FROM AIGC_prompt_tag_with_v5 a
    LEFT JOIN chat_filtered c
      ON a.prompt_id = c.prompt_id
    GROUP BY a.prompt_id, a.workflow, a.tags;
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
    end_date = datetime.strptime("2025-05-16", "%Y-%m-%d")

    curr_date = start_date
    while curr_date <= end_date:
        date_str = curr_date.strftime("%Y-%m-%d")
        insert_chat_rounds_for_date(date_str)
        curr_date += timedelta(days=1)
