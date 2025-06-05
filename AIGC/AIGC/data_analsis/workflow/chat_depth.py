# 文件名：chat_rounds.py

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse
import logging

# 初始化日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 数据库连接
import logging
import os
from dotenv import load_dotenv
load_dotenv()
def get_db_connection():
    password = urllib.parse.quote_plus(os.environ['DB_PASSWORD'])
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    engine = create_engine(DATABASE_URL)
    logging.info("✅ 数据库连接已建立。")
    return engine

# 插入指定日期的人均聊天轮次
def insert_chat_rounds_for_date(event_date: str):
    engine = get_db_connection()
    sql = f"""
    INSERT INTO flow_report_app.tbl_report_AIGC_chat_rounds
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

# 主方法，支持外部传参调用
def main(start_date_str: str, end_date_str: str):
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        if start_date > end_date:
            logging.error("❌ 开始日期不能晚于结束日期")
            return
    except ValueError as ve:
        logging.error(f"❌ 日期格式错误：{ve}")
        return

    curr_date = start_date
    while curr_date <= end_date:
        date_str = curr_date.strftime("%Y-%m-%d")
        insert_chat_rounds_for_date(date_str)
        curr_date += timedelta(days=1)

if __name__ == "__main__":
    main("2025-06-05", "2025-06-05")
