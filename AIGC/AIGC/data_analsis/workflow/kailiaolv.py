# 文件名：chat_start_rate.py

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 数据库连接配置
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(DATABASE_URL)

engine = get_db_connection()

# 插入单日 chat_start_rate 数据
def insert_chat_start_rate(event_date: str):
    logging.info(f"🚀 正在插入 {event_date} 的 chat_start_rate 数据")
    sql = f"""
    INSERT INTO tbl_report_AIGC_chat_start_rate
    WITH valid_prompts AS (
      SELECT DISTINCT prompt_id, workflow, tags
      FROM AIGC_prompt_tag_with_v5
    ),
    view_filtered AS (
      SELECT bot_id, user_id
      FROM flow_event_info.tbl_app_event_bot_view
      WHERE event_date = '{event_date}'
    ),
    chat_filtered AS (
      SELECT prompt_id, user_id
      FROM flow_event_info.tbl_app_event_chat_send
      WHERE event_date = '{event_date}'
    ),
    click_with_chat_flag AS (
      SELECT 
        v.bot_id,
        v.user_id,
        IF(c.user_id IS NOT NULL, 1, 0) AS has_chat
      FROM view_filtered v
      LEFT JOIN chat_filtered c
        ON v.user_id = c.user_id AND v.bot_id = c.prompt_id
    ),
    aggregated_clicks AS (
      SELECT
        bot_id,
        COUNT(*) AS click_user_cnt,
        SUM(has_chat) AS chat_user_cnt,
        ROUND(SUM(has_chat) * 1.0 / NULLIF(COUNT(*), 0), 4) AS chat_start_rate
      FROM click_with_chat_flag
      GROUP BY bot_id
    )
    SELECT
      '{event_date}' AS event_date,
      v.prompt_id,
      v.workflow,
      v.tags,
      IFNULL(a.click_user_cnt, 0) AS click_user_cnt,
      IFNULL(a.chat_user_cnt, 0) AS chat_user_cnt,
      IFNULL(a.chat_start_rate, 0) AS chat_start_rate
    FROM valid_prompts v
    LEFT JOIN aggregated_clicks a
      ON v.prompt_id = a.bot_id;
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
            logging.info(f"✅ 插入完成: {event_date}")
    except Exception as e:
        logging.error(f"❌ 插入失败：{event_date}，错误：{e}")

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
        insert_chat_start_rate(date_str)
        curr_date += timedelta(days=1)

# 可选：直接运行文件时默认行为
if __name__ == "__main__":
    main("2025-05-17", "2025-05-25")
