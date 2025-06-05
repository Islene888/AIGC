# 文件名：chat_start_rate_compare.py

from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import urllib.parse
import logging

# 日志设置
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

# 插入开聊率对比数据
def insert_chat_start_rate_compare(start_date_str: str, end_date_str: str):
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        if start_date > end_date:
            logging.error("❌ 开始日期不能晚于结束日期")
            return
    except ValueError as e:
        logging.error(f"❌ 日期格式错误：{e}")
        return

    engine = get_db_connection()
    with engine.begin() as conn:
        for i in range((end_date - start_date).days + 1):
            event_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
            sql = f"""
            INSERT INTO flow_report_app.tbl_report_chat_start_rate_compare_daily
            SELECT * FROM (
              SELECT
                a.event_date,
                a.workflow,
                'AIGC' AS bot_type,
                SUM(a.click_user_cnt) AS total_click_users,
                SUM(a.chat_user_cnt) AS total_chat_users,
                ROUND(SUM(a.chat_user_cnt) * 1.0 / NULLIF(SUM(a.click_user_cnt), 0), 4) AS chat_start_rate
              FROM tbl_report_AIGC_chat_start_rate a
              WHERE a.event_date = '{event_date}'
                AND a.workflow IS NOT NULL AND a.workflow != ''
              GROUP BY a.event_date, a.workflow

              UNION ALL

              SELECT
                '{event_date}' AS event_date,
                'ALL' AS workflow,
                'AllBots' AS bot_type,
                COUNT(DISTINCT v.user_id) AS total_click_users,
                COUNT(DISTINCT c.user_id) AS total_chat_users,
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
            try:
                conn.execute(text(sql))
                logging.info(f"✅ 插入完成: {event_date}")
            except Exception as e:
                logging.error(f"❌ 插入失败: {event_date}，错误：{e}")

# 主方法
def main(start_date_str: str, end_date_str: str):
    insert_chat_start_rate_compare(start_date_str, end_date_str)

# 脚本直接运行入口
if __name__ == "__main__":
    main("2025-04-16", "2025-05-29")
