# 文件名：click_rate_compare.py

from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import urllib.parse
import logging

# 日志设置
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 数据库连接
def get_engine():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    return create_engine(
        f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# 插入点击率对比日报
def insert_click_rate_compare(start_date_str: str, end_date_str: str):
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        if start_date > end_date:
            logging.error("❌ 开始日期不能晚于结束日期")
            return
    except ValueError as e:
        logging.error(f"❌ 日期格式错误：{e}")
        return

    engine = get_engine()
    with engine.begin() as conn:
        for i in range((end_date - start_date).days + 1):
            event_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
            sql = f"""
            INSERT INTO tbl_report_click_rate_compare_daily
            SELECT * FROM (
              SELECT
                a.event_date,
                a.workflow,
                'AIGC' AS bot_type,
                SUM(a.click_event_cnt) AS total_clicks,
                SUM(a.show_event_cnt) AS total_shows,
                ROUND(SUM(a.click_event_cnt) * 1.0 / NULLIF(SUM(a.show_event_cnt), 0), 4) AS click_rate
              FROM tbl_report_AIGC_click_rate a
              WHERE a.event_date = '{event_date}'
                AND a.workflow IS NOT NULL AND a.workflow != ''
              GROUP BY a.event_date, a.workflow

              UNION ALL

              SELECT
                '{event_date}' AS event_date,
                'ALL' AS workflow,
                'AllBots' AS bot_type,
                SUM(CASE WHEN b.event_name = 'view_bot' THEN b.events ELSE 0 END) AS total_clicks,
                SUM(CASE WHEN b.event_name = 'show_prompt_card' THEN b.events ELSE 0 END) AS total_shows,
                ROUND(
                  SUM(CASE WHEN b.event_name = 'view_bot' THEN b.events ELSE 0 END) * 1.0 /
                  NULLIF(SUM(CASE WHEN b.event_name = 'show_prompt_card' THEN b.events ELSE 0 END), 0),
                  4
                ) AS click_rate
              FROM flow_report_app.tbl_daily_app_event_version_count b
              WHERE b.event_date = '{event_date}'
                AND b.event_name IN ('view_bot', 'show_prompt_card')
            ) t;
            """
            try:
                conn.execute(text(sql))
                logging.info(f"✅ 插入完成: {event_date}")
            except Exception as e:
                logging.error(f"❌ 插入失败: {event_date}，错误：{e}")

# 主方法（统一接口）
def main(start_date_str: str, end_date_str: str):
    insert_click_rate_compare(start_date_str, end_date_str)

# 命令行入口
if __name__ == "__main__":
    main("2025-04-17", "2025-05-25")
