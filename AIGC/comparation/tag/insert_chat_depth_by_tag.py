# 文件名：chat_depth_by_tag.py

import urllib.parse
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import time
import logging

# 日志设置
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 数据库连接函数
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    db_url = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(
        db_url,
        pool_size=5,
        max_overflow=0,
        pool_recycle=3600
    )

# 插入单日聊天深度数据（按标签）
def insert_chat_depth_by_tag_for_date(date_str: str):
    logging.info(f"🚀 正在插入 {date_str} 聊天深度数据")
    engine = get_db_connection()

    # 检查是否有有效数据
    check_sql = f"""
        SELECT COUNT(*) FROM flow_event_info.tbl_app_event_chat_send 
        WHERE event_date = '{date_str}' 
          AND (method IS NULL OR method != 'regenerate')
    """
    with engine.connect() as conn:
        result = conn.execute(text(check_sql)).scalar()
        if result == 0:
            logging.warning(f"⚠️ 跳过 {date_str}：无聊天数据")
            return

    # 插入 SQL
    sql = f"""
    INSERT INTO tbl_report_chat_depth_by_tag
    SELECT * FROM (
      SELECT
        b.event_date,
        m.tag,
        m.bot_type,
        SUM(b.total_chat_events) AS total_chat_events,
        SUM(b.chat_user_count) AS total_chat_user_count,
        ROUND(SUM(b.total_chat_events) * 1.0 / NULLIF(SUM(b.chat_user_count), 0), 2) AS chat_depth_user
      FROM (
        SELECT
          c.event_date,
          c.prompt_id,
          COUNT(*) AS total_chat_events,
          COUNT(DISTINCT c.user_id) AS chat_user_count
        FROM flow_event_info.tbl_app_event_chat_send c
        WHERE c.event_date = '{date_str}'
          AND (c.method IS NULL OR c.method != 'regenerate')
        GROUP BY c.event_date, c.prompt_id
      ) b
      JOIN (
        SELECT
          t.prompt_id,
          t.event_date,
          t.tag,
          CASE 
            WHEN a.prompt_id IS NOT NULL THEN 'AIGC'
            ELSE 'AllBots'
          END AS bot_type
        FROM tbl_bot_tags_exploded_daily t
        LEFT JOIN AIGC_prompt_tag_with_v5 a
          ON t.prompt_id = a.prompt_id
        WHERE t.event_date = '{date_str}'
      ) m
      ON b.prompt_id = m.prompt_id AND b.event_date = m.event_date
      GROUP BY b.event_date, m.tag, m.bot_type
      HAVING SUM(b.total_chat_events) > 0
    ) tmp;
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        logging.info(f"✅ 插入完成: {date_str}")
    except Exception as e:
        logging.error(f"❌ 插入失败: {date_str}，原因: {e}")
        with open("../../insert_failed.sql", "a") as f:
            f.write(f"-- {date_str}\n{sql}\n\n")

# 主方法（统一入口）
def main(start_date_str: str, end_date_str: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    for i in range((end_date - start_date).days + 1):
        date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        insert_chat_depth_by_tag_for_date(date_str)
        if i % 5 == 0:
            time.sleep(1)

# 命令行入口
if __name__ == "__main__":
    main("2025-05-26", "2025-05-26")
