# 文件名：chat_depth_by_workflow.py

from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import urllib.parse
import logging

# 日志配置
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 数据库连接
def get_engine():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    return create_engine(
        f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# 插入逻辑（可复用）
def insert_chat_depth_by_workflow(start_date_str: str, end_date_str: str):
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
INSERT INTO tbl_report_chat_depth_compare_daily
WITH distinct_prompt AS (
  SELECT DISTINCT prompt_id, workflow
  FROM AIGC_prompt_tag_with_v5
)
SELECT
  '{event_date}' AS event_date,
  bot_type,
  workflow,
  SUM(total_msgs) AS total_msgs,
  SUM(user_count) AS user_count,
  CASE
    WHEN SUM(user_count) = 0 THEN 0
    ELSE ROUND(SUM(total_msgs) * 1.0 / NULLIF(SUM(user_count), 0), 4)
  END AS chat_depth_user
FROM (
  SELECT
    c.prompt_id AS bot_id,
    a.workflow,
    CASE 
      WHEN a.prompt_id IS NOT NULL THEN 'AIGC' 
      ELSE 'AllBots' 
    END AS bot_type,
    COUNT(*) AS total_msgs,  
    COUNT(DISTINCT c.user_id) AS user_count  
  FROM flow_event_info.tbl_app_event_chat_send c
  LEFT JOIN distinct_prompt a 
    ON c.prompt_id = a.prompt_id
  WHERE c.event_date = '{event_date}'
  GROUP BY c.prompt_id, a.workflow, bot_type
) t
GROUP BY bot_type, workflow;
            """
            try:
                conn.execute(text(sql))
                logging.info(f"✅ 插入完成: {event_date}")
            except Exception as e:
                logging.error(f"❌ 插入失败: {event_date}，错误：{e}")

# 主方法（供外部统一调用）
def main(start_date_str: str, end_date_str: str):
    insert_chat_depth_by_workflow(start_date_str, end_date_str)

# 脚本运行入口
if __name__ == "__main__":
    main("2025-05-23", "2025-05-25")
