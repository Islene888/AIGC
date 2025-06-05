# 文件名：bot_tags_exploded.py

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse
import logging

# 初始化日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 创建数据库连接
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

# 单日插入逻辑
def insert_bot_tags_exploded_daily(date_str: str):
    sql = f"""
    INSERT INTO flow_wide_info.tbl_bot_tags_exploded_daily
    WITH raw_tags AS (
      SELECT 
        ptl.`"PromptId"` AS prompt_id,
        GROUP_CONCAT(pt.`"name"`) AS tag_str 
      FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
      JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
      JOIN flow_rds_prod.tbl_wide_rds_prompt_tag_path ptp ON pt.`"pathId"` = ptp.`"id"`
      WHERE ptp.`"path"` LIKE '%V5%'
        AND ptl.`"PromptId"` IN (
          SELECT DISTINCT prompt_id
          FROM flow_event_info.tbl_app_event_show_prompt_card
          WHERE event_date = '{date_str}'
        )
      GROUP BY ptl.`"PromptId"`
    ),
    numbers AS (
      SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
      UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
      UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
      UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20
    )
    SELECT
      '{date_str}' AS event_date,
      r.prompt_id,
      TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(r.tag_str, ',', n.n), ',', -1)) AS tag
    FROM raw_tags r
    JOIN numbers n ON n.n <= LENGTH(r.tag_str) - LENGTH(REPLACE(r.tag_str, ',', '')) + 1
    WHERE TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(r.tag_str, ',', n.n), ',', -1)) != '';
    """
    engine = get_db_connection()
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
            logging.info(f"✅ 插入完成: {date_str}")
    except Exception as e:
        logging.error(f"❌ 插入失败: {date_str}，错误：{e}")

# 主函数（支持统一调用）
def main(start_date_str: str, end_date_str: str):
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        if start_date > end_date:
            logging.error("❌ 开始日期不能晚于结束日期")
            return
    except ValueError as e:
        logging.error(f"❌ 日期格式错误：{e}")
        return

    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d")
                 for i in range((end_date - start_date).days + 1)]

    for d in date_list:
        insert_bot_tags_exploded_daily(d)

# 默认入口
if __name__ == "__main__":
    main("2025-06-05", "2025-06-05")
