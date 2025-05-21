import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse

# 创建数据库连接
def get_db_connection():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    db_url = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(db_url)

# 插入数据函数
def insert_bot_tags_exploded_daily(date_str):
    sql = f"""
    INSERT INTO tbl_bot_tags_exploded_daily
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
    with engine.begin() as conn:
        conn.execute(text(sql))
        print(f"✅ 插入完成: {date_str}")

# 主函数（定义起止时间）
if __name__ == "__main__":
    START_DATE = "2025-04-17"
    END_DATE = "2025-05-16"

    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d")
                 for i in range((end_date - start_date).days + 1)]

    for d in date_list:
        insert_bot_tags_exploded_daily(d)
