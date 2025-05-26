import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime, timedelta
import logging

# 配置数据库连接
password = urllib.parse.quote_plus("flowgpt@2024.com")
DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
engine = create_engine(DATABASE_URL)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def insert_click_rate(event_date: str):
    logging.info(f"📌 正在插入 {event_date} 的点击率数据")
    insert_sql = f"""
    INSERT INTO tbl_report_AIGC_click_rate
    WITH valid_prompts AS (
      SELECT DISTINCT prompt_id, workflow, tags
      FROM AIGC_prompt_tag_with_v5
    ),
    view_count AS (
      SELECT bot_id, COUNT(DISTINCT event_id) AS click_event_cnt
      FROM flow_event_info.tbl_app_event_bot_view
      WHERE event_date = '{event_date}'
        AND bot_id IN (SELECT prompt_id FROM AIGC_prompt_tag_with_v5)
      GROUP BY bot_id
    ),
    show_count AS (
      SELECT prompt_id, COUNT(DISTINCT event_id) AS show_event_cnt
      FROM flow_event_info.tbl_app_event_show_prompt_card
      WHERE event_date = '{event_date}'
        AND prompt_id IN (SELECT prompt_id FROM AIGC_prompt_tag_with_v5)
      GROUP BY prompt_id
    )
    SELECT 
      '{event_date}' AS event_date,
      a.prompt_id,
      a.workflow,
      a.tags,
      IFNULL(v.click_event_cnt, 0) AS click_event_cnt,
      IFNULL(s.show_event_cnt, 0) AS show_event_cnt,
      ROUND(IFNULL(v.click_event_cnt, 0) / NULLIF(s.show_event_cnt, 0), 4) AS click_rate
    FROM valid_prompts a
    LEFT JOIN view_count v ON a.prompt_id = v.bot_id
    LEFT JOIN show_count s ON a.prompt_id = s.prompt_id;
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        logging.info(f"✅ 插入完成: {event_date}")


def main(start_date_str: str, end_date_str: str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    curr_date = start_date
    while curr_date <= end_date:
        date_str = curr_date.strftime("%Y-%m-%d")
        try:
            insert_click_rate(date_str)
        except Exception as e:
            logging.error(f"❌ 插入失败：{date_str}，错误：{e}")
        curr_date += timedelta(days=1)


# 可选的默认执行（也可以删除）
if __name__ == "__main__":
    main("2025-05-23", "2025-05-25")

