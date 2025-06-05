import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from datetime import date, timedelta, datetime

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

def insert_luna_tags_for_date(engine, target_date):
    query = """
    SELECT 
        ptl.`"PromptId"` AS prompt_id,
        pt.`"name"` AS tag
    FROM 
        flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
    JOIN 
        flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
    JOIN 
        flow_rds_prod.tbl_wide_rds_prompt_tag_path ptp ON pt.`"pathId"` = ptp.`"id"`
    WHERE 
        ptp.`"path"` LIKE '%%Luna%%'
      AND DATE(pt.`"createdAt"`) = :the_date
    """
    df = pd.read_sql_query(text(query), engine, params={'the_date': target_date.strftime('%Y-%m-%d')})
    if df.empty:
        print(f"{target_date} 没有需要插入的数据")
        return 0
    df['event_date'] = target_date
    df.to_sql(
        'AIGC_prompt_tag',
        con=engine,
        schema='flow_wide_info',
        if_exists='append',
        index=False,
        chunksize=1000,
        method='multi'
    )
    print(f"{target_date} 插入成功，数量：{len(df)}")
    return len(df)

def main(start_date_str, end_date_str):
    engine = get_db_connection()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    total = 0
    for i in range((end_date - start_date).days + 1):
        day = start_date + timedelta(days=i)
        total += insert_luna_tags_for_date(engine, day)
    print(f"全部插入完成，总计：{total} 条。")

if __name__ == "__main__":
    # 手动指定开始日期
    start_date = "2025-05-30"
    # 自动获取今天日期
    end_date = date.today().strftime("%Y-%m-%d")
    main(start_date, end_date)
