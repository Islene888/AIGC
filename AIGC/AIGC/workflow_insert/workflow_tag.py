import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        "mysql+pymysql://flowgptzmy:{}@3.135.224.186:9030/flow_wide_info?charset=utf8mb4".format(password)
    )
def insert_prompt_tag_with_v5(engine, event_date):
    query = """
    SELECT DISTINCT
      a.prompt_id,
      a.tag AS workflow,
      TRIM(REGEXP_REPLACE(pt.`"name"`, '^V5\\s+', '')) AS tags
    FROM 
        flow_wide_info.AIGC_prompt_tag a
    JOIN 
        flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl ON a.prompt_id = ptl.`"PromptId"`
    JOIN 
        flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
    JOIN 
        flow_rds_prod.tbl_wide_rds_prompt_tag_path ptp ON pt.`"pathId"` = ptp.`"id"`
    WHERE 
        ptp.`"path"` LIKE '%%V5%%'
      AND a.event_date = :event_date
    """
    df = pd.read_sql_query(text(query), engine, params={'event_date': event_date.strftime('%Y-%m-%d')})
    if df.empty:
        print(f"{event_date} 没有需要插入的数据")
        return 0
    # 保证字段完全一致
    df = df[['prompt_id', 'workflow', 'tags']]
    print(df.head())
    df.to_sql(
        'AIGC_prompt_tag_with_v5',
        con=engine,
        schema='flow_wide_info',
        if_exists='append',
        index=False,
        chunksize=1000,
        method='multi'
    )
    print(f"{event_date} 插入 AIGC_prompt_tag_with_v5 成功，数量：{len(df)}")
    return len(df)


def main(start_date_str, end_date_str):
    engine = get_engine()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    total = 0
    for i in range((end_date - start_date).days + 1):
        day = start_date + timedelta(days=i)
        total += insert_prompt_tag_with_v5(engine, day)
    print(f"全部插入完成，总计：{total} 条。")

if __name__ == "__main__":
    main("2025-05-30", "2025-06-02")
