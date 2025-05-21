import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse

# ========== 数据库连接 ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )
# ========== SQL 模板 ==========
def get_insert_sqls(event_date: str) -> list[str]:
    return [

        # 插入 Sora Top50
        f"""
        INSERT INTO tbl_top50_sora_bots_daily
        SELECT/*+ SET_VAR(query_timeout = 120000) */
          '{event_date}' AS event_date,
          cs.prompt_id,
          COUNT(DISTINCT cs.event_id) AS event_cnt
        FROM flow_event_info.tbl_app_event_chat_send cs
        JOIN (
            SELECT DISTINCT p.`"id"` AS prompt_id
            FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
            INNER JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
            INNER JOIN flow_rds_prod.tbl_wide_rds_prompt p ON ptl.`"PromptId"` = p.`"id"`
            WHERE pt.`"name"` = 'Sora'
        ) s ON cs.prompt_id = s.prompt_id
        WHERE cs.event_date = '{event_date}'
        GROUP BY cs.prompt_id
        ORDER BY event_cnt DESC
        LIMIT 100;
        """,

        # 插入 Non-Sora Top50
        f"""
        INSERT INTO tbl_top50_non_sora_bots_daily
        SELECT/*+ SET_VAR(query_timeout = 120000) */
          '{event_date}' AS event_date,
          cs.prompt_id,
          COUNT(DISTINCT cs.event_id) AS event_cnt
        FROM flow_event_info.tbl_app_event_chat_send cs
        WHERE cs.event_date = '{event_date}'
          AND cs.prompt_id NOT IN (
            SELECT DISTINCT p.`"id"` AS prompt_id
            FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
            INNER JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
            INNER JOIN flow_rds_prod.tbl_wide_rds_prompt p ON ptl.`"PromptId"` = p.`"id"`
            WHERE pt.`"name"` = 'Sora'
          )
        GROUP BY cs.prompt_id
        ORDER BY event_cnt DESC
        LIMIT 100;
        """


    ]

# ========== 主函数 ==========
def batch_insert(start_date: str, end_date: str):
    engine = get_engine()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    with engine.connect() as conn:
        while start_dt <= end_dt:
            current_day = start_dt.strftime("%Y-%m-%d")
            print(f"🚀 正在插入日期：{current_day}")
            sqls = get_insert_sqls(current_day)

            for i, sql in enumerate(sqls):
                try:
                    conn.execute(text(sql))
                    print(f"✅ SQL {i+1}/3 执行成功")
                except Exception as e:
                    print(f"❌ SQL {i+1}/3 执行失败：{e}")
            start_dt += timedelta(days=1)

# ========== 执行入口 ==========
if __name__ == "__main__":
    # 设置时间范围
    batch_insert("2025-04-02", "2025-04-20")
