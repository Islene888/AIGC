import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ========== 数据库连接 ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# ========== 两条 INSERT 模板 ==========
def get_insert_sqls(event_date: str) -> list[str]:
    return [
        # A. Top100 Sora Bot 开聊率
        f"""
        INSERT INTO tbl_bot_chat_start_rate_top100_daily
        SELECT /*+ SET_VAR(query_timeout = 120000) */
          t.event_date,
          t.prompt_id,
          'sora' AS bot_type,
          COUNT(DISTINCT c.user_id)    AS chat_user_cnt,
          COUNT(DISTINCT v.user_id)    AS click_user_cnt,
          ROUND(
            COUNT(DISTINCT c.user_id) 
            / NULLIF(COUNT(DISTINCT v.user_id), 0)
          , 4)                          AS chat_start_rate
        FROM tbl_top50_sora_bots_daily t
        LEFT JOIN flow_event_info.tbl_app_event_bot_view    v
          ON t.prompt_id  = v.bot_id
        AND v.event_date = '{event_date}'
        AND t.event_date = '{event_date}'
        LEFT JOIN flow_event_info.tbl_app_event_chat_send   c
          ON t.prompt_id  = c.prompt_id
         AND c.event_date = '{event_date}'
         AND t.event_date = '{event_date}'
        WHERE t.event_date = '{event_date}'
        GROUP BY t.event_date, t.prompt_id;
        """,

        # B. Top100 非 Sora Bot 开聊率
        f"""
        INSERT INTO tbl_bot_chat_start_rate_top100_daily
        SELECT /*+ SET_VAR(query_timeout = 120000) */
          t.event_date,
          t.prompt_id,
          'non_sora' AS bot_type,
          COUNT(DISTINCT c.user_id)    AS chat_user_cnt,
          COUNT(DISTINCT v.user_id)    AS click_user_cnt,
          ROUND(
            COUNT(DISTINCT c.user_id)
            / NULLIF(COUNT(DISTINCT v.user_id), 0)
          , 4)                          AS chat_start_rate
        FROM tbl_top50_non_sora_bots_daily t
        LEFT JOIN flow_event_info.tbl_app_event_bot_view    v
          ON t.prompt_id  = v.bot_id
         AND t.event_date = '{event_date}'
         AND v.event_date = '{event_date}'
        LEFT JOIN flow_event_info.tbl_app_event_chat_send   c
          ON t.prompt_id  = c.prompt_id
        AND t.event_date = '{event_date}'
        AND c.event_date = '{event_date}'
        WHERE t.event_date = '{event_date}'
        GROUP BY t.event_date, t.prompt_id;
        """
    ]

# ========== 批量执行函数 ==========
def batch_execute(start_date: str, end_date: str):
    engine = get_engine()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

    with engine.begin() as conn:
        while start_dt <= end_dt:
            day = start_dt.strftime("%Y-%m-%d")
            print(f"🚀 处理中：{day}")
            for i, sql in enumerate(get_insert_sqls(day), start=1):
                try:
                    conn.execute(text(sql))
                    print(f"   ✅ SQL {i}/2 执行成功")
                except Exception as e:
                    print(f"   ❌ SQL {i}/2 执行失败：{e}")
            start_dt += timedelta(days=1)

if __name__ == "__main__":
    # 比如：批量执行 2025‑04‑01 至 2025‑04‑07
    batch_execute("2025-04-01", "2025-04-20")
