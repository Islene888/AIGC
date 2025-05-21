import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ========== 数据库连接 ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )


# ========== 插入 SQL 模板 ==========
def get_insert_chat_rounds_sqls(event_date: str) -> list[str]:
    return [
        # A. Sora Bot
        f"""
        INSERT INTO tbl_bot_avg_chat_rounds_top50_daily
        SELECT /*+ SET_VAR(query_timeout = 120000) */
          '{event_date}' AS event_date,
          t.prompt_id,
          'sora' AS bot_type,
          COUNT(*) AS total_chat_rounds,
          COUNT(DISTINCT c.user_id) AS chat_user_cnt,
          ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT c.user_id),0), 2) AS avg_chat_rounds
        FROM tbl_top50_sora_bots_daily t
        LEFT JOIN flow_event_info.tbl_app_event_chat_send c
          ON t.prompt_id = c.prompt_id
         AND c.event_date = '{event_date}'
        WHERE t.event_date = '{event_date}'
        GROUP BY t.prompt_id;
        """,

        # B. Non-Sora Bot
        f"""
        INSERT INTO tbl_bot_avg_chat_rounds_top50_daily
        SELECT /*+ SET_VAR(query_timeout = 120000) */
          '{event_date}' AS event_date,
          t.prompt_id,
          'non_sora' AS bot_type,
          COUNT(*) AS total_chat_rounds,
          COUNT(DISTINCT c.user_id) AS chat_user_cnt,
          ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT c.user_id),0), 2) AS avg_chat_rounds
        FROM tbl_top50_non_sora_bots_daily t
        LEFT JOIN flow_event_info.tbl_app_event_chat_send c
          ON t.prompt_id = c.prompt_id
         AND c.event_date = '{event_date}'
        WHERE t.event_date = '{event_date}'
        GROUP BY t.prompt_id;
        """
    ]


# ========== 批量执行 ==========
def batch_insert_chat_rounds(start_date: str, end_date: str):
    engine = get_engine()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

    with engine.begin() as conn:
        while start_dt <= end_dt:
            day = start_dt.strftime("%Y-%m-%d")
            print(f"📊 插入聊天轮数数据：{day}")
            for i, sql in enumerate(get_insert_chat_rounds_sqls(day), start=1):
                try:
                    conn.execute(text(sql))
                    print(f"   ✅ SQL {i}/2 执行成功")
                except Exception as e:
                    print(f"   ❌ SQL {i}/2 执行失败：{e}")
            start_dt += timedelta(days=1)


if __name__ == "__main__":
    batch_insert_chat_rounds("2025-04-01", "2025-04-20")
