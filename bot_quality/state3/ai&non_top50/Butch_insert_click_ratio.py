import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ========== 数据库连接 ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# ========== 带分批逻辑的 SQL ==========
def get_insert_sqls(event_date: str, batch_id: int) -> list[str]:
    return [
        # A. Sora Bot
        f"""
        INSERT INTO tbl_bot_click_rate_top100_daily
        SELECT /*+ SET_VAR(query_timeout = 120000) */
          t.event_date,
          t.prompt_id,
          'sora' AS bot_type,
          COUNT(DISTINCT v.user_id)  AS click_user_cnt,
          COUNT(DISTINCT s.user_id)  AS show_user_cnt,
          ROUND(
            COUNT(DISTINCT v.user_id)
            / NULLIF(COUNT(DISTINCT s.user_id),0)
          , 4) AS click_rate
        FROM tbl_top50_sora_bots_daily t
        LEFT JOIN flow_event_info.tbl_app_event_bot_view v
          ON t.prompt_id  = v.bot_id
         AND v.event_date = '{event_date}'
        LEFT JOIN flow_event_info.tbl_app_event_show_prompt_card s
          ON t.prompt_id  = s.prompt_id
         AND s.event_date = '{event_date}'
        WHERE t.event_date = '{event_date}'
          AND MOD(CRC32(t.prompt_id), 10) = {batch_id}
        GROUP BY t.event_date, t.prompt_id;
        """,

        # B. Non-Sora Bot
        f"""
        INSERT INTO tbl_bot_click_rate_top100_daily
        SELECT /*+ SET_VAR(query_timeout = 120000) */
          t.event_date,
          t.prompt_id,
          'non_sora' AS bot_type,
          COUNT(DISTINCT v.user_id)  AS click_user_cnt,
          COUNT(DISTINCT s.user_id)  AS show_user_cnt,
          ROUND(
            COUNT(DISTINCT v.user_id)
            / NULLIF(COUNT(DISTINCT s.user_id),0)
          , 4) AS click_rate
        FROM tbl_top50_non_sora_bots_daily t
        LEFT JOIN flow_event_info.tbl_app_event_bot_view v
          ON t.prompt_id  = v.bot_id
         AND v.event_date = '{event_date}'
        LEFT JOIN flow_event_info.tbl_app_event_show_prompt_card s
          ON t.prompt_id  = s.prompt_id
         AND s.event_date = '{event_date}'
        WHERE t.event_date = '{event_date}'
          AND MOD(CRC32(t.prompt_id), 10) = {batch_id}
        GROUP BY t.event_date, t.prompt_id;
        """
    ]

# ========== 批量执行逻辑 ==========
def batch_execute(start_date: str, end_date: str):
    engine = get_engine()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

    with engine.begin() as conn:
        while start_dt <= end_dt:
            day = start_dt.strftime("%Y-%m-%d")
            print(f"\n🚀 处理日期：{day}")
            for batch_id in range(100):
                print(f"  ▶️ 执行 Batch {batch_id}/100")
                for i, sql in enumerate(get_insert_sqls(day, batch_id), start=1):
                    try:
                        conn.execute(text(sql))
                        print(f"     ✅ SQL {i}/2 成功")
                    except Exception as e:
                        print(f"     ❌ SQL {i}/2 失败：{e}")
            start_dt += timedelta(days=1)

if __name__ == "__main__":
    # 仅需设定日期范围
    batch_execute("2025-04-17", "2025-04-20")
