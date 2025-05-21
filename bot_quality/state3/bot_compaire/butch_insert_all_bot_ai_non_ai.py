from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime, timedelta

# ===== Step 1: 数据库连接 =====
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# ===== Step 2: 大盘插入 SQL =====
def generate_market_insert_sql(event_date: str) -> str:
    return f"""
    INSERT INTO tbl_bot_daily_quality_source
    SELECT
      '{event_date}' AS event_date,
      prompt_id,
      event_cnt,
      tile,
      layer_name,
      rn,
      'market' AS bot_type
    FROM (
      WITH base_stat AS (
        SELECT prompt_id,
               COUNT(DISTINCT user_id) AS chat_users,
               COUNT(*) AS total_chat,
               COUNT(DISTINCT event_id) AS event_cnt
        FROM flow_event_info.tbl_app_event_chat_send
        WHERE event_date = '{event_date}'
        GROUP BY prompt_id
        HAVING chat_users >= 3 OR event_cnt >= 5
      ),
      with_tile AS (
        SELECT *, NTILE(10) OVER (ORDER BY event_cnt DESC) AS tile
        FROM base_stat
      ),
      layered_sample AS (
        SELECT *,
               CONCAT(CAST((tile - 1) * 10 AS STRING), '-', CAST(tile * 10 AS STRING), '%') AS layer_name,
               ROW_NUMBER() OVER (PARTITION BY tile ORDER BY RAND()) AS rn
        FROM with_tile
      )
      SELECT *
      FROM layered_sample
      WHERE rn <= 1000
    ) t;
    """

# ===== Step 3: Sora 插入 SQL =====
def generate_sora_insert_sql(event_date: str) -> str:
    return f"""
    INSERT INTO tbl_bot_daily_quality_source
    SELECT
      '{event_date}' AS event_date,
      cs.prompt_id,
      COUNT(DISTINCT cs.event_id) AS event_cnt,
      NULL AS tile,
      NULL AS layer_name,
      NULL AS rn,
      'sora' AS bot_type
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
    ORDER BY event_cnt DESC;
    """

# ===== Step 4: 批量执行插入逻辑 =====
def run_batch_insert_for_range(start_date: str, end_date: str):
    engine = get_engine()
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    with engine.connect() as conn:
        for i in range((end - start).days + 1):
            current_date = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            print(f"\n📅 插入数据：{current_date}")

            print(f"  ➤ 插入 market...")
            conn.execute(text(generate_market_insert_sql(current_date)))
            print(f"  ✅ market 插入完成")

            print(f"  ➤ 插入 sora...")
            conn.execute(text(generate_sora_insert_sql(current_date)))
            print(f"  ✅ sora 插入完成")

# ✅ 一键运行 4 月 1 日 ~ 4 月 20 日
run_batch_insert_for_range("2025-04-01", "2025-04-20")
