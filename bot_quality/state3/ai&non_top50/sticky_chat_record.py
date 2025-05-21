import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ✅ 数据库连接
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# ✅ Sora Bot SQL 构造
def generate_sora_sql(cohort_date: str) -> str:
    cohort_end = (datetime.strptime(cohort_date, "%Y-%m-%d") + timedelta(days=14)).strftime("%Y-%m-%d")
    return f"""
    INSERT INTO tbl_user_chat_rounds
    SELECT/*+ SET_VAR(query_timeout = 120000) */
      user_id,
      prompt_id,
      event_date,
      'sora' AS bot_type,
      ROW_NUMBER() OVER (
        PARTITION BY user_id, prompt_id
        ORDER BY event_date
      ) AS chat_index
    FROM (
      SELECT DISTINCT cs.user_id, cs.prompt_id, cs.event_date
      FROM flow_event_info.tbl_app_event_chat_send cs
      INNER JOIN (
        SELECT prompt_id
        FROM tbl_top50_sora_bots_daily
        WHERE event_date = '{cohort_date}'
      ) top50
      ON cs.prompt_id = top50.prompt_id
      WHERE cs.event_date BETWEEN '{cohort_date}' AND '{cohort_end}'
    ) AS deduped;
    """

# ✅ non-Sora Bot SQL 构造
def generate_non_sora_sql(cohort_date: str) -> str:
    cohort_end = (datetime.strptime(cohort_date, "%Y-%m-%d") + timedelta(days=14)).strftime("%Y-%m-%d")
    return f"""
    INSERT INTO tbl_user_chat_rounds
    SELECT/*+ SET_VAR(query_timeout = 120000) */
      user_id,
      prompt_id,
      event_date,
      'non_sora' AS bot_type,
      ROW_NUMBER() OVER (
        PARTITION BY user_id, prompt_id
        ORDER BY event_date
      ) AS chat_index
    FROM (
      SELECT DISTINCT cs.user_id, cs.prompt_id, cs.event_date
      FROM flow_event_info.tbl_app_event_chat_send cs
      INNER JOIN (
        SELECT prompt_id
        FROM tbl_top50_non_sora_bots_daily
        WHERE event_date = '{cohort_date}'
      ) top50
      ON cs.prompt_id = top50.prompt_id
      WHERE cs.event_date BETWEEN '{cohort_date}' AND '{cohort_end}'
    ) AS deduped;
    """

# ✅ 主逻辑：批量循环写入两个 bot_type
def main():
    cohort_start = datetime.strptime("2025-04-16", "%Y-%m-%d")
    cohort_end = datetime.strptime("2025-04-22", "%Y-%m-%d")

    engine = get_engine()
    current_date = cohort_start

    while current_date <= cohort_end:
        cohort_str = current_date.strftime("%Y-%m-%d")

        try:
            with engine.begin() as conn:
                print(f"🚀 插入 sora | cohort: {cohort_str}")
                conn.execute(text(generate_sora_sql(cohort_str)))
                print(f"✅ sora 成功：{cohort_str}")
        except Exception as e:
            print(f"❌ sora 失败：{cohort_str}，错误：{e}")

        try:
            with engine.begin() as conn:
                print(f"🚀 插入 non_sora | cohort: {cohort_str}")
                conn.execute(text(generate_non_sora_sql(cohort_str)))
                print(f"✅ non_sora 成功：{cohort_str}")
        except Exception as e:
            print(f"❌ non_sora 失败：{cohort_str}，错误：{e}")

        current_date += timedelta(days=1)

if __name__ == "__main__":
    main()
