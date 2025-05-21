import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ✅ 数据库连接
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# ✅ 替换后的粘性分析 SQL（含 bot_type 和 d14_retention）
def generate_sql(cohort_date: str) -> str:
    return f"""
    INSERT INTO tbl_bot_daily_sticky_score
SELECT
  t.prompt_id,
  t.first_date AS event_date,
  t.bot_type,
  COUNT(DISTINCT t.user_id) AS new_users,

  -- 留存率
  ROUND(SUM(CASE WHEN t.day_gap = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT t.user_id), 4) AS d1_retention,
  ROUND(SUM(CASE WHEN t.day_gap = 3 THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT t.user_id), 4) AS d3_retention,
  ROUND(SUM(CASE WHEN t.day_gap = 7 THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT t.user_id), 4) AS d7_retention,
  ROUND(SUM(CASE WHEN t.day_gap = 14 THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT t.user_id), 4) AS d14_retention,

  -- 人均活跃天数
  ROUND(AVG(t.active_days), 2) AS avg_active_days,

  -- 综合粘性得分
  ROUND(
    40 * SUM(CASE WHEN t.day_gap = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT t.user_id) +
    20 * SUM(CASE WHEN t.day_gap = 3 THEN 1 ELSE 0 END) / COUNT(DISTINCT t.user_id) +
    10 * SUM(CASE WHEN t.day_gap = 7 THEN 1 ELSE 0 END) / COUNT(DISTINCT t.user_id) +
    10 * SUM(CASE WHEN t.day_gap = 14 THEN 1 ELSE 0 END) / COUNT(DISTINCT t.user_id) +
    20 * AVG(t.active_days) / 15
  , 2) AS sticky_score

FROM (
  SELECT
    f.user_id,
    f.prompt_id,
    f.first_date,
    f.bot_type,
    COUNT(DISTINCT b.event_date) AS active_days,
    DATEDIFF(b.event_date, f.first_date) AS day_gap
  FROM (
    SELECT user_id, prompt_id, event_date AS first_date, bot_type
    FROM tbl_user_chat_rounds
    WHERE chat_index = 1 AND event_date = '{cohort_date}'
  ) f
  LEFT JOIN tbl_user_chat_rounds b
    ON f.user_id = b.user_id AND f.prompt_id = b.prompt_id
    AND b.event_date > f.first_date
    AND b.event_date <= DATE_ADD(f.first_date, INTERVAL 14 DAY)
  GROUP BY f.user_id, f.prompt_id, f.first_date, f.bot_type, DATEDIFF(b.event_date, f.first_date)
) t
GROUP BY t.prompt_id, t.first_date, t.bot_type;

    """

# ✅ 主逻辑：循环多日插入分析结果
def main():
    start_date = datetime.strptime("2025-04-15", "%Y-%m-%d")
    end_date = datetime.strptime("2025-04-22", "%Y-%m-%d")

    engine = get_engine()
    current_date = start_date

    while current_date <= end_date:
        cohort_str = current_date.strftime("%Y-%m-%d")
        sql = generate_sql(cohort_str)

        print(f"🚀 正在插入 {cohort_str} 的 bot 粘性得分...")
        try:
            with engine.begin() as conn:
                conn.execute(text(sql))
            print(f"✅ 插入成功：{cohort_str}")
        except Exception as e:
            print(f"❌ 插入失败：{cohort_str}，错误：{e}")

        current_date += timedelta(days=1)

if __name__ == "__main__":
    main()
