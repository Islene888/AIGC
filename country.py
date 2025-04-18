import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime
import os

# ===== 数据库连接 =====
def get_engine():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    db_url = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_wide_info?charset=utf8mb4"
    return create_engine(db_url, connect_args={"connect_timeout": 600})

# ===== 单日建表与查询逻辑（使用 get_json_string 提取 geo 国家）=====
def create_tmp_tables_and_query(engine, experiment_id, target_date):
    next_day = (datetime.strptime(target_date, "%Y-%m-%d") + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    with engine.connect() as conn:
        print(f"🧹 清理 tmp 表 ({target_date})...")
        for tbl in [
            "tmp_user_variation_country", "tmp_user_geo_partial",
            "tmp_user_variation_country_final", "tmp_active_user_with_d1"
        ]:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
            except:
                pass

        print("🧩 创建去重后的用户分组表（ROW_NUMBER）...")
        conn.execute(text(f"""
            CREATE TABLE tmp_user_variation_country AS
            SELECT user_id, CAST(variation_id AS CHAR) AS variation
            FROM (
                SELECT user_id, variation_id,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp_assigned DESC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_id}'
                  AND timestamp_assigned <= '{target_date}'
            ) t
            WHERE rn = 1;
        """))

        print("🌍 创建 geo 国家信息（使用 get_json_string）...")
        conn.execute(text(f"""
            CREATE TABLE tmp_user_geo_partial AS
            SELECT /*+ SET_VAR(query_timeout = 120000) */
                user_id, 
                   get_json_string(geo, '$.country') AS country
            FROM flowgpt.tbl_event_app
            WHERE event_date = '{target_date}'
              AND user_id IS NOT NULL AND user_id != ''
              AND get_json_string(geo, '$.country') IS NOT NULL
              AND get_json_string(geo, '$.country') != '';
        """))

        print("🔗 合并 geo 与实验标签，仅保留交集用户")
        conn.execute(text(f"""
            CREATE TABLE tmp_user_variation_country_final AS
            SELECT v.user_id, v.variation, g.country
            FROM tmp_user_variation_country v
            JOIN tmp_user_geo_partial g ON v.user_id = g.user_id;
        """))
        conn.execute(text("DROP TABLE IF EXISTS tmp_user_variation_country"))
        conn.execute(text("CREATE TABLE tmp_user_variation_country AS SELECT * FROM tmp_user_variation_country_final"))
        conn.execute(text("DROP TABLE IF EXISTS tmp_user_variation_country_final"))

        print("📅 创建活跃用户表并判断 D1 留存...")
        conn.execute(text(f"""
            CREATE TABLE tmp_active_user_with_d1 AS
            SELECT a.user_id, a.active_date,
                   CASE WHEN b.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_d1
            FROM (
                SELECT DISTINCT user_id, active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date = '{target_date}'
                  AND keep_alive_flag = 1 AND user_id != ''
            ) a
            LEFT JOIN (
                SELECT DISTINCT user_id, active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date = '{next_day}'
                  AND keep_alive_flag = 1
            ) b
            ON a.user_id = b.user_id;
        """))

        print("📊 查询留存数据...")
        sql = f"""
            SELECT
                '{target_date}' AS dt,
                e.variation,
                e.country,
                COUNT(DISTINCT n.user_id) AS active_users,
                COUNT(DISTINCT CASE WHEN n.is_d1 = 1 THEN n.user_id END) AS d1
            FROM tmp_active_user_with_d1 n
            JOIN tmp_user_variation_country e ON n.user_id = e.user_id
            GROUP BY e.variation, e.country;
        """
        df = pd.read_sql(text(sql), conn)
        return df

# ===== 主流程（只跑 2025-04-03）=====
if __name__ == "__main__":
    experiment_id = "app_sensitive_image_exp"
    target_date = "2025-04-03"
    engine = get_engine()
    os.makedirs("daily_retention", exist_ok=True)

    try:
        df_result = create_tmp_tables_and_query(engine, experiment_id, target_date)
        if df_result is not None:
            df_result["country"] = df_result["country"].replace("", "unknown").fillna("unknown")
            df_result["retention_d1"] = df_result["d1"] / df_result["active_users"]
            output_path = f"daily_retention/retention_by_country_active_{experiment_id}_{target_date}.csv"
            df_result.to_csv(output_path, index=False)
            print(f"✅ 已保存：{output_path}")
            print(df_result)
    except Exception as e:
        print(f"❌ {target_date} 留存分析失败：", e)