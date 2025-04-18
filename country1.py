import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import os

# ===== 数据库连接 =====
def get_engine():
    password = urllib.parse.quote_plus("flowgpt@2024.com")
    db_url = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(db_url, connect_args={"connect_timeout": 600})

# ===== 单日留存分析主逻辑 =====
def run_retention_analysis(engine, experiment_id, target_date):
    next_day = (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    with engine.connect() as conn:
        print(f"\n📅 开始分析日期：{target_date}")
        for tbl in ["tmp_user_variation_country", "tmp_active_user_with_d1"]:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl}"))
            except Exception as e:
                print(f"⚠️ 清理表 {tbl} 失败：", e)

        # ========== 1. 分组信息 ==========
        print("🧩 创建分组表（去重 user_id，仅保留最新 variation）")
        conn.execute(text(f"""
            CREATE TABLE  tmp_user_variation_country AS
            SELECT /*+ SET_VAR(query_timeout = 120000) */
                   user_id, CAST(variation_id AS CHAR) AS variation
            FROM (
                SELECT user_id, variation_id,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_date DESC) AS rn
                FROM flow_wide_info.tbl_wide_experiment_assignment_hi
                WHERE experiment_id = '{experiment_id}'
                  AND event_date <= '{target_date}'
            ) t
            WHERE rn = 1;
        """))

        # ========== 2. 留存用户宽表 ==========
        print("📅 构建活跃用户留存宽表")
        conn.execute(text(f"""
            CREATE TABLE  tmp_active_user_with_d1 AS
            SELECT DISTINCT
                   a.user_id, a.active_date,
                   CASE WHEN b.user_id IS NOT NULL THEN 1 ELSE 0 END AS is_d1
            FROM (
                SELECT user_id, active_date
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date = '{target_date}'
                  AND keep_alive_flag = 1 AND user_id != ''
            ) a
            LEFT JOIN (
                SELECT user_id
                FROM flow_wide_info.tbl_wide_active_user_app_info
                WHERE active_date = '{next_day}'
                  AND keep_alive_flag = 1
            ) b ON a.user_id = b.user_id;
        """))

        # ========== 3. 查询汇总 ==========
        print("📊 生成留存分析结果")
        sql = f"""
            SELECT /*+ SET_VAR(query_timeout = 120000) */
                '{target_date}' AS event_date,
                e.variation,
                g.country,
                COUNT(DISTINCT n.user_id) AS active_users,
                COUNT(DISTINCT CASE WHEN n.is_d1 = 1 THEN n.user_id END) AS d1
            FROM tmp_active_user_with_d1 n
            JOIN tmp_user_variation_country e ON n.user_id = e.user_id
            LEFT JOIN flow_event_info.tbl_wide_user_active_geo_daily g
                ON n.user_id = g.user_id AND g.event_date = '{target_date}'
            GROUP BY e.variation, g.country;
        """
        df = pd.read_sql(text(sql), conn)

    # ========== 4. 后处理 ==========
    if df is not None and not df.empty:
        df["country"] = df["country"].replace("", "unknown").fillna("unknown")
        df["retention_d1"] = df["d1"] / df["active_users"].replace(0, 1)
        df["dt"] = target_date
        df["experiment_id"] = experiment_id
        return df
    else:
        print(f"⚠️ 没有生成数据：{target_date}")
        return pd.DataFrame()


# ===== 批量执行并汇总写入 CSV =====
def batch_retention_analysis(experiment_id, start_date, end_date):
    engine = get_engine()
    os.makedirs("daily_retention", exist_ok=True)
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    all_results = []

    while current <= end:
        target_date = current.strftime("%Y-%m-%d")
        try:
            df = run_retention_analysis(engine, experiment_id, target_date)
            if not df.empty:
                all_results.append(df)
        except Exception as e:
            print(f"❌ {target_date} 处理失败：{e}")
        current += timedelta(days=1)

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df = final_df[["dt", "experiment_id", "variation", "country", "active_users", "d1", "retention_d1"]]
        output_path = f"daily_retention/retention_by_country_active_{experiment_id}_{start_date.replace('-', '')}_{end_date.replace('-', '')}.csv"
        final_df.to_csv(output_path, index=False)
        print(f"\n✅ 所有数据已写入：{output_path}")
    else:
        print("⚠️ 没有任何数据生成")


# ===== 启动任务 =====
if __name__ == "__main__":
    experiment_id = "app_sensitive_image_exp"
    batch_retention_analysis(experiment_id, start_date="2025-04-02", end_date="2025-04-13")
