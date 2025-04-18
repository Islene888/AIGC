import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse

# ========== 配置数据库 ==========
password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
DB_URI = f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
engine = create_engine(DB_URI)

# ========== 批量插入函数 ==========
def insert_data_for_date(event_date_str):
    print(f"🚀 插入 {event_date_str} 的数据...")

    sql = f"""
        INSERT INTO flow_event_info.tbl_wide_user_active_geo_daily (event_date, country, user_id, platform, system_language)
        SELECT /*+ SET_VAR(query_timeout = 120000) */
            DISTINCT
            '{event_date_str}' AS event_date,
            COALESCE(g.country, 'unknown') AS country,
            a.user_id,
            g.platform,
            g.system_language
        FROM (
            SELECT DISTINCT user_id
            FROM flow_wide_info.tbl_wide_active_user_app_info
            WHERE active_date = '{event_date_str}'
              AND keep_alive_flag = 1
              AND user_id IS NOT NULL AND user_id != ''
        ) a
        LEFT JOIN (
            SELECT user_id,
                   get_json_string(geo, '$.country') AS country,
                   get_json_string(device, '$.system_language') AS system_language,
                   platform
            FROM flowgpt.tbl_event_app
            WHERE event_date = '{event_date_str}'
              AND get_json_string(geo, '$.country') IS NOT NULL
              AND get_json_string(geo, '$.country') != ''
        ) g ON a.user_id = g.user_id
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
        print(f"✅ 插入成功: {event_date_str}")
    except Exception as e:
        print(f"❌ 插入失败: {event_date_str}，错误: {e}")

# ========== 主流程 ==========
if __name__ == "__main__":
    start_date = datetime.strptime("2025-04-13", "%Y-%m-%d")
    end_date = datetime.today()

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        insert_data_for_date(date_str)
        current_date += timedelta(days=1)
