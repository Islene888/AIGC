import pandas as pd
from matplotlib import pyplot as plt
from sqlalchemy import create_engine
import urllib.parse
from typing import Dict
import re
from datetime import datetime, timedelta
from datetime import datetime
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'Microsoft YaHei', 'STHeiti']
plt.rcParams['axes.unicode_minus'] = False


def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    engine = create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )
    return engine

def get_top_bot_ids_by_type(event_date: str, top_n: int = 10) -> Dict[str, list]:
    engine = get_engine()

    sora_query = f"""
    SELECT DISTINCT p.`"id"` AS prompt_id
    FROM flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
    INNER JOIN flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
    INNER JOIN flow_rds_prod.tbl_wide_rds_prompt p ON ptl.`"PromptId"` = p.`"id"`
    WHERE pt.`"name"` = 'Sora'
    """
    sora_df = pd.read_sql(sora_query, engine)
    sora_ids = tuple(sora_df["prompt_id"].tolist()) or ("",)

    top_sora_query = f"""
    SELECT prompt_id
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{event_date}' AND prompt_id IN {sora_ids}
    GROUP BY prompt_id
    ORDER BY COUNT(DISTINCT event_id) DESC
    LIMIT {top_n}
    """
    top_sora_df = pd.read_sql(top_sora_query, engine)

    top_non_sora_query = f"""
    SELECT prompt_id
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{event_date}' AND prompt_id NOT IN {sora_ids}
    GROUP BY prompt_id
    ORDER BY COUNT(DISTINCT event_id) DESC
    LIMIT {top_n}
    """
    top_non_sora_df = pd.read_sql(top_non_sora_query, engine)

    return {
        "sora": top_sora_df["prompt_id"].tolist(),
        "non_sora": top_non_sora_df["prompt_id"].tolist()
    }


def generate_bot_quality_sql(bot_id: str, date='2025-01-15', sticky_start='2025-01-15', sticky_end='2025-02-15') -> Dict[str, str]:
    sql = {}

    sql['click_rate'] = f"""
    WITH view_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_bot_view
      WHERE event_date = '{date}' AND bot_id = '{bot_id}'
    ),
    show_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_show_prompt_card
      WHERE event_date = '{date}' AND prompt_id = '{bot_id}'
    )
    SELECT '{date}' AS event_date, '{bot_id}' AS prompt_id,
           COUNT(v.user_id) AS click_user_cnt,
           COUNT(s.user_id) AS show_user_cnt,
           ROUND(COUNT(v.user_id) / NULLIF(COUNT(s.user_id), 0), 4) AS click_rate
    FROM view_users v FULL OUTER JOIN show_users s ON v.user_id = s.user_id;
    """

    sql['chat_start_rate'] = f"""
    WITH chat_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_chat_send
      WHERE event_date = '{date}' AND prompt_id = '{bot_id}'
    ),
    click_users AS (
      SELECT DISTINCT user_id FROM flow_event_info.tbl_app_event_bot_view
      WHERE event_date = '{date}' AND bot_id = '{bot_id}'
    )
    SELECT '{date}' AS event_date, '{bot_id}' AS prompt_id,
           COUNT(c.user_id) AS chat_user_cnt,
           COUNT(v.user_id) AS click_user_cnt,
           ROUND(COUNT(c.user_id) / NULLIF(COUNT(v.user_id), 0), 4) AS chat_start_rate
    FROM chat_users c FULL OUTER JOIN click_users v ON c.user_id = v.user_id;
    """

    sql['avg_chat_rounds'] = f"""
    SELECT '{date}' AS event_date, '{bot_id}' AS prompt_id,
           COUNT(*) AS total_chat_rounds,
           COUNT(DISTINCT user_id) AS chat_user_cnt,
           ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE event_date = '{date}' AND prompt_id = '{bot_id}';
    """

    sql['sticky_score'] = f"""
    WITH base_chat AS (
      SELECT user_id, prompt_id, event_date
      FROM flow_event_info.tbl_app_event_chat_send
      WHERE prompt_id = '{bot_id}' AND event_date >= '{sticky_start}' AND event_date < '{sticky_end}'
    ),
    first_chat AS (
      SELECT user_id, prompt_id, MIN(event_date) AS first_chat_date
      FROM base_chat GROUP BY user_id, prompt_id
    ),
    chat_with_gap AS (
      SELECT b.prompt_id, b.user_id, b.event_date, f.first_chat_date,
             DATEDIFF(b.event_date, f.first_chat_date) AS day_gap
      FROM base_chat b
      JOIN first_chat f ON b.user_id = f.user_id AND b.prompt_id = f.prompt_id
      WHERE b.event_date > f.first_chat_date AND b.event_date <= DATE_ADD(f.first_chat_date, INTERVAL 15 DAY)
    )
    SELECT prompt_id,
           COUNT(DISTINCT user_id) AS total_return_users,
           SUM(16 - day_gap) AS sticky_score
    FROM chat_with_gap
    WHERE day_gap BETWEEN 1 AND 15
    GROUP BY prompt_id;
    """

    sql['avg_chat_rounds_user'] = f"""
    SELECT '{bot_id}' AS prompt_id,
           COUNT(*) AS total_chat_rounds,
           COUNT(DISTINCT user_id) AS total_user_cnt,
           ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS avg_chat_rounds_per_user
    FROM flow_event_info.tbl_app_event_chat_send
    WHERE prompt_id = '{bot_id}' AND event_date >= '{sticky_start}' AND event_date < '{sticky_end}';
    """

    sql['avg_retention_days'] = f"""
    WITH d0_users AS (
      SELECT user_id, MIN(event_date) AS first_date
      FROM flow_event_info.tbl_app_event_bot_view
      WHERE bot_id = '{bot_id}' AND event_date >= '{sticky_start}' AND event_date < DATE_ADD('{sticky_start}', INTERVAL 4 DAY)
      GROUP BY user_id
    ),
    future_activity AS (
      SELECT d.user_id, DATEDIFF(v.event_date, d.first_date) AS day_gap
      FROM d0_users d
      JOIN flow_event_info.tbl_app_event_bot_view v
        ON d.user_id = v.user_id AND v.bot_id = '{bot_id}'
      WHERE v.event_date > d.first_date AND v.event_date <= DATE_ADD(d.first_date, INTERVAL 14 DAY)
        AND v.event_date >= '{sticky_start}' AND v.event_date < DATE_ADD('{sticky_end}', INTERVAL 6 DAY)
    ),
    user_retention_days AS (
      SELECT user_id, COUNT(DISTINCT day_gap) AS retained_days
      FROM future_activity
      GROUP BY user_id
    )
    SELECT '{bot_id}' AS prompt_id,
           COUNT(*) AS user_cnt,
           ROUND(AVG(retained_days), 2) AS avg_retention_days
    FROM user_retention_days;
    """

    return sql

def run_sql(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

def is_valid_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def is_safe_identifier(text: str) -> bool:
    return bool(re.match(r'^[a-zA-Z0-9_\-]+$', text))

def analyze_bot_indicators(bot_id: str, date='2025-02-15', sticky_start='2025-02-01', sticky_end='2025-02-15') -> pd.DataFrame:
    print(f"\n🔍 正在分析 bot: {bot_id}...")

    # 参数校验
    for label, val in {'date': date, 'sticky_start': sticky_start, 'sticky_end': sticky_end}.items():
        if not is_valid_date(val):
            raise ValueError(f"❌ 参数错误: {label} 格式应为 YYYY-MM-DD，但收到: {val}")
    if not is_safe_identifier(bot_id):
        raise ValueError(f"❌ bot_id 包含非法字符: {bot_id}")

    engine = get_engine()
    sqls = generate_bot_quality_sql(bot_id, date, sticky_start, sticky_end)
    results = {}

    for name, sql in sqls.items():
        try:
            print(f"➡️ 运行指标 [{name}] 的 SQL...")
            df = run_sql(engine, sql)
            print(f"✅ {name} 查询成功，结果行数: {len(df)}")
            if 'prompt_id' in df.columns and not df.empty:
                df = df.set_index("prompt_id")
                for col in df.columns:
                    results[f"{name}_{col}"] = df.iloc[0][col]
            else:
                print(f"⚠️ {name} 查询返回空结果。")
                for col in ['click_user_cnt', 'show_user_cnt', 'chat_user_cnt', 'total_chat_rounds', 'sticky_score', 'avg_retention_days']:
                    results[f"{name}_{col}"] = None
        except Exception as e:
            print(f"❌ {name} 查询失败: {e}")
            results[f"{name}_error"] = str(e)

    results['prompt_id'] = bot_id
    return pd.DataFrame([results])

def normalize_and_score(df: pd.DataFrame, weights: dict = None) -> pd.DataFrame:
    if weights is None:
        weights = {
            "click_rate_click_rate": 0.1,
            "chat_start_rate_chat_start_rate": 0.1,
            "avg_chat_rounds_avg_chat_rounds": 0.2,
            "sticky_score_sticky_score": 0.2,
            "avg_chat_rounds_user_avg_chat_rounds_per_user": 0.2,
            "avg_retention_days_avg_retention_days": 0.2
        }

    def min_max(series):
        if series.nunique() <= 1:
            return pd.Series([0.5] * len(series), index=series.index)
        return (series - series.min()) / (series.max() - series.min())

    print("\n📊 开始归一化处理并计算加权得分...")
    for metric in weights:
        if metric in df.columns:
            df[f"{metric}_norm"] = min_max(df[metric])

    df["final_score"] = sum(
        df[f"{metric}_norm"] * weight
        for metric, weight in weights.items()
        if f"{metric}_norm" in df.columns
    )

    df["final_score"] = df["final_score"].fillna(0).infer_objects(copy=False)
    df["final_score"] = df["final_score"].apply(lambda x: max(x, 0.01))
    df["rank"] = df["final_score"].rank(ascending=False, method="min")
    print("✅ 得分计算完成。\n")
    return df


def is_valid_date(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def is_safe_identifier(text: str) -> bool:
    return bool(re.match(r'^[a-zA-Z0-9_\-]+$', text))

# ========== 查询执行 ==========
def run_sql(engine, sql: str) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

def analyze_bot_indicators(bot_id: str, date: str, sticky_start: str, sticky_end: str) -> pd.DataFrame:
    print(f"\n🔍 正在分析 bot: {bot_id}...")
    for label, val in {'date': date, 'sticky_start': sticky_start, 'sticky_end': sticky_end}.items():
        if not is_valid_date(val):
            raise ValueError(f"❌ 参数错误: {label} 格式应为 YYYY-MM-DD，但收到: {val}")
    if not is_safe_identifier(bot_id):
        raise ValueError(f"❌ bot_id 包含非法字符: {bot_id}")

    engine = get_engine()
    sqls = generate_bot_quality_sql(bot_id, date, sticky_start, sticky_end)
    results = {}

    for name, sql in sqls.items():
        try:
            print(f"➡️ 运行指标 [{name}] 的 SQL...")
            df = run_sql(engine, sql)
            print(f"✅ {name} 查询成功，结果行数: {len(df)}")
            if 'prompt_id' in df.columns and not df.empty:
                df = df.set_index("prompt_id")
                for col in df.columns:
                    results[f"{name}_{col}"] = df.iloc[0][col]
            else:
                for col in ['click_user_cnt', 'show_user_cnt', 'chat_user_cnt', 'total_chat_rounds', 'sticky_score', 'avg_retention_days']:
                    results[f"{name}_{col}"] = None
        except Exception as e:
            print(f"❌ {name} 查询失败: {e}")
            results[f"{name}_error"] = str(e)

    results['prompt_id'] = bot_id
    return pd.DataFrame([results])

if __name__ == "__main__":
    # 分析日期
    target_date = "2025-04-15"
    #前n个bot质量分析
    top_n = 20

    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    sticky_start = (target_dt - timedelta(days=14)).strftime("%Y-%m-%d")
    sticky_end = (target_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 分析设置: target_date={target_date}, sticky_start={sticky_start}, sticky_end={sticky_end}")

    bot_groups = get_top_bot_ids_by_type(target_date, top_n)
    print("🚀 开始批量分析 bot...")

    all_results = []
    for group_type, bot_list in bot_groups.items():
        for bot_id in bot_list:
            try:
                df = analyze_bot_indicators(bot_id, date=target_date, sticky_start=sticky_start, sticky_end=sticky_end)
                df["bot_type"] = group_type
                all_results.append(df)
            except Exception as e:
                print(f"❌ 分析 bot {bot_id} 失败: {e}")


    if all_results:
        all_df = pd.concat(all_results, ignore_index=True)
        print("📋 所有 bot 分析结果数量：", all_df.shape)

        scored_df = normalize_and_score(all_df)

        sora_df = scored_df[scored_df["bot_type"] == "sora"]
        non_sora_df = scored_df[scored_df["bot_type"] == "non_sora"]

        sora_df.to_csv("sora_bot_quality.csv", index=False)
        non_sora_df.to_csv("non_sora_bot_quality.csv", index=False)
        print("📁 已分别保存 sora 和非 sora 的评分结果。")

        # 🎨 绘图：Sora vs 非 Sora 平均得分对比
        plt.figure(figsize=(6, 4))
        plt.bar(["Sora", "非 Sora"], [sora_df["final_score"].mean(), non_sora_df["final_score"].mean()], color=["#4c9be8", "#f1948a"])
        plt.title("Sora vs 非 Sora Bot 平均质量评分对比")
        plt.ylabel("平均得分")
        plt.ylim(0, 1)
        plt.tight_layout()
        plt.savefig("bot_quality_comparison.png")
        plt.show()
    else:
        print("⚠️ 所有分析都失败，没有生成评分结果。")