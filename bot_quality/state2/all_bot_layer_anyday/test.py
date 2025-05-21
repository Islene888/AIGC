import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime, timedelta
from typing import List, Dict

from bot_quality.state2.all_bot_layer_anyday.bot_any_date_layer import normalize_and_score
from bot_quality.state2.sora_all_bot.anyday.sora_any_day_top10 import analyze_bot_indicators


# ========== 数据库连接 ==========
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    )

# ========== 获取每日分层 Bot 和 AI Bot 列表 ==========
def get_bot_lists(event_date: str) -> Dict[str, pd.DataFrame]:
    engine = get_engine()

    stratified_query = f"""
        SELECT prompt_id, layer_name
        FROM tbl_bot_stratified_sample_10layers
        WHERE event_date = '{event_date}'
    """
    ai_query = f"""
        SELECT prompt_id
        FROM tbl_top5000_sora_bots_daily
        WHERE event_date = '{event_date}'
    """

    df_stratified = pd.read_sql(stratified_query, engine)
    df_ai = pd.read_sql(ai_query, engine)
    return {"stratified": df_stratified, "ai": df_ai}

# ========== 匹配 AI Bot 到最相近的层级 ==========
def match_ai_to_layer(event_date: str,
                      indicators: List[str],
                      weights: Dict[str, float]):
    bot_lists = get_bot_lists(event_date)
    df_stratified = bot_lists["stratified"]
    df_ai = bot_lists["ai"]
    sticky_start = (datetime.strptime(event_date, "%Y-%m-%d") - timedelta(days=14)).strftime("%Y-%m-%d")
    sticky_end = (datetime.strptime(event_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    all_ids = pd.concat([df_stratified["prompt_id"], df_ai["prompt_id"]]).drop_duplicates()
    print(f"共需分析 Bot 数量: {len(all_ids)}")

    results = []
    for bot_id in all_ids:
        df = analyze_bot_indicators(bot_id, event_date, sticky_start, sticky_end)
        results.append(df)

    all_results_df = pd.concat(results, ignore_index=True)
    scored_df = normalize_and_score(all_results_df, weights)

    # 添加层级信息
    scored_df = scored_df.merge(df_stratified, on="prompt_id", how="left")

    # 分层平均分
    layer_score = scored_df[~scored_df["layer_name"].isna()].groupby("layer_name")["final_score"].mean().to_dict()

    # 匹配 AI Bot 所属层
    def match_layer(score):
        return min(layer_score.items(), key=lambda x: abs(x[1] - score))[0]

    df_ai_scored = scored_df[scored_df["prompt_id"].isin(df_ai["prompt_id"])]
    df_ai_scored["matched_layer"] = df_ai_scored["final_score"].apply(match_layer)

    df_ai_scored.to_csv(f"ai_bot_layer_match_{event_date}.csv", index=False)
    print(f"✅ 输出完成：ai_bot_layer_match_{event_date}.csv")

# ========== 示例入口 ==========
if __name__ == "__main__":
    match_ai_to_layer(
        event_date="2025-04-02",
        indicators=[
            "click_rate", "chat_start_rate", "avg_chat_rounds",
            "sticky_score", "avg_chat_rounds_user", "avg_retention_days"
        ],
        weights={
            "click_rate_click_rate": 0.1,
            "chat_start_rate_chat_start_rate": 0.1,
            "avg_chat_rounds_avg_chat_rounds": 0.2,
            "sticky_score_sticky_score": 0.2,
            "avg_chat_rounds_user_avg_chat_rounds_per_user": 0.2,
            "avg_retention_days_avg_retention_days": 0.2
        }
    )
