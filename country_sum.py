import os
import pandas as pd

# ========== 配置 ==========
FOLDER = "daily_retention"
EXPERIMENT_ID = "app_sensitive_image_exp"  # 可自定义
OUTPUT_FILE = f"retention_summary_{EXPERIMENT_ID}.csv"

# ========== 加载所有 CSV ==========
def aggregate_retention_data(folder):
    all_files = [f for f in os.listdir(folder) if f.endswith(".csv")]
    print(f"📂 找到 {len(all_files)} 个留存文件")

    all_dfs = []
    for file in sorted(all_files):  # 按日期顺序排序
        file_path = os.path.join(folder, file)
        try:
            df = pd.read_csv(file_path)
            all_dfs.append(df)
        except Exception as e:
            print(f"❌ 加载失败: {file_path}，错误: {e}")

    if not all_dfs:
        print("⚠️ 没有加载到任何数据")
        return pd.DataFrame()

    result_df = pd.concat(all_dfs, ignore_index=True)
    return result_df

# ========== 主流程 ==========
if __name__ == "__main__":
    df_all = aggregate_retention_data(FOLDER)

    if not df_all.empty:
        df_all["retention_d1"] = df_all["d1"] / df_all["active_users"]
        df_all = df_all.sort_values(["dt", "variation", "country"])

        output_path = os.path.join(FOLDER, OUTPUT_FILE)
        df_all.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"\n✅ 聚合完成，已保存到：{output_path}")
        print(df_all.head())
    else:
        print("❌ 聚合失败，无数据")
