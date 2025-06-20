import pandas as pd
import urllib
from sqlalchemy import create_engine

# 1. 读取 CSV 文件
df = pd.read_csv('workflow2.csv')  # 确保包含 'id' 和 'workflow' 两列
df = df.rename(columns={'id': 'prompt_id', 'workflow': 'tag'})
df = df[['prompt_id', 'tag']].dropna()

# 增加 event_date 字段
df['event_date'] = '2025-05-10'

# 2. 建立数据库连接
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")
    return create_engine(
        f"mysql+pymysql://flowgptzmy:{password}@3.135.224.186:9030/flow_wide_info?charset=utf8mb4"
    )

engine = get_engine()

# 3. 写入数据库
try:
    df.to_sql(
        name='AIGC_prompt_tag',
        con=engine,
        if_exists='append',   # or 'replace'
        index=False,
        method='multi'
    )
    print("✅ 数据已成功导入到 AIGC_prompt_tag 表中")
except Exception as e:
    print(f"❌ 数据导入失败，错误原因：{e}")
