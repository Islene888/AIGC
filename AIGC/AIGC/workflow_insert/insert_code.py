import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text

# 1. 建立数据库连接
def get_engine():
    password = urllib.parse.quote_plus("GgJ34Q1aGTO7")  # 替换为你的真实密码
    return create_engine(
        "mysql+pymysql://flowgptzmy:{}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4".format(password)
    )

engine = get_engine()

# 2. 读取并写入 AIGC_prompt_tag 表（append 模式）
try:
    df = pd.read_csv("workflow2.csv")  # 确保包含 'id' 和 'workflow'
    df = df.rename(columns={"id": "prompt_id", "workflow": "tag"})
    df = df[["prompt_id", "tag"]].dropna()

    # 追加写入 AIGC_prompt_tag 表
    df.to_sql(
        name="AIGC_prompt_tag",
        con=engine,
        if_exists="append",  # ✅ 追加写入
        index=False,
        method="multi"
    )
    print("✅ CSV 数据已成功追加到 AIGC_prompt_tag 表中")
except Exception as e:
    print(f"❌ CSV 导入失败：{e}")
    exit()

# 3. 建表 + 插入 SQL 脚本
sql = """
-- 删除目标表
DROP TABLE IF EXISTS AIGC_prompt_tag_with_v5;

-- 创建目标表
CREATE TABLE AIGC_prompt_tag_with_v5 (
  prompt_id STRING,
  workflow STRING,
  tags STRING
)
ENGINE = OLAP
DUPLICATE KEY(prompt_id)
DISTRIBUTED BY HASH(prompt_id) BUCKETS 10
PROPERTIES (
  "replication_num" = "1",
  "compression" = "LZ4"
);

-- 插入合并数据
INSERT INTO AIGC_prompt_tag_with_v5 (prompt_id, workflow, tags)
SELECT 
    a.prompt_id,
    a.tag AS workflow,
    b.tag_values AS tags
FROM 
    AIGC_prompt_tag a
LEFT JOIN (
    SELECT 
        ptl.`"PromptId"` AS prompt_id,
        GROUP_CONCAT(REGEXP_REPLACE(pt.`"name"`, '^V5\\s+', '') SEPARATOR ', ') AS tag_values
    FROM 
        flow_rds_prod.tbl_wide_rds_prompt_tag_link ptl
    JOIN 
        flow_rds_prod.tbl_wide_rds_prompt_tag pt ON ptl.`"PromptTagId"` = pt.`"id"`
    JOIN 
        flow_rds_prod.tbl_wide_rds_prompt_tag_path ptp ON pt.`"pathId"` = ptp.`"id"`
    WHERE 
        ptp.`"path"` LIKE '%V5%'
    GROUP BY ptl.`"PromptId"`
) b ON a.prompt_id = b.prompt_id;
"""

# 4. 执行 SQL 脚本
try:
    with engine.connect() as conn:
        for statement in sql.strip().split(';'):
            if statement.strip():
                conn.execute(text(statement))
        print("✅ AIGC_prompt_tag_with_v5 表创建并插入成功")
except Exception as e:
    print(f"❌ SQL 执行失败：{e}")
