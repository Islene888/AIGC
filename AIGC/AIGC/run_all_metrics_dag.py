import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pathlib import Path
print("🧠 DAG 文件加载成功")

# ✅ 添加你的项目根路径（不是 AIGC/AIGC，是它的上层）
sys.path.append("/Users/islenezhao/PythonProject/PythonProject2")

# ✅ 正确导入模块（模块是 AIGC.AIGC.run_all_metrics）
from AIGC.AIGC import run_all_metrics

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='run_all_metrics_daily',
    default_args=default_args,
    description='每日运行 AIGC 指标分析',
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 5, 20),
    catchup=False,
    tags=['aigc'],
) as dag:

    def run_yesterday(**kwargs):
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")
        run_all_metrics.run_all(date_str, date_str)

    run_task = PythonOperator(
        task_id='run_all_metrics_task',
        python_callable=run_yesterday,
    )
