import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from pathlib import Path

sys.path.append(str(Path("/Users/islenezhao/PythonProject/PythonProject2")))

from AIGC.AIGC.data_analsis.workflow import dianjilv, kailiaolv, liaotiaoshendu
from AIGC.AIGC.data_analsis.tag import by_tag_3_tables
from AIGC.comparation.workflow import char_round, chat_start, click
from AIGC.comparation.tag import all_tag, insert_chat_depth_by_tag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='run_all_metrics_parallel',
    default_args=default_args,
    description='将 AIGC 9 个模块任务并行调度执行',
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 5, 20),
    catchup=False,
    tags=['aigc', 'metrics'],
) as dag:

    def wrapper(func):
        def _wrapped(**kwargs):
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            logging.info(f"▶️ Running {func.__name__}({date}, {date})")
            func.main(date, date)
        return _wrapped

    # 定义 9 个任务
    t_dianjilv = PythonOperator(task_id='dianjilv', python_callable=wrapper(dianjilv))
    t_kailiaolv = PythonOperator(task_id='kailiaolv', python_callable=wrapper(kailiaolv))
    t_liaotiaoshendu = PythonOperator(task_id='liaotiaoshendu', python_callable=wrapper(liaotiaoshendu))
    t_by_tag = PythonOperator(task_id='by_tag_3_tables', python_callable=wrapper(by_tag_3_tables))
    t_click = PythonOperator(task_id='click_compare', python_callable=wrapper(click))
    t_chat_start = PythonOperator(task_id='chat_start_compare', python_callable=wrapper(chat_start))
    t_char_round = PythonOperator(task_id='char_round_compare', python_callable=wrapper(char_round))
    t_all_tag = PythonOperator(task_id='all_tag_summary', python_callable=wrapper(all_tag))
    t_insert_chat_depth = PythonOperator(task_id='insert_chat_depth_by_tag', python_callable=wrapper(insert_chat_depth_by_tag))

    # 任务依赖（可选）
    first_group = [t_dianjilv, t_kailiaolv, t_liaotiaoshendu, t_by_tag]
    second_group = [t_click, t_chat_start, t_char_round, t_all_tag, t_insert_chat_depth]

    for upstream in first_group:
        for downstream in second_group:
            upstream >> downstream

def run_all(start_date: str, end_date: str):
    logging.info(f"🧩 Running all metrics from {start_date} to {end_date}")
    dianjilv.main(start_date, end_date)
    kailiaolv.main(start_date, end_date)
    liaotiaoshendu.main(start_date, end_date)
    by_tag_3_tables.main(start_date, end_date)
    click.main(start_date, end_date)
    chat_start.main(start_date, end_date)
    char_round.main(start_date, end_date)
    all_tag.main(start_date, end_date)
    insert_chat_depth_by_tag.main(start_date, end_date)
