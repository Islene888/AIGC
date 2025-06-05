from AIGC.AIGC.data_analsis.tag import three_metrics_by_tag
from AIGC.AIGC.data_analsis.workflow import click_rate, active_rate, chat_depth
from AIGC.AIGC.workflow_insert import insert_luna
from AIGC.comparation.tag import all_tag, insert_chat_depth_by_tag
from AIGC.comparation.workflow import char_round, chat_start, click

def run_all(start_date, end_date):
    print(f"任务启动，区间：{start_date} ~ {end_date}")
    insert_luna.main(start_date, end_date)
    click_rate.main(start_date, end_date)
    active_rate.main(start_date, end_date)
    chat_depth.main(start_date, end_date)
    three_metrics_by_tag.main(start_date, end_date)
    char_round.main(start_date, end_date)
    chat_start.main(start_date, end_date)
    click.main(start_date, end_date)
    all_tag.main(start_date, end_date)
    insert_chat_depth_by_tag.main(start_date, end_date)
    print("全部任务执行完毕！")

if __name__ == "__main__":
    # 自定义日期区间
    start_date = "2025-06-05"
    end_date = "2025-06-05"
    run_all(start_date, end_date)
