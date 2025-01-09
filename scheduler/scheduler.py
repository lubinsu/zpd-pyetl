import traceback

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import logging

executors = {'default': ThreadPoolExecutor(10)}
scheduler = BackgroundScheduler(executors=executors)

def start_scheduler():
    """启动调度器"""
    if not scheduler.running:
        scheduler.add_job(periodic_task, 'cron', minute='*', max_instances=20, id='cron_task')
        scheduler.start()
        logging.info("全局调度器已启动")


def periodic_task():
    try:
        response = requests.get("http://127.0.0.1:8383/run_cron/")
        logging.info(f"run_cron任务执行完成，状态码: {response.status_code}")
    except Exception as e:
        logging.error(f"run_cron任务执行失败: {traceback.format_exc()}")