# @Time : 03/28/2024 09:14 AM
# @Author : lubinsu
# @File : cron_main.py
# @desc : 定时调度扫描

import subprocess
from datetime import *

import croniter
import requests
from pymysql import InterfaceError

from distribution.mysql_distributed_lock import mysql_distributed_lock
from utils import *


def CronRunCurrentTime(now, sched):
    cron = croniter.croniter(sched, now + datetime.timedelta(minutes=-1))
    return cron.get_next(datetime.datetime).strftime("%Y-%m-%d %H:%M")


def exec_jobs(cron):
    if cron.cron_type == "shell":
        # os.system(cron.jobs_name)
        subprocess.run(cron.jobs_name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    elif cron.cron_type == "pyjobs":
        url = 'http://127.0.0.1:8383/runjob/{}'.format(cron.jobs_name)
        requests.get(url)
    logging.info("crontab任务结束：{}".format(cron.jobs_name))


def task(cron):
    logging.info("crontab任务开始：{}".format(cron.jobs_name))
    db = None
    try:
        if cron.is_concurrency == "N":
            # try:
            db = Database(**cron.db_config)
            conn = db.getConnection()
            lock = mysql_distributed_lock(conn, cron.jobs_name, timeout=2)
            if lock.acquire():
                try:
                    exec_jobs(cron)
                finally:
                    lock.release()
            else:
                logging.info("任务重复执行，已退出, {}".format(cron.jobs_name))

        else:
            exec_jobs(cron)

    except Exception:
        logging.error("{}执行异常, {}".format(cron.jobs_name, traceback.format_exc()))
    finally:
        # if lock is not None:
        #     try:
        #         lock.release()
        #     # 报错则重新连接释放
        #     except InterfaceError:
        #         conn = db.getConnection()
        #         lock = mysql_distributed_lock(conn, cron.jobs_name, timeout=2)
        #         lock.release()
        if db is not None:
            db.close()
