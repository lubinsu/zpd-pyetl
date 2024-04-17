# @Time : 03/28/2024 09:14 AM
# @Author : lubinsu
# @File : cron_main.py
# @desc : 定时调度扫描

import subprocess
from datetime import *
import fcntl

import croniter
import requests

from utils import *


def CronRunCurrentTime(now, sched):
    cron = croniter.croniter(sched, now + datetime.timedelta(minutes=-1))
    return cron.get_next(datetime.datetime).strftime("%Y-%m-%d %H:%M")


def exec_jobs(cron):
    if cron.cron_type == "shell":
        # os.system(cron.jobs_name)
        subprocess.run(cron.jobs_name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    elif cron.cron_type == "pyjobs":
        url = 'http://127.0.0.1:8383/runjob/{}'.format(cron.jobs_name)
        requests.get(url)
    logging.info("crontab任务结束：{}".format(cron.jobs_name))


def task(cron):
    logging.info("crontab任务开始：{}".format(cron.jobs_name))
    try:
        if cron.is_concurrency == "N":
            try:
                with open("/tmp/py_crontab_{}.lock".format(cron.id), "w") as f:
                    # 获取文件锁
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    # print("文件锁获取成功")
                    exec_jobs(cron)
                    # print("文件锁释放成功")
                    # 释放文件锁
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except BlockingIOError as e:
                logging.info("任务重复执行，已退出, {}".format(cron.jobs_name))
                # print("文件已被锁定")
        else:
            exec_jobs(cron)

    except Exception:
        logging.error("{}执行异常, {}".format(cron.jobs_name, traceback.format_exc()))
