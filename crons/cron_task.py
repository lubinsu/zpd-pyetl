# @Time : 03/28/2024 09:14 AM
# @Author : lubinsu
# @File : cron_main.py
# @desc : 定时调度扫描

import subprocess
from datetime import *

import croniter
import requests

from utils import *


def CronRunCurrentTime(now, sched):
    cron = croniter.croniter(sched, now + datetime.timedelta(minutes=-1))
    return cron.get_next(datetime.datetime).strftime("%Y-%m-%d %H:%M")


def task(cron):
    logging.info("crontab任务开始：{}".format(cron.jobs_name))
    try:
        if cron.cron_type == "shell":
            # os.system(cron.jobs_name)
            subprocess.run(cron.jobs_name, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        elif cron.cron_type == "pyjobs":
            url = 'http://127.0.0.1:8383/runjob/{}'.format(cron.jobs_name)
            requests.get(url)
            # if v_rst.status_code == 200:
            #     print(v_rst.text)
            # else:
            #     print('Failed to retrieve data, status code:', v_rst.status_code)
        logging.info("crontab任务结束：{}".format(cron.jobs_name))
    except Exception as e:
        logging.error("{}执行异常, {}".format(cron.jobs_name, traceback.format_exc()))
