# @Time : 03/28/2024 09:14 AM
# @Author : lubinsu
# @File : cron_main.py
# @desc : 定时调度扫描

import sys
import main
from datetime import *
from xml.dom.minidom import parse

from CronTab import CronTab
from rest import log_config_db
from zpd_logging.handler.logger_handler_to_mysql import LoggerHandlerToMysql
import requests
import threading
import subprocess

import croniter

from utils import *


def CronRunCurrentTime(sched):
    cron = croniter.croniter(sched, datetime.datetime.now() + datetime.timedelta(minutes=-1))
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


if __name__ == '__main__':
    # 读取配置
    db_dom = parse("{}/resources/db.xml".format(sys.path[0]))
    db_document = db_dom.documentElement
    db = get_db_config(db_document)
    log_levels = get_log_level(db_document)
    log_config_db(db, log_levels)

    cron_list = etl.fromdb(db.getConnection(),
                           "SELECT A.CRON, A.JOBS_NAME, A.CRON_TYPE"
                           " FROM py_crontabs A "
                           " WHERE STATE = 'Y'")

    logging.info("异步执行crontab任务开始")
    threads = []

    try:
        for item in etl.dicts(cron_list):

            cron = CronTab(item['CRON'], item['JOBS_NAME'], item['CRON_TYPE'])

            # 验证cron表达式
            if croniter.croniter.is_valid(cron.cron):

                now = datetime.datetime.now()
                nearest = CronRunCurrentTime(cron.cron)
                now_str = now.strftime("%Y-%m-%d %H:%M")
                # print("当前时间", now_str)
                # print("最近的时间", nearest)

                # 如果满足执行要求，则开始执行
                if now_str == nearest:
                    t = threading.Thread(target=task, args=(cron,))
                    # print("提交完成：", datetime.datetime.now())
                    threads.append(t)
                    # t.start()
            else:
                # 验证cron表达式
                logging.error("cron表达式校验异常：{}".format(cron.cron))
        for t in threads:
            t.start()

        for t in threads:
            t.join()  # 等待所有线程完成

        logging.info("异步执行crontab任务结束")
    except Exception as e:
        logging.error(traceback.format_exc())
