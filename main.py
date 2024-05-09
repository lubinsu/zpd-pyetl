# @Time : 7/07/2021 09:14 AM
# @Author : lubinsu
# @File : main.py
# @desc : 主程序，添加任务
# 动态参数测试测试用例：python36 main.py sys_dynamic-config-template.xml p_truncate_quick_tbs,动态参数测试
# 数据库配置方式调用示例：python36 main.py db 数据同步测试_PostgreSQL

import os
import sys
import traceback
from logging.handlers import RotatingFileHandler
from xml.dom.minidom import parse
from datetime import *

import time

from utils import *
import argparse

# logging.basicConfig(level=logging.INFO, filemode='a')

# 所有数据库连接
from zpd_logging.handler.logger_handler_to_mysql import LoggerHandlerToMysql

databases = {}
# 所有job
jobs = {}
# 系统参数
sysParams = {}

etl_base = None


# 加载日志配置
def logConfig(logName):
    file_log_handler = RotatingFileHandler(
        "{}/log/{}".format(sys.path[0], "{}.log".format(logName.replace('.xml', ''))),
        maxBytes=1024 * 1024 * 100,
        backupCount=5)
    formatter = logging.Formatter('%(asctime)s - [line:%(lineno)d] - %(levelname)s: %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)


def log_config_db(db, job_chain, log_levels):
    configdb_str = 'mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8' % (
        db.user, db.password, db.host, int(db.port), db.db)

    formatter = logging.Formatter(
        '%(asctime)s-[line:%(lineno)d]-[%(job_chain)s]-[%(current_job)s]-%(levelname)s:%(message)s')
    formatter2 = logging.Formatter(
        '%(asctime)s-[line:%(lineno)d]-%(levelname)s:%(message)s')
    handler = LoggerHandlerToMysql(configdb_str, formatter, job_chain,
                                   "{}_{}".format(time.strftime("%H%M", time.localtime()), os.getpid()))
    handler.setLevel(log_levels["db_level"])
    handler.setFormatter(formatter)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(log_levels["console_level"])
    console.setFormatter(formatter2)

    logger = logging.getLogger()
    logger.setLevel("DEBUG")
    logger.addHandler(handler)
    #
    # chlr = logging.StreamHandler(sys.stdout)
    # handler.setLevel(logging.INFO)

    # 创建一个格式器formatter并将其添加到处理器handler
    # formatter2 = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # chlr.setFormatter(formatter2)

    # 为日志器logger添加上面创建的处理器handler
    logger.addHandler(console)


if __name__ == '__main__':

    try:
        # job_chain = "->".join(map(lambda x: "{}..".format(x[0: 10]), sys.argv[2].split(",")))
        job_chain = sys.argv[2].replace(",", "->")
        parser = argparse.ArgumentParser(usage="usage: main.py db jobname1,jobname2 [-h] [--create=true]")
        parser.add_argument("--create", help="抽取过程中，是否自动建表")
        args_create = None
        if len(sys.argv[3:]) >= 1:
            args = parser.parse_args(sys.argv[3:])
            args_create = args.create.upper()

        if sys.argv[1] == "db":
            # 如果是通过数据库配置的，则先读取数据库连接
            # logConfig(sys.argv[2])
            db_dom = parse("{}/resources/db.xml".format(sys.path[0]))
            db_document = db_dom.documentElement
            db = get_db_config(db_document)
            log_levels = get_log_level(db_document)
            log_config_db(db, job_chain, log_levels)
            try:
                del_log(db)
            except:
                logging.warning("删除历史日志出错。")
            logging.info("正在读取数据库配置...")
            databases = get_databases_by_db(db)

            jobs = get_jobs_by_db(db, job_chain)

            db.close()

            logging.info("正在执行任务：{}".format(job_chain))
        else:
            logConfig(sys.argv[1])
            dom = parse("{}/resources/{}".format(sys.path[0], sys.argv[1]))
            document = dom.documentElement
            logging.info("正在执行任务：{}".format(sys.argv[1]))
            databases = get_databases(document)
            jobs = getJobs(document)

        for job in sys.argv[2].split(","):
            # execute(job)
            if job in jobs:
                if args_create is not None and args_create == "TRUE":
                    jobs[job].target[0].set_create(True)

                if jobs[job].jobParams.is_fail_continue == 'Y':

                    try:
                        jobs[job].execute(databases)
                    except cx_Oracle.DatabaseError as e:
                        logging.error("数据库异常: {}".format(e))
                    except pymysql.DatabaseError as e:
                        logging.error("数据库异常: {}".format(e))
                    except pymssql.DatabaseError as e:
                        logging.error("数据库异常: {}".format(e))
                    except KeyError as e:
                        logging.error(traceback.format_exc())
                        logging.error("未匹配到字段: {}".format(e))
                    except Exception as e:
                        logging.error(traceback.format_exc())
                else:
                    jobs[job].execute(databases)
            else:
                # 如果JOB不存在或者状态为N，不再退出，仅仅进行错误告警
                logging.error("job不存在：{}".format(job))

        for key in databases:
            # print("关闭数据库：{}".format(key))
            databases[key].close()

        logging.info("任务结束：{}".format(job_chain))
        exit(0)
    except cx_Oracle.DatabaseError as e:
        logging.error("数据库异常: {}".format(e))
        exit(1)
    except pymysql.DatabaseError as e:
        logging.error("数据库异常: {}".format(e))
        exit(1)
    except pymssql.DatabaseError as e:
        logging.error("数据库异常: {}".format(e))
        exit(1)
    except KeyError as e:
        logging.error("未匹配到字段: {}".format(e))
        exit(1)
    except Exception as e:
        logging.error(traceback.format_exc())
        exit(1)
