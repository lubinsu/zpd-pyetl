# @Time : 7/07/2021 09:14 AM
# @Author : lubinsu
# @File : main.py
# @desc : REST服务主程序，启动：python2 rest.py，可通过脚本启动：rest_monitor.sh 或者 rest_restart.sh
# REST服务调用方式示例：
# 状态校验: curl --connect-timeout 5 -m 5 "http://127.0.0.1:8383/check_rest_status/"
# 流程调用: curl --connect-timeout 5 "http://127.0.0.1:8383/runjob/invoke_proc,log_write_back"

from gevent import monkey
from gevent.pywsgi import WSGIServer
import sys

monkey.patch_all()

from crons.cron_task import *
from crons.CronTab import CronTab
import threading

from logging.handlers import RotatingFileHandler
from xml.dom.minidom import parse
from datetime import *
import time

from multiprocessing import cpu_count, Process

from utils import *
from flask import Flask, session
from threading import Lock

app = Flask(__name__)
app.secret_key = 'zpd_secret_key_xxxx'

# logging.basicConfig(level=logging.INFO, filemode='a')

# 所有数据库连接
# databases = {}
# 所有job
jobs = {}
# 系统参数
sysParams = {}
db_document = None
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


def log_config_db(db, log_levels):
    configdb_str = 'mysql+pymysql://%s:%s@%s:%d/%s?charset=utf8' % (
        db.user, db.password, db.host, int(db.port), db.db)

    formatter = logging.Formatter(
        '%(asctime)s-[line:%(lineno)d]-[%(job_chain)s]-[%(current_job)s]-%(levelname)s:%(message)s')
    formatter2 = logging.Formatter(
        '%(asctime)s-[line:%(lineno)d]-%(levelname)s:%(message)s')
    from zpd_logging.handler.logger_handler_to_mysql import LoggerHandlerToMysql
    handler = LoggerHandlerToMysql(configdb_str, formatter, "job_chain",
                                   "{}_{}".format(time.strftime("%H%M", time.localtime()), os.getpid()))
    handler.setLevel(log_levels["db_level"])
    handler.setFormatter(formatter)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(log_levels["console_level"])
    console.setFormatter(formatter2)

    logger = logging.getLogger()
    logger.setLevel("DEBUG")
    logger.addHandler(handler)

    logger.addHandler(console)


@app.route('/')
def hello_world():
    return "欢迎使用中普达ETL服务"


@app.route('/check_rest_status/')
def check_rest_status():
    lock.acquire()
    db_status = get_db_config(db_document)
    status_code = check_status(db_status)
    lock.release()

    return status_code


@app.route('/run_cron/')
def run_cron():
    # 读取配置
    db_dom = parse("{}/resources/db.xml".format(sys.path[0]))
    db_document = db_dom.documentElement

    lock.acquire()
    db = get_db_config(db_document)
    # log_levels = get_log_level(db_document)
    # log_config_db(db, log_levels)
    lock.release()

    cron_list = etl.fromdb(db.getConnection(),
                           "SELECT A.ID, A.IS_CONCURRENCY, A.CRON, A.JOBS_NAME, A.CRON_TYPE"
                           " FROM py_crontabs A "
                           " WHERE STATE = 'Y'")

    logging.info("异步执行crontab任务开始")
    threads = []

    try:
        now = datetime.datetime.now()
        for item in etl.dicts(cron_list):

            try:

                cron = CronTab(item['ID'], item['CRON'], item['JOBS_NAME'], item['CRON_TYPE'], item['IS_CONCURRENCY'])

                # 验证cron表达式
                if croniter.croniter.is_valid(cron.cron):

                    now = datetime.datetime.now()
                    nearest = CronRunCurrentTime(now, cron.cron)
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
            except Exception as e:
                logging.error(traceback.format_exc())
        for t in threads:
            t.start()

        for t in threads:
            t.join()  # 等待所有线程完成

        logging.info("异步执行crontab任务结束")
    except Exception as e:
        logging.error(traceback.format_exc())
        if db is not None:
            db.close()
        return "异步执行异常"
    finally:
        if db is not None:
            db.close()
    return "异步执行结束"


@app.route('/runjob/<all_jobs>')
def runjobs(all_jobs):
    # print(all_jobs)
    return run_job(all_jobs)


def run_job(all_jobs):
    databases = {}
    db_meta = None
    exit_val = 0
    try:
        job_chain = all_jobs.replace(",", "->")
        session["job_chain"] = job_chain
        session["current_job"] = job_chain
        session["timestmp"] = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S.%f'))
        # print("session[\"job_chain\"]:{}".format(session["job_chain"]))
        # print("session[\"current_job\"]:{}".format(session["current_job"]))
        # 如果是通过数据库配置的，则先读取数据库连接
        # logConfig(sys.argv[2])
        for hdlr in logging.getLogger().handlers:
            if hdlr.__class__.__name__ == "LoggerHandlerToMysql":
                hdlr.setJobChain(job_chain)

        lock.acquire()

        db_meta = get_db_config(db_document)

        try:
            del_log(db_meta)
        except:
            logging.warning("删除历史日志出错。")
        logging.info("正在读取数据库配置...")
        get_databases_by_db_rest(db_meta, databases)
        jobs = get_jobs_by_db(db_meta, job_chain)
        lock.release()

        logging.info("正在执行任务：{}".format(job_chain))

        for job in all_jobs.split(","):
            if job in jobs:
                session['current_job'] = job
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
                        logging.error("未匹配到字段: {}".format(e))
                    except Exception as e:
                        logging.error(traceback.format_exc())
                else:
                    jobs[job].execute(databases)
            else:
                # 如果JOB不存在或者状态为N，不再退出，仅仅进行错误告警
                logging.error("job不存在：{}".format(job))

        logging.info("任务结束：{}".format(job_chain))
        exit_val = 0
        # return "任务结束：{}".format(job_chain)

    except cx_Oracle.DatabaseError as e:
        logging.error("数据库异常: {}".format(e))
        exit_val = 1
    except pymysql.DatabaseError as e:
        logging.error("数据库异常: {}".format(e))
        exit_val = 1
    except pymssql.DatabaseError as e:
        logging.error("数据库异常: {}".format(e))
        exit_val = 1
    except KeyError as e:
        logging.error("未匹配到字段: {}".format(e))
        exit_val = 1
    except Exception as e:
        logging.error(traceback.format_exc())
        exit_val = 1

    finally:
        for key in databases:
            # print("关闭数据库：{}".format(key))
            databases[key].close()
        if db_meta is not None:
            db_meta.close()
        # exit(exit_val)
        return "任务结束：{}".format(job_chain)


def run(MULTI_PROCESS, proc_count=1):
    if not MULTI_PROCESS:
        WSGIServer(('0.0.0.0', 8383), app).serve_forever()
    else:
        multi_server = WSGIServer(('0.0.0.0', 8383), app)
        multi_server.start()

        def server_forever():
            multi_server.start_accepting()
            try:
                multi_server._stop_event.wait()
            except:
                raise

        print(cpu_count())
        # 根据CPU数量，起线程，可以进行修改。
        for i in range(proc_count):
            p = Process(target=server_forever)
            p.start()


if __name__ == '__main__':

    try:

        db_dom = parse("{}/resources/db.xml".format(sys.path[0]))
        db_document = db_dom.documentElement
        db = get_db_config(db_document)
        log_levels = get_log_level(db_document)
        process_count = get_process_count(db_document)
        log_config_db(db, log_levels)
        lock = Lock()

        # app.run(host="0.0.0.0", port=8383, debug=False)
        # Windows下测试时，用单进程即可
        # run(False)
        run(True, process_count)
        # server = pywsgi.WSGIServer(('0.0.0.0', 8383), app)
        logging.warning("欢迎使用中普达ETL服务")
        # server.serve_forever()

        db.close()
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
