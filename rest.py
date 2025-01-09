# @Time : 7/07/2021 09:14 AM
# @Author : lubinsu
# @File : main.py
# @desc : REST服务主程序，启动：python3 rest.py，可通过脚本启动：rest_monitor.sh 或者 rest_restart.sh
# REST服务调用方式示例：
# 状态校验: curl --connect-timeout 5 -m 5 "http://127.0.0.1:8383/check_rest_status/"
# 流程调用: curl --connect-timeout 5 "http://127.0.0.1:8383/runjob/invoke_proc,log_write_back"
from gevent import monkey
monkey.patch_all()

import logging
from scheduler import *
from gevent.pywsgi import WSGIServer
import sys
import requests
import time

# from scripts.ce.pysynch import InvalidUsage



from crons.cron_task import *
from crons.CronTab import CronTab
import threading

from logging.handlers import RotatingFileHandler
from xml.dom.minidom import parse
from datetime import *
import time

from multiprocessing import cpu_count, Process

from utils import *
from flask import Flask, session, jsonify, request
from threading import Lock

app = Flask(__name__)
app.secret_key = 'zpd_secret_key_xxxx'

# logging.basicConfig(level=logging.INFO, filemode='a')

# 全局变量定义
# databases = {}  # 所有数据库连接（已注释）
jobs = {}  # 所有作业字典，存储作业名称与作业对象的映射
sysParams = {}  # 系统参数字典，存储系统配置参数
db_document = None  # 数据库配置文件DOM对象
etl_base = None  # ETL基础配置

def logConfig(logName):
    """
    配置日志文件处理程序
    
    参数:
        logName: 日志文件名（通常为XML配置文件名）
    
    功能:
        1. 创建RotatingFileHandler，用于日志文件轮转
        2. 设置日志格式
        3. 将处理程序添加到根日志记录器
        
    实现细节:
        - 日志文件存储在项目目录下的log子目录
        - 单个日志文件最大100MB
        - 最多保留5个备份日志文件
        - 日志格式包含时间、行号、日志级别和消息
    """
    file_log_handler = RotatingFileHandler(
        "{}/log/{}".format(sys.path[0], "{}.log".format(logName.replace('.xml', ''))),
        maxBytes=1024 * 1024 * 100,
        backupCount=5)
    formatter = logging.Formatter('%(asctime)s - [line:%(lineno)d] - %(levelname)s: %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)


def log_config_db(db, log_levels):
    """
    配置数据库日志处理程序
    
    参数:
        db: 数据库连接配置对象
        log_levels: 日志级别配置字典
        
    功能:
        1. 创建MySQL日志处理器
        2. 配置控制台日志处理器
        3. 设置日志格式
        4. 将处理器添加到根日志记录器
        
    实现细节:
        - 使用LoggerHandlerToMysql将日志写入数据库
        - 同时输出日志到控制台
        - 日志格式包含时间、行号、作业链、当前作业和日志级别
        - 支持不同的日志级别配置（数据库和控制台）
    """
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
    """
    欢迎页面路由
    
    路径: 
        / (根路径)
        
    返回:
        欢迎使用中普达ETL服务的字符串
        
    功能:
        提供REST服务的欢迎页面，用于验证服务是否正常运行
    """
    return "欢迎使用中普达ETL服务"


@app.route('/check_rest_status/')
def check_rest_status():
    """
    检查REST服务状态的接口
    
    路径:
        /check_rest_status/
        
    功能:
        1. 获取数据库连接
        2. 检查数据库连接状态
        3. 返回服务状态码
        
    返回:
        服务状态码字符串
        
    实现细节:
        - 使用锁机制保证线程安全
        - 自动释放数据库连接
        - 返回的状态码可用于监控服务健康状况
    """
    db_status = None
    try:
        lock.acquire()
        db_status = get_db_config(db_document)
        status_code = check_status(db_status)
    finally:
        lock.release()
        if db_status:
            db_status.close()
    return status_code

@app.route('/check_scheduler_jobs', methods=['GET'])
def check_scheduler_jobs():
    """
    检查调度器任务状态
    
    路径:
        /check_scheduler_jobs (GET请求)
        
    功能:
        1. 检查调度器是否正在运行
        2. 获取所有调度任务
        3. 格式化任务信息
        4. 返回任务列表
        
    返回:
        JSON格式响应:
        - code: 状态码 (1: 成功, -1: 失败)
        - msg: 状态信息
        - data: 任务列表，包含任务ID和下次运行时间
        
    实现细节:
        - 使用scheduler.scheduler获取任务信息
        - 格式化时间为'%Y-%m-%d %H:%M:%S'格式
        - 返回标准化的JSON响应
    """
    if scheduler.scheduler.running:
        jobs = scheduler.scheduler.get_jobs()
        job_list = [{'id': job.id, 'next_run_time': job.next_run_time.strftime('%Y-%m-%d %H:%M:%S')} for job in jobs]
        return jsonify({'code': 1, 'msg': '成功获取任务列表', 'data': job_list})
    return jsonify({'code': -1, 'msg': '调度器未运行', 'data': None})


@app.route('/run_cron/')
def run_cron():
    """
    执行定时任务的接口
    
    路径:
        /run_cron/
        
    功能:
        1. 从数据库读取定时任务配置
        2. 验证cron表达式
        3. 检查任务执行时间
        4. 异步执行符合条件的任务
        5. 返回执行状态信息
        
    返回:
        执行状态字符串 ("异步执行结束" 或 "异步执行异常")
        
    实现细节:
        - 使用线程池异步执行任务
        - 自动管理数据库连接
        - 支持并发执行多个任务
        - 自动验证cron表达式有效性
        - 记录详细的执行日志
    """
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
    run_msg = ""
    try:
        now = datetime.datetime.now()
        for item in etl.dicts(cron_list):

            try:

                cron = CronTab(item['ID'], item['CRON'], item['JOBS_NAME']
                               , item['CRON_TYPE'], item['IS_CONCURRENCY'], db.get_config())

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
        run_msg = "异步执行结束"
    except Exception as e:
        logging.error(traceback.format_exc())
        run_msg = "异步执行异常"
    finally:
        if db is not None:
            db.close()
    return run_msg


# 用于存储第三方调用参数的全局变量
third_party_params = {}

@app.route('/runjob/<all_jobs>', methods=['GET', 'POST'])
def runjobs(all_jobs):
    """
    执行指定作业链的接口
    
    参数:
        all_jobs: 以逗号分隔的作业名称列表
        支持通过GET/POST传递额外参数，格式为JSON
        
    返回: 
        作业执行结果
        
    功能:
        1. 处理作业链执行
        2. 接收并存储第三方调用参数
        3. 保持对原有接口的兼容性
        
    实现细节:
        - 支持GET和POST方法
        - 自动解析JSON格式参数
        - 将参数存储在全局变量third_party_params中
        - 参数格式: {"key1": "value1", "key2": "value2"}
    """
    global third_party_params
    
    # 处理GET请求参数
    if request.method == 'GET':
        params = request.args.to_dict()
        if params:
            third_party_params = params
            
    # 处理POST请求参数
    elif request.method == 'POST':
        if request.is_json:
            third_party_params = request.get_json()
        else:
            third_party_params = request.form.to_dict()
            
    return run_job(all_jobs)


def run_job(all_jobs):
    """
    执行作业链的核心函数
    
    参数:
        all_jobs: 以逗号分隔的作业名称列表
        
    功能:
        1. 初始化数据库连接
        2. 设置作业链上下文
        3. 按顺序执行作业
        4. 处理异常情况
        5. 清理资源
        
    返回:
        作业执行状态信息字符串
        
    实现细节:
        - 使用锁机制保证线程安全
        - 自动管理数据库连接
        - 支持失败继续执行模式
        - 记录详细的执行日志
        - 处理多种数据库异常
        - 自动清理历史日志
    """
    databases = {}
    db_meta = None
    exit_val = 0
    try:
        job_chain = all_jobs.replace(",", "->")
        session["job_chain"] = job_chain
        session["current_job"] = job_chain
        session["third_party_params"] = third_party_params
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
        logging.error(traceback.format_exc())
        logging.error("数据库异常: {}".format(e))
        exit_val = 1
    except pymysql.DatabaseError as e:
        logging.error(traceback.format_exc())
        logging.error("数据库异常: {}".format(e))
        exit_val = 1
    except pymssql.DatabaseError as e:
        logging.error(traceback.format_exc())
        logging.error("数据库异常: {}".format(e))
        exit_val = 1
    except KeyError as e:
        logging.error(traceback.format_exc())
        logging.error("未匹配到字段: {}".format(e))
        exit_val = 1
    except Exception as e:
        logging.error(traceback.format_exc())
        exit_val = 1

    finally:
        for key in databases:
            # print("关闭数据库：{}".format(key))
            databases[key].close()
            # logging.info("关闭{}".format(key))
        if db_meta is not None:
            db_meta.close()
            # logging.info("关闭{}".format("db_meta"))
        # exit(exit_val)
        return "任务结束：{}".format(job_chain)


def run(MULTI_PROCESS, proc_count=1):
    """
    启动REST服务的函数
    
    参数:
        MULTI_PROCESS: 是否使用多进程模式 (布尔值)
        proc_count: 进程数量 (默认值为1)
        
    功能:
        1. 单进程模式: 直接启动WSGI服务器
        2. 多进程模式: 根据CPU核心数启动多个进程
        3. 处理服务器启动和停止事件
        
    返回:
        无返回值
        
    实现细节:
        - 使用gevent的WSGIServer实现高性能并发
        - 支持单进程和多进程两种模式
        - 多进程模式下根据CPU核心数自动调整进程数量
        - 使用Process类实现多进程
        - 自动处理服务器启动和停止事件
    """
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
    """
    主程序入口
    
    功能:
        1. 加载配置文件
        2. 初始化日志
        3. 启动REST服务
        4. 启动调度器
        5. 异常处理
        
    实现细节:
        - 解析数据库配置文件
        - 配置数据库连接
        - 设置日志级别
        - 启动多进程REST服务
        - 初始化调度器
        - 处理多种数据库异常
        - 自动关闭数据库连接
        - 返回适当的退出码
    """
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
        start_scheduler()
        # server = pywsgi.WSGIServer(('0.0.0.0', 8383), app)

        logging.warning("欢迎使用中普达ETL服务")
        # server.serve_forever()

        db.close()
        exit(0)

    except cx_Oracle.DatabaseError as e:
        logging.error("数据库异常: {}".format(e))
        logging.error("数据库异常: {}".format(traceback.format_exc()))
        exit(1)
    except pymysql.DatabaseError as e:
        logging.error("数据库异常: {}".format(traceback.format_exc()))
        exit(1)
    except pymssql.DatabaseError as e:
        logging.error("数据库异常: {}".format(e))
        logging.error("数据库异常: {}".format(traceback.format_exc()))
        exit(1)
    except KeyError as e:
        logging.error("数据库异常: {}".format(traceback.format_exc()))
        logging.error("未匹配到字段: {}".format(e))
        exit(1)
    except Exception as e:
        logging.error(traceback.format_exc())
