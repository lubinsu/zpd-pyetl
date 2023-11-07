# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from logging.handlers import RotatingFileHandler

import petl as etl
import datetime
import sys
from xml.dom.minidom import parse
from utils import *
import logging

logging.basicConfig(level=logging.INFO, filemode='a')

databases = {}
jobs = {}
sysParams = {}


def logConfig(logName):
    file_log_handler = RotatingFileHandler(
        "{}/log/{}".format(sys.path[0], "{}.log".format(logName.replace('.xml', ''))),
        maxBytes=1024 * 1024 * 100,
        backupCount=10)
    formatter = logging.Formatter('%(asctime)s - [line:%(lineno)d] - %(levelname)s: %(message)s')
    file_log_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_log_handler)


def msgLog(msg, stepTime):
    logging.info("{}, elapsed time：{}".format(msg, datetime.datetime.now() - stepTime))
    return datetime.datetime.now()


def execute(job):
    steptime = datetime.datetime.now()
    if jobs[job].jobType == "syn":
        sourceSQL = jobs[job].source.sql
        sourceDb = jobs[job].source.conName

        targetTable = jobs[job].target[0].target
        targetDb = jobs[job].target[0].conName

        msgLog("job:{},开始同步数据: {}".format(job, sourceSQL), steptime)
        sourceConnection = databases[sourceDb].getConnection()
        if databases[targetDb].type_ == "oracle":
            connection2 = databases[targetDb].getCursor()
        else:
            connection2 = databases[targetDb].getConnection()

        table = etl.fromdb(sourceConnection, sourceSQL)

        # if databases[sourceDb].type_ == "mysql":
        #     sourceConnection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

        if databases[targetDb].type_ == "mysql":
            connection2.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

        if databases[targetDb].type_ == "oracle":
            etl.todb(table=table, dbo=connection2, tablename=targetTable)
        else:
            etl.todb(table=table, dbo=connection2, tablename=targetTable)
        steptime = msgLog("job:{},数据同步完成: {}".format(job, targetTable), steptime)

    # elif jobs[job].jobType == "syn_dynamic":
    #     sourceSQL = jobs[job].source.sql
    #     sourceDb = jobs[job].source.conName
    #
    #     paramsDb = jobs[job].jobParams.conName
    #     paramsSQL = jobs[job].jobParams.sql
    #
    #     sourceConnection = databases[sourceDb].getConnection()
    #
    #     msgLog("job:{},开始轮询同步数据: {}".format(job, sourceSQL), steptime)
    #     for target in jobs[job].target:
    #         targetDb = target.conName
    #
    #         targetConn = databases[targetDb].getConnection()
    #         if databases[targetDb].type_ == "mysql":
    #             targetConn.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    #
    #         target.setConn(targetConn)
    #     msgLog("job:{},数据库连接创建完成: {}".format(job, sourceSQL), steptime)
    #
    #     paramsConnection = databases[paramsDb].getConnection()
    #
    #     paramsTable = etl.fromdb(paramsConnection, paramsSQL)
    #
    #     i = 1
    #     for row in etl.dicts(paramsTable):
    #         for target in jobs[job].target:
    #             target.getConn().cursor().execute(target.target.format(**row))
    #             if i % 1001 == 0:
    #                 target.getConn().commit()
    #         i = i + 1
    #
    #     for target in jobs[job].target:
    #         target.getConn().commit()
    #
    #     i = 0
    #     # 获取动态参数
    #     for row in etl.dicts(paramsTable):
    #         table = etl.fromdb(sourceConnection, sourceSQL.format(**row))
    #         msgLog("job:{},开始同步数据: {}".format(job, sourceSQL.format(**row)), steptime)
    #         for target in jobs[job].target:
    #
    #         if i == 0:
    #             etl.todb(table=table, dbo=connection2, tablename=targetTable.format(**row))
    #             i = i + 1
    #         else:
    #             etl.appenddb(table=table, dbo=connection2, tablename=targetTable.format(**row))
    #
    #         msgLog("job:{},数据同步完成: {}".format(job, targetTable.format(**row)), steptime)
    #
    #     steptime = msgLog("job:{},数据同步完成: {}".format(job, targetTable), steptime)

    elif jobs[job].jobType == "procedure":
        targetProc = jobs[job].target[0].target
        targetDb = jobs[job].target[0].conName

        msgLog("job:{},开始调用存储过程: {}.{}".format(job, targetDb, targetProc), steptime)
        connection = databases[targetDb].getConnection()
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        with connection.cursor() as cursor:
            try:
                cursor.execute(targetProc)
                connection.commit()
            except Exception as err:
                logging.error(err)
                connection.rollback()
        connection.close()

        steptime = msgLog("job:{},调用存储过程完成: {}.{}".format(job, targetDb, targetProc), steptime)
    elif jobs[job].jobType == "stream":
        sourceSQL = jobs[job].source.sql
        sourceDb = jobs[job].source.conName
        sourceConnection = databases[sourceDb].getConnection()
        table = etl.fromdb(sourceConnection, sourceSQL)

        msgLog("job:{},开始轮询同步数据: {}".format(job, sourceSQL), steptime)
        for target in jobs[job].target:
            targetDb = target.conName

            targetConn = databases[targetDb].getConnection()
            if databases[targetDb].type_ == "mysql":
                targetConn.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

            target.setConn(targetConn)
        msgLog("job:{},数据库连接创建完成: {}".format(job, sourceSQL), steptime)
        i = 1
        for row in etl.dicts(table):
            for target in jobs[job].target:
                target.getConn().cursor().execute(target.target.format(**row))
                if i % 1001 == 0:
                    target.getConn().commit()
            i = i + 1

        for target in jobs[job].target:
            target.getConn().commit()
        steptime = msgLog("job:{},轮询同步数据完成".format(job), steptime)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    logConfig(sys.argv[1])
    dom = parse("{}/resources/{}".format(sys.path[0], sys.argv[1]))
    document = dom.documentElement

    logging.info("正在执行任务：{}".format(sys.argv[1]))
    databases = get_databases(document)
    jobs = getJobs(document)

    for job in sys.argv[2].split(","):
        execute(job)
    logging.info("任务结束：{}".format(sys.argv[1]))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
