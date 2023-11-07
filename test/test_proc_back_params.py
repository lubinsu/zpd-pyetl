import petl as etl
import pymysql
import cx_Oracle
import datetime
import sys
from xml.dom.minidom import parse
from Database import *
from Job import Job
import logging

logging.basicConfig(level=logging.DEBUG, filemode='a')
import pymssql


def msgLog(msg, stepTime):
    print("{}, elapsed time：{}".format(msg, datetime.datetime.now() - stepTime))
    return datetime.datetime.now()


def insertBatch(self, sql, nameParams=[]):
    """batch insert much rows one time,use location parameter"""
    self.cursor.prepare(sql)
    self.cursor.executemany(None, nameParams)
    self.commit()


def insert2(self, connection, insertParam=[]):
    cursor = connection.cursor()
    # M=[(11,'sa','sa'),]
    sql = "insert into Python_Oracle (id,kinds,numbers) values (:id,:kinds,:numbers)"
    if (len(insertParam) == 0):
        print("插入的数据行的参数不能为空！")
    else:
        cursor.prepare(sql)
        result = cursor.executemany(None, insertParam)

        print("Insert result:", result)
        # count=cursor.execute("SELECT COUNT(*) FROM python_modules")
        # print("count of python_modules:",count)

    connection.commit()

    cursor.close()
    connection.close()


try:

    steptime = datetime.datetime.now()
    msgLog("开始执行", steptime)
    connection1 = pymysql.connect(password="123456", database="ydhl", user="root", host="192.168.41.136", port=3306)
    o_cnn = cx_Oracle.Connection("{}/{}@{}:{}/{}".format("system", "123456", "lubinsu", int(1521), "orcl"))
    # o_cnn = cx_Oracle.Connection("{}/{}@{}:{}/{}".format("dwd", "dwd123", "192.168.2.45", int(1521), "orcl"))
    o_cursor = o_cnn.cursor()
    # o_cursor = CursorProxy(cx_Oracle.Connection(o_cnn).cursor())
    connection2 = CursorProxy(o_cursor)
    # table = etl.fromdb(connection1, "select ID, RUN_MSG, STEP_MSG from etl_run_log")

    # mysqlCursor = connection1.cursor()
    # o_cursor.execute("SELECT ID, PROC_NAME, START_TIME, END_TIME, RUN_STATE, RUN_MSG, STEP_MSG FROM ETL_RUN_LOG")

    sql = "insert into etl_run_log (id, proc_name, start_time, end_time, run_state, run_msg, step_msg) values (%s,%s,%s,%s,%s,%s,%s)"
    ######################################################################################
    # 测试数据：4319条
    # 原生手写 oracle->mysql executemany耗时：06秒
    # M = []
    # for row in o_cursor:
    #     try:
    #         M.append((row[0], row[1], row[2], row[3], row[4], row[5], row[5]))
    #     except AttributeError:
    #         pass
    # # mysqlCursor.prepare(sql)
    # mysqlCursor.executemany(sql, M)
    #
    # connection1.commit()
    # msgLog("执行结束", steptime)
    ######################################################################################
    # 测试数据：4319*2条
    # petl oracle->mysql  耗时：06秒
    table = etl.fromdb(connection1, "SELECT ID,LOGIN_USER_ID,LOGIN_DEPART_ID,FUNCTION_NAME,HTTP_STATE_CODE,USE_TIME,MESSAGE,RECORD_TIME,SHEBEI_ID,YIYUAN_ID FROM log_record")

    print(etl.head(table, 1))
    if len(etl.head(table, 1)) >= 2:
        etl.todb(table=table, dbo=connection2, tablename="LOG_RECORD")
        print("有数据")
    else:
        print("没数据")

    # connection1.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    # etl.todb(table=table, dbo=connection1, tablename="etl_run_log")
    # msgLog("执行结束", steptime)

    ######################################################################################
    # 测试数据：8638条
    # petl mysql->oracle  耗时：0.4秒
    # connection1.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    # table = etl.fromdb(connection1, "select ID, PROC_NAME, START_TIME, END_TIME, RUN_STATE, RUN_MSG, STEP_MSG from etl_run_log")
    # o_cursor = CursorProxy(o_cnn.cursor())
    # etl.todb(table=table, dbo=o_cursor, tablename="ETL_RUN_LOG")
    # msgLog("执行结束", steptime)

    # msgLog("准备关闭", steptime)
    # mysqlCursor.close()
    # connection1.close()
    # msgLog("已关闭connection1", steptime)
    # o_cursor.close()
    # msgLog("已关闭o_cursor", steptime)
    # o_cnn.close()
    # msgLog("已关闭o_cnn", steptime)


    # print(table)
    # for row in etl.data(etl.cut(table, 'ID')):
    #     print(row[0])

    # for row in etl.dicts(table):
    #     print("update source_table a set a.run_state = {run_state} where a.id = {ID}".format(**row))

except Exception as err:
    print(err)
