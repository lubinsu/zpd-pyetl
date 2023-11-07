# @Time : 7/07/2021 09:14 AM
# @Author : lubinsu
# @File : Transform.py
# @desc : transform 类

import datetime
import logging

'''
transform作为数据流过程中的步骤，需要先在job中配置source，而后的每一个步骤均在transform中配置。
如果只是执行SQL语句（块）或者执行存储过程则只需要在job的source sql中进行配置即可，无需配置transform。
'''


class Transform:
    class TransParams:
        def __init__(self, con_name, sql):
            self.conName = con_name
            self.sql = sql

    class From:
        def __init__(self, conName, sql):
            self.conName = conName
            self.sql = sql

    class To:
        def __init__(self, conName, target):
            self.conName = conName
            self.target = target

        def setConn(self, conn):
            self.connection = conn

        def getConn(self):
            return self.connection

        def setEngine(self, conn):
            self.engine = conn

        def getEngine(self):
            return self.engine

    def msgLog(self, msg, stepTime):
        logging.info("{}, elapsed time：{}".format(msg, datetime.datetime.now() - stepTime))
        return datetime.datetime.now()

    def __init__(self, name, job_type, src_con_name, source_sql, tos, param_con_name='', param_sql=''):
        self.source = self.From(src_con_name, source_sql)
        self.target = tos
        self.jobParams = self.TransParams(param_con_name, param_sql)
        self.name = name
        self.jobType = job_type
