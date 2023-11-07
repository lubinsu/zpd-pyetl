#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time : 8/26/2021 10:28 AM 
# @Author : lubinsu 
# @File : logger_handler_to_mysql.py
# @desc : 日志写入mysql

# coding: utf-8
import logging

from sqlalchemy import (Column, DateTime, Integer, String, create_engine, text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from threading import Lock
from flask import session

# 创建对象的基类：
Base = declarative_base()


class PyEtlRunLog(Base):
    # 表的名字：
    __tablename__ = 'py_etl_run_log'

    # 表的结构:
    id = Column('id', Integer, primary_key=True)
    create_time = Column('create_time', DateTime, nullable=False, server_default=text("'0000-00-00 00:00:00.000'"))
    status = Column('status', String(10))
    message = Column('message', String(255))
    line_no = Column('line_no', Integer)
    job_chain = Column('job_chain', String(255))
    current_job = Column('current_job', String(255))
    pid = Column('pid', String(255))


class LoggerHandlerToMysql(logging.Handler):
    def __init__(self, configdb_str, formatter, job_chain, pid):
        self.configdb_str = configdb_str
        self.config_engine = create_engine(self.configdb_str)
        self.ConfigSession = sessionmaker(bind=self.config_engine)
        self.config_session = self.ConfigSession()

        self.formatter = formatter
        self.job_chain = job_chain
        self.current_job = job_chain
        self.pid = pid
        self.lock = Lock()
        logging.Handler.__init__(self)

    def setCurrentJob(self, current_job):
        self.current_job = current_job

    def setJobChain(self, job_chain):
        self.job_chain = job_chain

    # 适配ETL服务
    def getJobChain(self):
        try:
            if "job_chain" in session.keys():
                return session["job_chain"]
            else:
                return self.job_chain
        except:
            return self.job_chain

    # 适配ETL服务
    def getCurrentJob(self):
        try:
            if "current_job" in session.keys():
                return session["current_job"]
            else:
                return self.current_job
        except:
            return self.current_job

    # 适配ETL服务
    def getPid(self):
        try:
            if "timestmp" in session.keys():
                return session["timestmp"]
            else:
                return self.pid
        except:
            return self.pid

    def emit(self, record):
        self.lock.acquire()

        self.config_engine = create_engine(self.configdb_str)
        self.ConfigSession = sessionmaker(bind=self.config_engine)
        self.config_session = self.ConfigSession()

        # print("session[\"job_chain\"]:{}".format(session["job_chain"]))
        # print("session[\"current_job\"]:{}".format(session["current_job"]))

        record.job_chain = self.getJobChain()
        record.current_job = self.getCurrentJob()

        # print("record.job_chain:{}".format(record.job_chain))
        # print("record.current_job:{}".format(record.current_job))
        record.pid = self.getPid()
        self.formatter.format(record)
        log_model = PyEtlRunLog()
        log_model.create_time = record.asctime.replace(",", ".")
        log_model.status = record.levelname

        log_model.message = record.message
        log_model.line_no = record.lineno
        log_model.job_chain = record.job_chain
        log_model.current_job = record.current_job
        log_model.pid = record.pid
        self.config_session.add(log_model)
        self.close()
        self.lock.release()
        pass

    def close(self):
        self.config_session.commit()
        self.config_session.close()
        pass
