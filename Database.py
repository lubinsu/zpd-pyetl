# 支持DB2
# import ibm_db_dbi
import traceback

import pymysql
import psycopg2
import cx_Oracle
import pymssql
# 支持DB2
# import ibm_db
# import iris
from pymysql.constants import CLIENT
from sqlalchemy import create_engine

from encryption.base64_encode import *
import logging


class CustomError(Exception):
    """自定义异常类"""
    pass

class CursorProxy(object):
    def __init__(self, cursor):
        self._cursor = cursor

    def executemany(self, statement, parameters, **kwargs):
        # convert parameters to a list
        parameters = list(parameters)
        # pass through to proxied cursor
        return self._cursor.executemany(statement, parameters, **kwargs)

    def __getattr__(self, item):
        return getattr(self._cursor, item)


class Database:

    def __init__(self, name, db, type_, host, port, user, password, back_host=None):

        # if self.name == 'name' and self.db == 'his_interface':
        #     raise CustomError("his_interface 开始创建，但是名字不对。")

        self.name = name
        self.db = db
        self.type_ = type_
        self.host = host
        self.port = port
        self.user = user
        self.password = decode(password)
        self.conn = None
        self.engine = None
        self.cursor = None
        self.back_host = back_host

    def getConnection(self):

        # if self.conn is not None:
        #     self.conn.ping(reconnect=True)
        # if self.name == 'his_interface' and self.conn is None:
        #     raise CustomError("his_interface 开始创建")

        if self.type_ == "oracle":
            if self.conn is None:
                try:
                    self.conn = cx_Oracle.Connection(
                        "{}/{}@{}:{}/{}".format(self.user, self.password, self.host, int(self.port), self.db))
                except:
                    try:
                        logging.warning("DataBase connect error,retry another method.")
                        self.conn = cx_Oracle.Connection(self.user, self.password,
                                                         cx_Oracle.makedsn(self.host, int(self.port), self.db))
                    except:
                        if self.back_host is not None:
                            try:
                                logging.warning("主库连接异常，已切换为备库")
                                self.conn = cx_Oracle.Connection(
                                    "{}/{}@{}:{}/{}".format(self.user, self.password, self.back_host, int(self.port), self.db))
                            except:
                                logging.warning("DataBase connect error,retry another method.")
                                self.conn = cx_Oracle.Connection(self.user, self.password,
                                                                 cx_Oracle.makedsn(self.back_host, int(self.port), self.db))
                        else:
                            raise


        elif self.type_ == "mysql":
            if self.conn is None:
                try:
                    self.conn = pymysql.connect(password=self.password, database=self.db, user=self.user, host=self.host,
                                                port=int(self.port), client_flag=CLIENT.MULTI_STATEMENTS
                                                , autocommit=True)
                except:
                    if self.back_host is not None:
                        logging.warning("主库连接异常，已切换为备库")
                        self.conn = pymysql.connect(password=self.password, database=self.db, user=self.user,
                                                    host=self.back_host,
                                                    port=int(self.port), client_flag=CLIENT.MULTI_STATEMENTS
                                                    , autocommit=True)
                    else:
                        raise

        elif self.type_ == "sqlserver":

            # 部分存在密码为空的情况，无需输入密码
            if self.conn is None and (self.password is None or self.password == ''):
                self.conn = pymssql.connect(host=self.host, user=self.user, port=int(self.port),
                                            database=self.db)

            elif self.conn is None:
                self.conn = pymssql.connect(host=self.host, user=self.user, port=int(self.port), password=self.password,
                                            database=self.db)

        # elif self.type_ == "db2":
        #
        #     if self.conn is None:
        #         dsn = (
        #             "DRIVER={{IBM DB2 ODBC DRIVER}};"
        #             "DATABASE={0};"
        #             "HOSTNAME={1};"
        #             "AUTHENTICATION=SERVER;"
        #             "PORT={2};"
        #             "PROTOCOL=TCPIP;"
        #             "UID={3};"
        #             "PWD={4};").format(self.db, self.host, int(self.port), self.user, self.password)
        #
        #         ibm_db_conn = ibm_db.connect(dsn, "", "")
        #         self.conn = ibm_db_dbi.Connection(ibm_db_conn)


        elif self.type_ == "postgre":
            if self.conn is None:
                self.conn = psycopg2.connect(database=self.db, user=self.user, password=self.password, host=self.host,
                                             port=self.port)

        # elif self.type_ == "iris":
        #     if self.conn is None:
        #         self.conn = iris.connect("{}:{}/{}".format(self.host, int(self.port), self.db), self.user, self.password)
        #     return self.conn
        else:
            return None

        logging.warning("#连接成功#{}#{}#{}".format(self.conn, self.name, self.get_config()))

        return self.conn

    def getEngine(self):
        if self.type_ == "oracle":
            if self.engine is None:
                self.engine = create_engine(
                    'oracle://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.db))
            return self.engine
        elif self.type_ == "mysql":
            if self.engine is None:
                self.engine = create_engine(
                    'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(self.user, self.password, self.host, self.port,
                                                                         self.db))
            return self.engine
        elif self.type_ == "sqlserver":
            if self.engine is None:
                self.engine = create_engine(
                    'mssql+pymssql://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.db))
            return self.engine
        elif self.type_ == "postgre":
            if self.engine is None:
                self.engine = create_engine(
                    'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port,
                                                                  self.db))
            return self.engine

        elif self.type_ == "db2":
            if self.engine is None:
                self.engine = create_engine(
                    "db2:///?Server={}&Port={}&User={}&Password={}&Database={}"
                    .format(self.host, self.port, self.user, self.password, self.db))
            return self.engine
        else:
            return None

    def getCursor(self):
        if self.cursor is None and self.type_ == "oracle":
            self.cursor = CursorProxy(cx_Oracle.Connection(
                "{}/{}@{}:{}/{}".format(self.user, self.password, self.host, int(self.port), self.db)).cursor())
        elif self.cursor is None and self.type_ == "db2":
            self.cursor = self.conn.cursor()
        return self.cursor

    def get_config(self):
        # 返回对象的所有属性字典
        return {
            'name': self.name,
            'db': self.db,
            'type_': self.type_,
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': encode(self.password),  # 假设 password 已经被解码
            'back_host': self.back_host
        }

    def close(self):
        # logging.warning("#连接关闭#{}#{}".format(self.conn, self.name))
        # if self.name == 'his_interface' and self.conn is None:
        #     raise CustomError("his_interface is None")

        if self.cursor is not None:
            self.cursor.close()

        if self.conn is not None:
            self.conn.close()
