import pymysql
import psycopg2
import cx_Oracle
import pymssql
# import iris
from pymysql.constants import CLIENT
from sqlalchemy import create_engine

from encryption.base64_encode import *
import logging


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

            return self.conn
        elif self.type_ == "mysql":
            if self.conn is None:
                try:
                    self.conn = pymysql.connect(password=self.password, database=self.db, user=self.user, host=self.host,
                                                port=int(self.port), client_flag=CLIENT.MULTI_STATEMENTS)
                except:
                    if self.back_host is not None:
                        logging.warning("主库连接异常，已切换为备库")
                        self.conn = pymysql.connect(password=self.password, database=self.db, user=self.user,
                                                    host=self.back_host,
                                                    port=int(self.port), client_flag=CLIENT.MULTI_STATEMENTS)
                    else:
                        raise
            return self.conn
        elif self.type_ == "sqlserver":
            if self.conn is None:
                self.conn = pymssql.connect(host=self.host, user=self.user, port=int(self.port), password=self.password,
                                            database=self.db)
            return self.conn
        elif self.type_ == "postgre":
            if self.conn is None:
                self.conn = psycopg2.connect(database=self.db, user=self.user, password=self.password, host=self.host,
                                             port=self.port)
            return self.conn
        # elif self.type_ == "iris":
        #     if self.conn is None:
        #         self.conn = iris.connect("{}:{}/{}".format(self.host, int(self.port), self.db), self.user, self.password)
        #     return self.conn
        else:
            return None

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
        else:
            return None

    def getCursor(self):
        if self.cursor is None:
            self.cursor = CursorProxy(cx_Oracle.Connection(
                "{}/{}@{}:{}/{}".format(self.user, self.password, self.host, int(self.port), self.db)).cursor())
        return self.cursor

    def close(self):
        if self.cursor is not None:
            self.cursor.close()

        if self.conn is not None:
            self.conn.close()
