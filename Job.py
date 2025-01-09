# @Time : 7/07/2021 09:14 AM
# @Author : lubinsu
# @File : Job.py
# @desc : Job 类

import datetime
import os

import execjs
import traceback
from pymysql.converters import escape_string
from flask import session
from plugins import *
from job_utils import *

import pandas as pd
import petl as etl
from lxml import etree
from suds.client import Client
from suds.xsd.doctor import ImportDoctor, Import

from Database import *
from zpd_parser.json_parser import *
from zpd_parser.xml_parser import get_xml_val
import csv

cx_type = {'VARCHAR2': cx_Oracle.STRING,
           'NUMBER': cx_Oracle.NUMBER,
           'DATE': cx_Oracle.DATETIME,
           'TIMESTAMP': cx_Oracle.TIMESTAMP,
           'CHAR': cx_Oracle.FIXED_CHAR,
           'CLOB': cx_Oracle.CLOB}


# 参数类
class Param:

    def __str__(self):
        return "name:{} in_out:{} type:{}".format(self.name, self.in_out, self.param_type)

    def __init__(self, name, in_out, param_type):
        self.name = name
        self.in_out = in_out
        self.param_type = param_type # 参数类


class BlobSeq:

    def __str__(self):
        return "field_seq:{} field_name:{} is_blob:{}".format(self.field_seq, self.field_name, self.is_blob)

    def __init__(self, field_seq, field_name, is_blob):
        self.field_seq = field_seq
        self.field_name = field_name
        self.is_blob = is_blob


def replace_placeholders(query, replacements):
    """
    替换 SQL 字符串中的占位符
    :param query: 包含占位符的 SQL 字符串
    :param replacements: 包含替换值的字典
    :return: 替换后的字符串
    """
    # 匹配 {key} 样式的占位符
    pattern = r'\{(\w+)\s*\}'  # 匹配花括号中的键名，并忽略多余空格

    def replacer(match):
        key = match.group(1).strip()  # 提取占位符中的键名
        if key in replacements:
            return str(replacements[key])  # 返回替换后的值
        return match.group(0)  # 如果未找到替换值，保持原样

    # 使用正则替换占位符
    return re.sub(pattern, replacer, query)

def run_case(url, method, params, doctor):
    values = tuple(params.values())  # 存储传进来的json的value
    client = Client(url, doctor=doctor)
    f_name = "client.service." + method
    try:
        # 执行接口方法，方法名为变量，参数值也为变量，这样可用于不同ws接口测试
        result = eval(f_name)(*values)
        ret = result
    except Exception as e:
        ret = {"code": "-1", "errMsg": str(e)}
    return ret


# 每个Job都会产生一个实例
class Job:
    class JobParams:
        def __init__(self, con_name, sql, is_fail_continue="N"):
            self.conName = con_name
            self.sql = sql
            # 失败是否继续执行，默认为 N，不执行
            self.is_fail_continue = is_fail_continue

    # 来源
    class From:
        def __init__(self, conName, sql):
            self.conName = conName
            self.sql = sql

    # 目标
    class To:
        def __init__(self, conName, target, type_="", params=None, source_field="", exception_patterns=None, retries=0, delay=0):
            if params is None:
                params = []
            self.type_ = type_
            self.params = params
            self.conName = conName
            self.target = target
            self.create = False
            self.source_field = source_field
            self.exception_patterns = exception_patterns
            self.retries = retries
            self.delay = delay

        def set_conn(self, conn):
            self.connection = conn

        def get_conn(self):
            return self.connection

        def set_create(self, is_create=True):
            self.create = is_create

        def get_create(self):
            return self.create

    # 记录日志
    def msgLog(self, msg, stepTime, level="INFO"):

        # for hdlr in logging.getLogger().handlers:
        #     if hdlr.__class__.__name__ == "LoggerHandlerToMysql":
        #         hdlr.setCurrentJob(self.name)
        if level == "INFO":
            logging.info("{}, elapsed time：{}".format(msg, datetime.datetime.now() - stepTime))
        elif level == "DEBUG":
            logging.debug("{}, elapsed time：{}".format(msg, datetime.datetime.now() - stepTime))

        return datetime.datetime.now()

    # 执行Job
    def execute(self, databases):

        for hdlr in logging.getLogger().handlers:
            if hdlr.__class__.__name__ == "LoggerHandlerToMysql":
                hdlr.setCurrentJob(self.name)
        steptime = datetime.datetime.now()
        self.msgLog("执行任务开始", steptime, level="INFO")

        # logging.debug("执行Job任务：{}".format(self.name))
        if self.jobType == "syn":
            # sourceSQL = self.source.sql
            try:
                # sourceSQL = self.source.sql.format(**session)
                sourceSQL = replace_placeholders(self.source.sql, session["third_party_params"])
            except:
                sourceSQL = self.source.sql

            sourceDb = self.source.conName

            targetTable = self.target[0].target
            target_db = self.target[0].conName

            self.msgLog("开始同步数据: {}".format(sourceSQL), steptime, level="DEBUG")
            sourceConnection = databases[sourceDb].getConnection()

            if databases[target_db].type_ in ["oracle", "db2"]:
                connection2 = databases[target_db].getCursor()
            else:
                connection2 = databases[target_db].getConnection()
            logging.debug("获取数据库连接完成：{}".format(sourceDb))

            table = etl.fromdb(sourceConnection, sourceSQL)

            # if databases[sourceDb].type_ == "mysql":
            #     sourceConnection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

            if databases[target_db].type_ == "mysql":
                logging.debug("执行Job任务：{}, {}".format(self.jobType, "mysql连接需要设置SQL_MODE"))
                with connection2.cursor() as cursor:
                    cursor.execute('SET SQL_MODE=ANSI_QUOTES')

            if databases[target_db].type_ == "oracle" and len(etl.head(table, 1)) >= 2:
                logging.debug("执行Job任务：{}, {}".format(self.jobType, "目标数据库：oracle，数据存在，执行插入语句"))
                # create_table(table=table, dbo=connection2, tablename=targetTable, schema=None, commit=True,
                #              constraints=True, metadata=None,
                #              dialect=None, sample=1000)
                # etl.make_create_table_statement(table, tablename, schema=None,
                #                 constraints=True, metadata=None, dialect=None)
                etl.todb(table=table, dbo=connection2, tablename=targetTable, create=self.target[0].get_create())
            elif len(etl.head(table, 1)) >= 2:
                logging.debug("执行Job任务：{}, {}".format(self.jobType, "数据存在，执行插入语句"))
                etl.todb(table=table, dbo=connection2, tablename=targetTable, create=self.target[0].get_create())
            steptime = self.msgLog("数据同步完成，目标表: {}".format(targetTable), steptime)

        # 同步BLOB字段
        elif self.jobType == "syn_blob":
            sourceSQL = self.source.sql
            sourceDb = self.source.conName

            targetTable = self.target[0].target
            target_db = self.target[0].conName

            self.msgLog("开始同步数据: {}".format(sourceSQL), steptime, level="DEBUG")
            sourceConnection = databases[sourceDb].getConnection()

            connection2 = databases[target_db].getConnection()
            logging.debug("获取数据库连接完成：{}".format(sourceDb))

            if databases[target_db].type_ == "mysql":
                if databases[sourceDb].type_ in ["oracle", "db2"]:
                    o_cursor = sourceConnection.cursor()

                o_cursor.execute(sourceSQL)

                for row in o_cursor:
                    i = 0
                    fields_arr = []
                    arr = []
                    while i < len(self.target[0].params):
                        fields_arr.append(self.target[0].params[i].field_name)
                        if self.target[0].params[i].is_blob == 1:

                            if row[i] == None:
                                image=None
                            else:
                                image=row[i].read()
                            arr.append(image)
                        else:
                            arr.append(row[i])
                        i=i+1

                    fields = ",".join([s for s in fields_arr])
                    quots = ",".join(["%s" for s in fields_arr])
                    #print("insert into {} ({}) values ({})".format(targetTable, fields, quots))
                    insert_sql = "insert into {} ({}) values ({});".format(targetTable, fields, quots)
                    connection2.cursor().execute(insert_sql, tuple(arr))
                    connection2.commit()

            steptime = self.msgLog("数据同步完成，目标表: {}".format(targetTable), steptime)

        elif self.jobType == "syn_dynamic":
            paramsSQL = self.source.sql
            paramsDb = self.source.conName

            self.msgLog("开始轮询同步数据: {}".format(paramsSQL), steptime, level="DEBUG")
            paramsConnection = databases[paramsDb].getConnection()
            paramsTable = etl.fromdb(paramsConnection, paramsSQL)
            logging.debug("执行Job任务：{}, {}".format(self.jobType, "获取动态SQL完成"))
            i = 0
            # 获取动态参数
            for row in etl.dicts(paramsTable):
                for transform in self.transforms:

                    if transform.jobType == 'transform':
                        sourceDb = transform.source.conName
                        sourceSql = transform.source.sql
                        table = etl.fromdb(databases[sourceDb].getConnection(), sourceSql.format(**row))

                        self.msgLog("开始同步数据: {}".format(sourceSql.format(**row)), steptime, level="DEBUG")
                        if '.' in transform.target[0].target.format(**row):
                            targetTable = transform.target[0].target.format(**row).split('.')[1]
                            schema_name = transform.target[0].target.format(**row).split('.')[0]
                            target_db = transform.target[0].conName
                        else:
                            targetTable = transform.target[0].target.format(**row)
                            schema_name = None
                            target_db = transform.target[0].conName

                        if databases[target_db].type_ in ["oracle", "db2"]:
                            connection2 = databases[target_db].getCursor()
                        else:
                            connection2 = databases[target_db].getConnection()

                        if databases[target_db].type_ == "mysql":
                            logging.debug("执行Job任务：{}, {}".format(self.jobType, "mysql连接需要设置SQL_MODE"))
                            connection2.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

                        dcount = len(etl.head(table, 1))
                        if dcount >= 2:
                            logging.debug("执行Job任务：{}, {}".format(self.jobType, "数据存在，执行插入语句"))
                            etl.appenddb(table=table, dbo=connection2, schema=schema_name, tablename=targetTable)

                        row["_status"] = '1'
                        row["_errmsg"] = '执行成功'
                        steptime = self.msgLog("数据同步完成，目标表: {}".format(targetTable), steptime, level="DEBUG")
                    elif transform.jobType == "sql":
                        # 20220608 添加对null的转义
                        # print(row)
                        target_sql = transform.target[0].target
                        target_db = transform.target[0].conName
                        # print(target_sql)
                        sql = target_sql.format(**row).replace("'None'", "null").replace("None", "null")
                        # print("sql:{}".format(sql))
                        # print("transform.target[0]:{}".format(transform.target[0]))
                        # print("target_db:{}".format(target_db))
                        try:
                            if databases[target_db].type_ in ["oracle", "db2"]:
                                row_count = databases[target_db].getCursor().execute(sql)
                            else:
                                row_count = databases[target_db].getConnection().cursor().execute(sql)
                            row["_status"] = '1'
                            row["_errmsg"] = '执行成功:更新条数为：{}'.format(str(row_count))
                        except (Exception) as e1:
                            ex1 = "timed out"
                            ex2 = "Deadlock"
                            # ex3 = "Duplicate"
                            if ex1 in str(e1) or ex2 in str(e1):
                                try:
                                    if databases[target_db].type_ in ["oracle", "db2"]:
                                        row_count = databases[target_db].getCursor().execute(sql)
                                    else:
                                        row_count = databases[target_db].getConnection().cursor().execute(sql)
                                    row["_status"] = '1'
                                    row["_errmsg"] = '重试成功:更新条数为：{}'.format(str(row_count))
                                    logging.warning("重试成功，执行的SQL：{}".format(sql))
                                except Exception as e:
                                    row["_status"] = '2'
                                    row["_errmsg"] = escape_string(repr(e))
                                    logging.error("SQL重试失败：{}，执行的SQL：{}".format(traceback.format_exc(), sql))
                            else:
                                row["_status"] = '2'
                                row["_errmsg"] = escape_string(repr(e1))
                                logging.error("SQL执行失败：{}，执行的SQL：{}".format(traceback.format_exc(), sql))

                i = i + 1

            steptime = self.msgLog("数据同步完成.", steptime)

        elif self.jobType == "procedure":
            targetProc = self.target[0].target
            try:
                # sourceSQL = self.source.sql.format(**session)
                targetProc = replace_placeholders(self.target[0].target, session["third_party_params"])

            except:

                targetProc = self.target[0].target

            target_db = self.target[0].conName

            self.msgLog("开始调用存储过程: {}.{}".format(target_db, targetProc), steptime)
            if databases[target_db].type_ == "oracle":
                # 获取参数清单
                with databases[target_db].getConnection().cursor() as cursor:
                    cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
                databases[target_db].getConnection().cursor().execute(targetProc)
                # databases[target_db].getConnection().cursor().callproc(targetProc)  # ['Nick', 'Nick, Good Morning!']
                # steptime = self.msgLog("调用存储过程: {}，完成".format(targetProc), steptime, level="DEBUG")
            else:
                connection = databases[target_db].getConnection()
                # cursor = connection.cursor(pymysql.cursors.DictCursor)

                with connection.cursor() as cursor:
                    try:
                        cursor.execute(targetProc)
                        connection.commit()
                    except Exception as err:
                        logging.error(err)
                        connection.rollback()

            steptime = self.msgLog("调用存储过程完成: {}.{}".format(target_db, targetProc), steptime)
        elif self.jobType == "shell":
            target_shell = self.target[0].target
            # target_db = self.target[0].conName

            self.msgLog("开始调用shell脚本: {}".format(target_shell), steptime)
            v_rst = os.system(target_shell)
            if v_rst == 0:
                steptime = self.msgLog("调用shell脚本完成: {}".format(target_shell), steptime)
            else:
                logging.error("调用shell失败：{}".format(v_rst))

        elif self.jobType == "stream":
            # sourceSQL = self.source.sql
            try:
                # sourceSQL = self.source.sql.format(**session)
                sourceSQL = replace_placeholders(self.source.sql, session["third_party_params"])
            except:
                sourceSQL = self.source.sql

            sourceDb = self.source.conName
            sourceConnection = databases[sourceDb].getConnection()
            table = etl.fromdb(sourceConnection, sourceSQL)

            self.msgLog("开始轮询同步数据: {}".format(sourceSQL), steptime)
            for target in self.target:
                if target.type_ in ['sql', 'table', 'procedure', 'transform', 'list_2_table']:
                    target_db = target.conName
                    target_conn = databases[target_db].getConnection()
                    if databases[target_db].type_ == "mysql":
                        target_conn.cursor().execute('SET SQL_MODE=ANSI_QUOTES')

                    target.set_conn(target_conn)
            self.msgLog("数据库连接创建完成: {}".format(sourceSQL), steptime, level="DEBUG")
            i = 1
            for row in etl.dicts(table):
                for target in self.target:
                    if target.type_ == "sql":
                        # 20220608 添加对null的转义
                        # print(row)
                        sql = target.target.format(**row).replace("'None'", "null").replace("None", "null")
                        # print("sql:{}".format(sql))

                        try:
                            # 执行SQL，返回成功标识，如果失败重试，同时返回重试次数
                            exec_result, retry_count = exec_sql(
                                sql=sql,
                                row=row,
                                target=target,
                                databases=databases,
                                retries=target.retries,
                                delay=target.retries,
                                backoff=1,
                                patterns=target.exception_patterns
                            )

                            if exec_result is None:
                                row["_status"] = '2'
                                row["_errmsg"] = "SQL执行失败, 重试次数: {}".format(retry_count)
                            else:
                                row["_status"] = '1'
                                row["_errmsg"] = exec_result
                                # logging.error("SQL执行成功，返回信息：{}，执行的SQL：{}".format(exec_result, sql))
                            # print("_errmsg:{}".format(row["_errmsg"]))
                        except Exception as e:
                                row["_status"] = '2'
                                row["_errmsg"] = escape_string(repr(e))
                                logging.error("SQL执行失败：{}，执行的SQL：{}".format(traceback.format_exc(), sql))

                    elif target.type_ == "shell":

                        target_shell = target.source_field.format(**row)
                        self.msgLog("开始调用shell脚本: {}".format(target_shell), steptime)
                        v_rst = os.system(target_shell)
                        if v_rst == 0:
                            steptime = self.msgLog("调用shell脚本完成: {}".format(target_shell), steptime)
                        else:
                            logging.error("调用shell失败：{}".format(v_rst))

                    elif target.type_ == "javascript":
                        # 执行javascript脚本
                        js = target.target.format(**row)
                        self.msgLog("执行javascript脚本: {}".format(js), steptime, level="DEBUG")
                        docjs = execjs.compile(js)
                        for var in target.params:
                            res = docjs.eval(var.name)
                            row[var.name] = res

                    # 动态调用自定义函数，用于非结构化数据处理。
                    # 配置字段：py_transform.from_sql，以及 py_transform.to_target
                    elif target.type_ == "dy_function":
                        func_name = row[target.target.format(**row)]
                        rst = eval("{0}".format(func_name))(row, databases)
                        row = row if rst is None else rst

                    elif target.type_ == "http":
                        # 执行http
                        url = target.source_field.format(**row)
                        self.msgLog("调用http接口: {}".format(url), steptime, level="DEBUG")
                        import requests

                        headers = {}
                        for head in target.params:
                            headers[head.name] = head.param_type

                        if len(headers) == 0 and 'headers' in row:
                            headers = json.loads(row['headers'])

                        if "encode" not in row:
                            row['encode'] = "utf-8"

                        if 'body' not in row:
                            row['body'] = '{}'

                        if 'application/x-www-form-urlencoded' in headers["Content-Type"]:
                            data =json.loads(row['body'])
                        else:
                            data =row['body'].encode(row['encode'])

                        if 'req_method' in row and str(row['req_method']).upper() == "GET":
                            r = requests.get(url=url)
                        else:
                            r = requests.post(url=url,
                                              data=data,
                                              headers=headers)

                        row[target.target.format(**row)] = escape_string(repr(r.text))
                        row["{}_unescape".format(target.target.format(**row))] = r.text

                    elif target.type_ == "WebService":
                        # 执行WebService
                        url = target.source_field.format(**row)
                        self.msgLog("调用WebService接口: {}".format(url), steptime, level="DEBUG")

                        doctor = None
                        # imp = Import('http://www.w3.org/2001/XMLSchema',
                        #              location='http://www.w3.org/2001/XMLSchema.xsd')
                        # imp.filter.add('http://WebXml.com.cn/')
                        # doctor = ImportDoctor(imp)
                        r = run_case(url, row['method'], json.loads(row['body']), doctor)
                        from suds import sudsobject
                        if str(type(r)) == "<class 'suds.sudsobject.ArrayOfString'>":
                            row[target.target.format(**row)] = escape_string(repr(sudsobject.asdict(r)))
                            row["{}_unescape".format(target.target.format(**row))] = sudsobject.asdict(r)
                        else:
                            row[target.target.format(**row)] = escape_string(repr(r))
                            row["{}_unescape".format(target.target.format(**row))] = r

                    elif target.type_ == "jsonParser":
                        # 执行jsonParser解析JSON
                        target_field = target.target.format(**row)
                        self.msgLog("开始调用JSON解析器: {}".format(row[target.source_field]), steptime, level="DEBUG")
                        #json解析字符串增加根节点root
                        row["{}_unescape".format(target.source_field)] = '{{"root":{}}}'.format(row["{}_unescape".format(target.source_field)])
                        row[target.source_field] = '{{"root":{}}}'.format(row[target.source_field])

                        if "{}_unescape".format(target.source_field) in row:
                            json_obj_o_ = json.loads(row["{}_unescape".format(target.source_field)])
                        else:
                            json_obj_o_ = json.loads(row[target.source_field])
                        items_ = []
                        item_ = {}

                        for p in target.params:
                            tmp_items_ = get_json_val(json_obj_o_, {"match_path": p.name, "map_field": p.param_type})
                            if isinstance(tmp_items_, dict):
                                item_.update(tmp_items_)
                            elif isinstance(tmp_items_, list):
                                items_.extend(tmp_items_)

                        for ind, val in enumerate(items_):
                            items_[ind] = dict(item_, **val)
                        if len(items_) == 0:
                            items_ = [item_]

                        row[target_field] = items_
                    elif target.type_ == "xmlParser":
                        # 执行jsonParser解析JSON
                        target_field = target.target.format(**row)
                        self.msgLog("开始调用XML解析器: {}".format(row[target.source_field]), steptime, level="DEBUG")

                        root = None
                        root_unescape = None
                        if "{}_unescape".format(target.source_field) in row:
                            root_unescape = row["{}_unescape".format(target.source_field)]
                        else:
                            root_unescape = row[target.source_field]

                        if "encode" in row and str(type(root_unescape)) != "<class 'dict'>":
                            root = etree.XML(root_unescape.encode("{}".format(row["encode"])))
                        else:
                            root = etree.XML(root_unescape)

                        items_ = []
                        item_ = {}

                        for p in target.params:
                            items_ = get_xml_val(root, {"match_path": p.name, "map_field": p.param_type}, item_, items_)

                        for ind, val in enumerate(items_):
                            items_[ind] = dict(item_, **val)
                        if len(items_) == 0:
                            items_ = [item_]

                        row[target_field] = items_
                    elif target.type_ == "list_2_table":
                        # 数组存储到表
                        # print(type(row[target.source_field][0]['road']))
                        # row[target.source_field][0]['road'] = ''
                        # row[target.source_field][0]['assistant_action'] = ''
                        # print(row[target.source_field])

                        # df = json_normalize(row[target.source_field])
                        df = pd.DataFrame(row[target.source_field])
                        # df.columns = ['instruction','orientation','road','distance','duration','polyline','action','assistant_action','walk_type']
                        # df = pd.DataFrame(json.loads(res.text)["result"]["siteList"])
                        df.to_sql(name=target.target.format(**row), con=databases[target.conName].getEngine(),
                                  index=False, if_exists='append')
                    elif target.type_ == "export":
                        output_file = f"{row['file_name']}.csv"
                        control_file = f"{row['file_name']}.ctl"
                        exe_sql = row['exe_sql']
                        start_time = datetime.datetime.now()
                        self.msgLog("开始导出表数据: {}".format(output_file), steptime, level="DEBUG")
                        connection = sourceConnection
                        with connection.cursor(pymysql.cursors.SSCursor) as cursor, open(output_file, 'w', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file, delimiter=row['delimiter'])
                            cursor.execute(exe_sql)
                            columns = [desc[0] for desc in cursor.description]
                            # writer.writerow(columns) 
                            row_count = 0
                            buffer = []
                            batch_size = 100000
                            for col in cursor:
                                buffer.append(col)
                                row_count += 1
                                if len(buffer) >= batch_size:
                                    writer.writerows(buffer)
                                    buffer = []
                            if buffer:
                                writer.writerows(buffer)

                            elapsed_time = datetime.datetime.now() - start_time
                        with open(control_file, 'w', encoding='utf-8') as ctl_file:
                            ctl_file.write(f"导出文件: {output_file}\n")
                            ctl_file.write(f"导出时间: {datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}\n")
                            ctl_file.write(f"导出查询: {exe_sql}\n")
                            ctl_file.write(f"导出列: {', '.join(columns)}\n")
                            ctl_file.write(f"总行数: {row_count}\n")
                            ctl_file.write(f"总耗时：{elapsed_time.total_seconds()} 秒")
                    elif target.type_ == "procedure":
                        # 给参数赋值，新建出参
                        if databases[target.conName].type_ == "oracle":
                            # 获取参数清单
                            try:
                                args = []
                                p_out = {}
                                for p in target.params:
                                    # 20220609 add upper 对大小写不做限制
                                    if p.in_out.upper() == "IN":
                                        args.append(row[p.name])
                                    elif p.in_out.upper() == "OUT":
                                        p_type = cx_type[p.param_type.upper()]
                                        p_out[p.name] = target.get_conn().cursor().var(p_type)
                                        args.append(p_out[p.name])
                                # 调用存储过程
                                steptime = self.msgLog("开始调用存储过程: {}".format(target.target), steptime, level="DEBUG")
                                target.get_conn().cursor().callproc(target.target, args)  # ['Nick', 'Nick, Good Morning!']
                                for p in p_out.keys():
                                    p_out[p] = p_out[p].getvalue()
                                row.update(p_out)

                                row["_status"] = '1'
                                row["_errmsg"] = '执行成功'

                            except (Exception) as e1:
                                # 无返回值
                                # print("执行的SQL：{}".format(sql))
                                row["_status"] = '2'
                                row["_errmsg"] = escape_string(repr(e1))
                                logging.error("SQL执行失败：{}，执行的SQL：{}".format(traceback.format_exc(), sql))

                            steptime = self.msgLog("调用存储过程: {}，完成".format(target.target), steptime, level="DEBUG")
                        # elif databases[target.conName].type_ == "sqlserver":
                        #     p_out = {}
                        #     # 调用存储过程
                        #     steptime = self.msgLog("开始调用存储过程: {}".format(target.target), steptime, level="DEBUG")
                        #     df = pd.read_sql(target.target.format(**row), con=target.getConn())
                        #     var_cols = df.columns.values
                        #     result = next(df.iterrows())[1]
                        #     for col in var_cols:
                        #         p_out[col] = result[col]
                        #
                        #     # 处理未命名的出参
                        #     k = 1
                        #     # bug key "" 不存在时会报错,已修复
                        #     if "" in result.index.values:
                        #         for col in result[""]:
                        #             # 有的存储过程返回参数是没有字段名的，这里默认给 result_打头的字段名
                        #             colname = "result_{}".format(k)
                        #             k = k + 1
                        #             p_out[colname] = col
                        #
                        #     row.update(p_out)
                        #     steptime = self.msgLog("调用存储过程: {}，完成".format(target.target), steptime, level="DEBUG")
                        elif databases[target.conName].type_ in ("mysql", "sqlserver"):
                            # 有返回值
                            if 'Create Table' in row:
                                row['Create Table'] = row['Create Table'].replace("'", "''")
                            sql = target.target.format(**row).replace("'None'", "null").replace("None", "null")
                            try:
                                # 20220608 添加对null的转义
                                p_out = {}
                                steptime = self.msgLog("开始调用存储过程: {}".format(target.target), steptime, level="DEBUG")
                                df = pd.read_sql(sql, con=target.get_conn())
                                var_cols = df.columns.values
                                result = next(df.iterrows())[1]
                                # if 'create_sql' in row:
                                #     print("create_sql:".format(row['create_sql']))
                                # if 'show_sql' in row:
                                #     print("show_sql:".format(row['show_sql']))

                                for col in var_cols:
                                    p_out[col] = result[col]

                                # 处理未命名的出参
                                k = 1
                                # bug key "" 不存在时会报错,已修复
                                if "" in result.index.values:
                                    for col in result[""]:
                                        # 有的存储过程返回参数是没有字段名的，这里默认给 result_打头的字段名
                                        colname = "result_{}".format(k)
                                        k = k + 1
                                        p_out[colname] = col

                                row.update(p_out)

                                row["_status"] = '1'
                                row["_errmsg"] = '执行成功'

                            except (Exception) as e1:
                                # 无返回值
                                # print("执行的SQL：{}".format(sql))
                                row["_status"] = '2'
                                row["_errmsg"] = escape_string(repr(e1))
                                logging.error("SQL执行失败：{}，执行的SQL：{}".format(traceback.format_exc(), sql))
                            steptime = self.msgLog("调用存储过程: {}，完成".format(target.target), steptime, level="DEBUG")

                    if i % 1 == 0 and target.conName is not None:
                        target.get_conn().commit()
                i = i + 1

            for target in self.target:
                if target.conName is not None:
                    target.get_conn().commit()
            steptime = self.msgLog("轮询同步数据完成", steptime)

    def __init__(self, name, job_type, src_con_name, source_sql, tos, transforms=None, param_con_name='', param_sql='',
                 is_fail_continue='N'):
        if transforms is None:
            transforms = []
        self.transforms = transforms
        self.source = self.From(src_con_name, source_sql)
        self.target = tos
        self.jobParams = self.JobParams(param_con_name, param_sql, is_fail_continue)
        self.name = name
        self.jobType = job_type
