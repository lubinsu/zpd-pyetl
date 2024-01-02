# @Time : 7/07/2021 09:14 AM
# @Author : lubinsu
# @File : utils.py
# @desc : utils工具包

from Job import *
from Transform import Transform
from datetime import timedelta
'''
数据库配置方式：获取job所需的各项数据库配置，前提是需要先配置db.xml,用来连接ETL资料库
'''


def get_databases_by_db(db):
    databases = {}
    database_list = etl.fromdb(db.getConnection(),
                               "SELECT NAME, DB, DBTYPE, HOST, PORT , USER, PASSWORD, BACK_HOST FROM py_connections conn INNER JOIN py_dbtype dt ON conn.db_type_id = dt.id")
    for item in etl.dicts(database_list):
        name = item['NAME']
        db = item['DB']
        type_ = item['DBTYPE']
        host = item['HOST']
        port = item['PORT']
        user = item['USER']
        password = item['PASSWORD']
        back_host = item['BACK_HOST']

        databases[name] = Database(name, db, type_, host, port, user, password, back_host)

    return databases


def get_databases_by_db_rest(db, databases):
    database_list = etl.fromdb(db.getConnection(),
                               "SELECT NAME, DB, DBTYPE, HOST, PORT , USER, PASSWORD, BACK_HOST FROM py_connections conn INNER JOIN py_dbtype dt ON conn.db_type_id = dt.id")
    for item in etl.dicts(database_list):
        name = item['NAME']
        db = item['DB']
        type_ = item['DBTYPE']
        host = item['HOST']
        port = item['PORT']
        user = item['USER']
        password = item['PASSWORD']
        back_host = item['BACK_HOST']

        if name not in databases.keys():
            databases[name] = Database(name, db, type_, host, port, user, password, back_host)



def get_conn(name, item):
    db = item.getElementsByTagName("db")[0].childNodes[0].data
    type_ = item.getElementsByTagName("dbType")[0].childNodes[0].data
    host = item.getElementsByTagName("host")[0].childNodes[0].data
    port = item.getElementsByTagName("port")[0].childNodes[0].data
    user = item.getElementsByTagName("user")[0].childNodes[0].data
    password = item.getElementsByTagName("password")[0].childNodes[0].data

    return Database(name, db, type_, host, port, user, password)

'''
检查服务状态
'''
def check_status(db):

    cur = db.getConnection().cursor()
    cur.execute("SELECT f_check_rest_status()")

    results = cur.fetchall()
    status_code = "0"
    for row in results:
        status_code = row[0]
        # print("status_code:" + str(status_code))

    cur.close()
    db.getConnection().commit()
    return status_code


'''
xml文件配置方式：获取job所需的各项数据库配置
'''


def get_databases(document):
    databases = {}
    database_list = document.getElementsByTagName("connections")[0].getElementsByTagName("connection")
    for item in database_list:
        name = item.getElementsByTagName("name")[0].childNodes[0].data
        databases[name] = get_conn(name, item)
    return databases


'''
数据库配置方式：从资料库获取job任务的各项配置，前提是需要先配置db.xml,用来连接ETL资料库
'''


def get_jobs_by_db(db, job_chain):
    jobs = {}
    logging.debug("获取job列表")
    job_list = etl.dicts(etl.fromdb(db.getConnection(),
                                    " SELECT j.id job_id, j.name, j.is_fail_continue, t.jobType, conn1.name AS from_db, j.source_sql "
                                    " FROM py_jobs j "
                                    "   INNER JOIN py_connections conn1 ON j.source_conn_id = conn1.id "
                                    "   INNER JOIN py_jobtype t ON j.job_type_id = t.id "
                                    " WHERE j.state = 'Y' AND j.name in({})".format(
                                        ",".join(map(lambda x: "'{}'".format(x), job_chain.split("->"))))
                                    ))
    for item in job_list:
        name = item["name"]
        jobType = item["jobType"]
        is_fail_continue = item["is_fail_continue"]

        logging.debug("获取每个job详细步骤")
        sql2 = "SELECT tr.job_id, tr.id trans_id, tr.`name`, tt.trans_type_name transType, conn2.name AS trans_from_db, tr.from_sql, conn3.name AS trans_to_db, tr.to_target, tr.order " \
               "FROM py_transforms tr " \
               "	LEFT JOIN py_connections conn2 ON tr.from_conn_id = conn2.id " \
               "	LEFT JOIN py_connections conn3 ON tr.to_conn_id = conn3.id " \
               "	LEFT JOIN py_trans_type tt ON tr.type_id = tt.id " \
               "WHERE tr.job_id = {} " \
               "AND tr.state = 'Y' " \
               "ORDER BY tr.order ASC ".format(item["job_id"])
        # trans_ordered = etl.fromdb(db.getConnection(), sql2)
        # trans_ordered_dict = etl.dicts(trans_ordered)
        df = pd.read_sql(sql2, con=db.getConnection())
        if jobType == "syn":
            from_con_name = item["from_db"]
            from_sql = item["source_sql"]

            to = next(df.iterrows())[1]
            to_con_name = to["trans_to_db"]
            to_table = to["to_target"]

            jobs[name] = Job(name, jobType, from_con_name, from_sql, [Job.To(to_con_name, to_table)],
                             is_fail_continue=is_fail_continue)
        elif jobType == "syn_blob":
            from_con_name = item["from_db"]
            from_sql = item["source_sql"]

            to = next(df.iterrows())[1]
            to_con_name = to["trans_to_db"]
            to_table = to["to_target"]

            sql = "SELECT a.field_seq, a.field_name, a.is_blob " \
                  "FROM py_blob_fields a WHERE a.job_id = {} and a.state = 'Y' ".format(item["job_id"])

            blob_fileds = pd.read_sql(sql, con=db.getConnection())

            blob_seqs = []

            for index, row in blob_fileds.iterrows():
                b = BlobSeq(row["field_seq"], row["field_name"], row["is_blob"])
                blob_seqs.append(b)

            # params = []
            # tos.append(Job.To(conName, target, type_, paths, source_field=source_field))

            jobs[name] = Job(name, jobType, from_con_name, from_sql, [Job.To(to_con_name, to_table, params=blob_seqs)],
                             is_fail_continue=is_fail_continue)
        elif jobType == "syn_dynamic":

            transforms = []
            for index, transform in df.iterrows():
                name_ = transform["name"]
                job_type_ = transform["transType"]

                from_con_name = transform["trans_from_db"]
                from_sql = transform["from_sql"]

                to_con_name = transform["trans_to_db"]
                to_table = transform["to_target"]
                transforms.append(
                    Transform(name_, job_type_, from_con_name, from_sql, [Transform.To(to_con_name, to_table)]))

            params_conName = item["from_db"]
            params_sql = item["source_sql"]

            jobs[name] = Job(name, jobType, params_conName, params_sql, [], transforms,
                             is_fail_continue=is_fail_continue)
        elif jobType == "procedure":
            conName = item["from_db"]
            procName = item["source_sql"]

            jobs[name] = Job(name, jobType, "", "", [Job.To(conName, procName)], is_fail_continue=is_fail_continue)
        elif jobType == "stream":
            tos = []
            from_con_name = item["from_db"]
            from_sql = item["source_sql"]
            transforms = None

            for index, to_item in df.iterrows():
                conName = to_item["trans_to_db"]
                target = to_item["to_target"]
                type_ = to_item["transType"]
                if type_ == "sql":
                    tos.append(Job.To(conName, target, type_))

                elif type_ == "shell":
                    tos.append(Job.To(None, target, type_, source_field=from_sql))

                elif type_ in ['jsonParser', 'xmlParser']:

                    # 来源变量
                    source_field = to_item["from_sql"]
                    sql = "SELECT j.match_path, j.map_field FROM py_data_parser j " \
                          "where trans_id = {} and state = 'Y' order by `order` ".format(to_item["trans_id"])

                    paths_ordered = pd.read_sql(sql, con=db.getConnection())
                    paths = []
                    for index, param in paths_ordered.iterrows():
                        p = Param(param["match_path"], "None", param["map_field"])
                        paths.append(p)

                    tos.append(Job.To(conName, target, type_, paths, source_field=source_field))
                elif type_ == "list_2_table":

                    # 来源变量
                    source_field = to_item["from_sql"]
                    tos.append(Job.To(conName, target, type_, source_field=source_field))

                elif type_ == "javascript":
                    sql = "SELECT" \
                          "       tr.id," \
                          "       pvc.var_name," \
                          "       pvc.var_type " \
                          "FROM" \
                          "       py_transforms tr" \
                          "       INNER JOIN py_trans_type tp ON tr.type_id = tp.id" \
                          "       INNER JOIN py_variable_cfg pvc ON tr.id = pvc.trans_id " \
                          "WHERE" \
                          "       tp.trans_type_name = 'javascript'" \
                          "and tr.id = {}".format(to_item["trans_id"])
                    params_ordered = pd.read_sql(sql, con=db.getConnection())

                    procVars = []
                    for index, param in params_ordered.iterrows():
                        p = Param(param["var_name"], "None", param["var_type"])
                        procVars.append(p)
                    tos.append(Job.To(conName, target, type_, procVars))
                elif type_ == "http" or type_ == "WebService":
                    sql = "SELECT" \
                          "	tr.id," \
                          "	pvc.header_key," \
                          "	pvc.header_value " \
                          "FROM" \
                          "	py_transforms tr" \
                          "	INNER JOIN py_trans_type tp ON tr.type_id = tp.id" \
                          "	INNER JOIN py_header_cfg pvc ON tr.id = pvc.trans_id " \
                          "WHERE" \
                          "	tp.trans_type_name IN('http', 'WebService')" \
                          "AND tr.id = {}".format(to_item["trans_id"])
                    header_res = pd.read_sql(sql, con=db.getConnection())
                    headers = []
                    for index, param in header_res.iterrows():
                        header = Param(param["header_key"], "None", param["header_value"])
                        headers.append(header)

                    # 来源变量
                    source_field = to_item["from_sql"]
                    tos.append(Job.To(conName, target, type_, headers, source_field=source_field))
                elif type_ == "procedure":

                    sql = "SELECT " \
                          "	tr.id, prm.`name`, prm.in_out_type, prm.data_type,prm.order " \
                          "FROM " \
                          "	py_transforms tr " \
                          "	INNER JOIN py_trans_type tp ON tr.type_id = tp.id " \
                          "	INNER JOIN py_procedure_params prm ON tr.id = prm.trans_id  " \
                          "WHERE " \
                          "	tp.trans_type_name = 'procedure' " \
                          "and prm.state = 'Y' " \
                          "and tr.id = {} " \
                          "ORDER BY prm.`order` ASC".format(to_item["trans_id"])
                    params_ordered = pd.read_sql(sql, con=db.getConnection())

                    procVars = []
                    for index, param in params_ordered.iterrows():
                        p = Param(param["name"], param["in_out_type"], param["data_type"])
                        procVars.append(p)
                    tos.append(Job.To(conName, target, type_, procVars))

            jobs[name] = Job(name, jobType, from_con_name, from_sql, tos, transforms=transforms,
                             is_fail_continue=is_fail_continue)

    return jobs


def getJobs(document):
    jobs = {}
    jobList = document.getElementsByTagName("jobs")[0].getElementsByTagName("job")
    for item in jobList:
        name = item.getElementsByTagName("name")[0].childNodes[0].data
        jobType = item.getElementsByTagName("jobType")[0].childNodes[0].data
        if jobType == "syn":
            from_conName = item.getElementsByTagName("from")[0].getElementsByTagName("conName")[0].childNodes[0].data
            from_sql = item.getElementsByTagName("from")[0].getElementsByTagName("sql")[0].childNodes[0].data

            to_conName = item.getElementsByTagName("to")[0].getElementsByTagName("conName")[0].childNodes[0].data
            to_table = item.getElementsByTagName("to")[0].getElementsByTagName("target")[0].childNodes[0].data

            jobs[name] = Job(name, jobType, from_conName, from_sql, [Job.To(to_conName, to_table)])
        elif jobType == "syn_dynamic":

            transforms = []
            transformsList = item.getElementsByTagName("transforms")[0].getElementsByTagName("transform")
            for transform in transformsList:
                name_ = transform.getElementsByTagName("name")[0].childNodes[0].data
                jobType_ = transform.getElementsByTagName("jobType")[0].childNodes[0].data

                from_conName = transform.getElementsByTagName("from")[0].getElementsByTagName("conName")[0].childNodes[
                    0].data
                from_sql = transform.getElementsByTagName("from")[0].getElementsByTagName("sql")[0].childNodes[0].data

                to_conName = transform.getElementsByTagName("to")[0].getElementsByTagName("conName")[0].childNodes[
                    0].data
                to_table = transform.getElementsByTagName("to")[0].getElementsByTagName("target")[0].childNodes[0].data
                transforms.append(
                    Transform(name_, jobType_, from_conName, from_sql, [Transform.To(to_conName, to_table)]))

            params_conName = item.getElementsByTagName("params")[0].getElementsByTagName("conName")[0].childNodes[
                0].data
            params_sql = item.getElementsByTagName("params")[0].getElementsByTagName("sql")[0].childNodes[0].data

            jobs[name] = Job(name, jobType, params_conName, params_sql, [], transforms)
        elif jobType == "procedure":
            conName = item.getElementsByTagName("target")[0].getElementsByTagName("conName")[0].childNodes[0].data
            procName = item.getElementsByTagName("target")[0].getElementsByTagName("procName")[0].childNodes[0].data

            jobs[name] = Job(name, jobType, "", "", [Job.To(conName, procName)])
        elif jobType == "stream":
            tos = []
            from_conName = item.getElementsByTagName("from")[0].getElementsByTagName("conName")[0].childNodes[0].data
            from_sql = item.getElementsByTagName("from")[0].getElementsByTagName("sql")[0].childNodes[0].data

            toList = item.getElementsByTagName("tos")[0].getElementsByTagName("to")
            for item in toList:
                conName = item.getElementsByTagName("conName")[0].childNodes[0].data
                target = item.getElementsByTagName("target")[0].childNodes[0].data
                type_ = item.getElementsByTagName("type")[0].childNodes[0].data
                if type_ == "sql":
                    tos.append(Job.To(conName, target, type_))
                elif type_ == "procedure":
                    params = item.getElementsByTagName("params")[0].getElementsByTagName("param")
                    procVars = []
                    for param in params:
                        l = param.childNodes[0].data.split("|")
                        p = Param(l[0], l[1], l[2])
                        procVars.append(p)
                    tos.append(Job.To(conName, target, type_, procVars))

            jobs[name] = Job(name, jobType, from_conName, from_sql, tos)

    return jobs


def get_db_config(document):
    item = document.getElementsByTagName("connection")[0]
    name = item.getElementsByTagName("name")[0].childNodes[0].data
    return get_conn(name, item)

def del_log(db):
    n_days_ago = str(int((datetime.datetime.now() - timedelta(days=7)).strftime("%d")))
    one_days_ago = str((datetime.datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
    now_date_file = str(datetime.datetime.now().strftime("%Y-%m-%d"))

    if not os.path.exists("{}.lock".format(now_date_file)):
        logging.info("删除历史日志：P{}".format(n_days_ago))
        # 删除前一天的文件
        if os.path.exists("{}.lock".format(one_days_ago)):
            os.remove("{}.lock".format(one_days_ago))
        file = open("{}.lock".format(now_date_file), 'w')
        file.close()
        trunc_sql = "ALTER TABLE py_etl_run_log TRUNCATE PARTITION P{}".format(n_days_ago)
        cursor = db.getConnection().cursor()
        cursor.execute(trunc_sql)


def get_log_level(document):
    log_levels = {"console_level": "INFO", "db_level": "INFO"}
    try:
        item = document.getElementsByTagName("log.level")[0]
        log_levels["console_level"] = item.getElementsByTagName("console_level")[0].childNodes[0].data
        log_levels["db_level"] = item.getElementsByTagName("db_level")[0].childNodes[0].data
    except (Exception) as e:
        log_levels = {"console_level": "INFO", "db_level": "INFO"}

    return log_levels

def get_process_count(document):
    item = document.getElementsByTagName("processes")[0]
    process_count = item.getElementsByTagName("count")[0].childNodes[0].data
    return int(process_count)