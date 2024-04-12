import datetime
import json
import time
from collections import OrderedDict

import xmltodict
from lxml.etree import _Element
from pandas import json_normalize
from Database import *
import pandas as pd

from Database import *

'''
if __name__ == '__main__':
    conn = cx_Oracle.Connection("{}/{}@{}:{}/{}".format("dwd", "dwd123", "192.168.2.45", int(1521), "orcl"))
    cursor = conn.cursor()
    # 声明变量
    a = {}
    a['s_no'] = '001'  # plsql入参
    a['s_name'] = cursor.var(cx_Oracle.STRING)  # plsql出参
    # 调用存储过程
    args = []
    args.append(a['s_no'])
    args.append(a['s_name'])
    cursor.callproc('p_get_student_name', args)  # ['Nick', 'Nick, Good Morning!']
    # 打印返回值
    print(a['s_name'])
    # <cx_Oracle.STRING with value 'Nick, Good Morning!'>
    print(a['s_name'].getvalue())
    # Nick, Good Morning!
    # 资源关闭
    cursor.close()
    conn.close()
'''

'''
if __name__ == '__main__':
    connection1 = pymysql.connect(password="123456", database="etl_interface", user="root", host="192.168.41.135", port=3306)

    df = pd.read_sql("select * from py_transforms tr order by job_id asc, tr.order desc", con = connection1)
    head_row = next(df.iterrows())[1]
    print(head_row['job_id'], head_row['order'])
    # for index, row in head_row.iterrows():
    #     print(row["job_id"],row["order"],)

    # print(head_row['job_id'],head_row['to_target'], head_row['order'])
    for index, row in df.iterrows():
        print(row["job_id"],row["order"],)
'''
'''if __name__ == '__main__':

    thirty_days_ago = str(int((datetime.now() - timedelta(days=30)).strftime("%d")))
    one_days_ago = str((datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))
    now_date_file = str(datetime.now().strftime("%Y-%m-%d"))

    if not os.path.exists("{}.lock".format(now_date_file)):
        # 删除前一天的文件
        if os.path.exists("{}.lock".format(one_days_ago)):
            os.remove("{}.lock".format(one_days_ago))
        file = open("{}.lock".format(now_date_file), 'w')
        file.close()
        trunc_sql = "ALTER TABLE py_etl_run_log TRUNCATE PARTITION P{}".format(thirty_days_ago)
        print(trunc_sql)
'''


# if __name__ == '__main__':
#     job_chain = "同步log_record->p_get_student_name"
#     print(",".join(map(lambda x: "'{}'".format(x), job_chain.split("->"))))
#     print(" SELECT j.id job_id, j.name, t.jobType, conn1.name AS from_db, j.source_sql "
#                                     " FROM py_jobs j "
#                                     "   INNER JOIN py_connections conn1 ON j.source_conn_id = conn1.id "
#                                     "   INNER JOIN py_jobtype t ON j.job_type_id = t.id "
#                                     " WHERE j.state = 'Y' AND j.name in({})".format(",".join(map(lambda x: "'{}'".format(x), job_chain.split("->")))))

# if __name__ == '__main__':
#     conn = pymssql.connect(host="192.168.2.45", user="sa", password="zpd1qaz@WSX",
#                            database="test", port=1433, charset="utf8")
#
#     cur = conn.cursor()
#
#     # sql = 'DECLARE @return_value int,@result_code nvarchar(1024),@result_msg nvarchar(1024);' \
#     #       'EXEC @return_value = [AppData].[dbo].[Fyit_AddDept] @parentid = {0},@dept = N\'{1}\',@display = 1,@result_code = @result_code OUTPUT,@result_msg = @result_msg OUTPUT;' \
#     #       'SELECT    @result_code as N\'@result_code\',@result_msg as N\'@result_msg\',@return_value as N\'Return Value\';'
#
#     sql = "declare @resutl nvarchar(32);" \
#           "exec write_back_tw '{}',@resutl OUTPUT;" \
#           "SELECT @resutl as tw;"
#
#     df = pd.read_sql(sql.format("006"), con=conn)
#     # cur.execute(sql.format("006"))
#     # result = cur.fetchall()
#     # for index, row in df.iteritems():
#     #     print(index)
#     var_cols = df.columns.values
#     result = next(df.iterrows())[1]
#     for col in var_cols:
#         print("体温单查询结果，字段：{}，体温:{}".format(col, result[col]))
#     # for index, transform in df.iterrows():
#     #     print(transform["resutl"])
#     # result = next(df.iterrows())[1]
#     # print(df)
#     # print(result)

# 执行用例
# def run_case(url, method, params, doctor):
#     values = tuple(params.values())  # 存储传进来的json的value
#     client = Client(url, doctor=doctor)
#     fName = "client.service." + method
#     try:
#         # 执行接口方法，方法名为变量，参数值也为变量，这样可用于不同ws接口测试
#         result = eval(fName)(*values)
#         ret = result
#     except Exception as e:
#         ret = {"code": "-1", "errMsg": str(e)}
#     return ret


# if __name__ == '__main__':
#     from suds.client import Client
#     import time
#
#     from suds.xsd.doctor import ImportDoctor, Import
#
#     imp = Import('http://www.w3.org/2001/XMLSchema', location='http://www.w3.org/2001/XMLSchema.xsd')
#     imp.filter.add('http://WebXml.com.cn/')
#     doctor = ImportDoctor(imp)
#
#     url = 'http://www.webxml.com.cn/WebServices/WeatherWebService.asmx?wsdl'
#
#     start_time = time.clock()
#     print(run_case(url, 'getSupportCity', {"byProvinceName": "黑龙江"}, doctor))
#     print("%s cost %s second" % (os.path.basename(sys.argv[0]), time.clock() - start_time))
#

# if __name__ == '__main__':
#     str = '<?xml version="1.0" encoding="utf-8"?> ' \
#           '<collection shelf="New Arrivals"><movie title="Enemy Behind"><type>War, Thriller</type><format>DVD</format><year>2003</year><rating>PG</rating><stars>10</stars><description>Talk about a US-Japan war</description></movie><movie title="Transformers"><type>Anime, Science Fiction</type><format>DVD</format><year>1989</year><rating>R</rating><stars>8</stars><description>A schientific fiction</description></movie><movie title="Trigun"><type>Anime, Action</type><format>DVD</format><episodes>4</episodes><rating>PG</rating><stars>10</stars><description>Vash the Stampede!</description></movie><movie title="Ishtar"><type>Comedy</type><format>VHS</format><rating>PG</rating><stars>2</stars><description>Viewable boredom</description></movie></collection>'
#     import xml.dom.minidom
#     from xml.dom.minidom import parse
#
#     # 使用minidom解析器打开 XML 文档
#     DOMTree = xml.dom.minidom.parseString(str)
#     collection = DOMTree.documentElement
#     if collection.hasAttribute("shelf"):
#         print("Root element : %s" % collection.getAttribute("shelf"))
#
#     # 在集合中获取所有电影
#     movies = collection.getElementsByTagName("movie")
#
#     print(movies[0].getAttribute("title"))
#
#     # 打印每部电影的详细信息
#     for movie in movies:
#         print("*****Movie*****")
#         if movie.hasAttribute("title"):
#             print("Title: %s" % movie.getAttribute("title"))
#
#         type = movie.getElementsByTagName('type')[0]
#         print("Type: %s" % type.childNodes[0].data)
#         format = movie.getElementsByTagName('format')[0]
#         print("Format: %s" % format.childNodes[0].data)
#         rating = movie.getElementsByTagName('rating')[0]
#         print("Rating: %s" % rating.childNodes[0].data)
#         description = movie.getElementsByTagName('description')[0]
#         print("Description: %s" % description.childNodes[0].data)


# if __name__ == '__main__':
#     json_str = '{"statusCode":"000000","desc":"请求成功","result":{"pm":{"aqi":"65","area":"南京","area_code":"nanjing",' \
#                '"co":"1.1","ct":"2022-01-14 13:40:01.954","no2":"46","num":"145","o3":"35","o3_8h":"31","pm10":"73",' \
#                '"pm2_5":"47","primary_pollutant":"颗粒物(PM2.5)","quality":"良好","so2":"7"},"ret_code":0,"siteList":[{' \
#                '"aqi":"50","co":"0.4","ct":"2021-12-27 09:20:37.525","no2":"37","o3":"13","o3_8h":"36","pm10":"50",' \
#                '"pm2_5":"19","primary_pollutant":"-","quality":"优质","site_name":"六合雄州","so2":"2"},{"aqi":"50",' \
#                '"co":"0.8","ct":"2021-12-27 09:20:37.528","no2":"39","o3":"25","o3_8h":"30","pm10":"50","pm2_5":"34",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"瑞金路","so2":"3"},{"aqi":"46","co":"0.6",' \
#                '"ct":"2021-12-27 09:20:37.531","no2":"55","o3":"24","o3_8h":"29","pm10":"46","pm2_5":"29",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"山西路","so2":"4"},{"aqi":"45","co":"0.2",' \
#                '"ct":"2021-12-27 09:20:37.533","no2":"35","o3":"33","o3_8h":"32","pm10":"45","pm2_5":"29",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"玄武湖","so2":"4"},{"aqi":"44","co":"0.6",' \
#                '"ct":"2021-12-27 09:20:37.535","no2":"58","o3":"8","o3_8h":"31","pm10":"44","pm2_5":"19",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"仙林大学城","so2":"8"},{"aqi":"44","co":"0.6",' \
#                '"ct":"2021-12-27 09:20:37.538","no2":"33","o3":"19","o3_8h":"31","pm10":"44","pm2_5":"23",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"溧水永阳","so2":"6"},{"aqi":"42","co":"0.6",' \
#                '"ct":"2021-12-27 09:20:37.540","no2":"26","o3":"41","o3_8h":"33","pm10":"40","pm2_5":"29",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"江宁彩虹桥","so2":"6"},{"aqi":"42","co":"0.5",' \
#                '"ct":"2021-12-27 09:20:37.542","no2":"35","o3":"33","o3_8h":"32","pm10":"39","pm2_5":"29",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"草场门","so2":"6"},{"aqi":"41","co":"0.8",' \
#                '"ct":"2021-12-27 09:20:37.545","no2":"29","o3":"40","o3_8h":"34","pm10":"41","pm2_5":"24",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"中华门","so2":"3"},{"aqi":"39","co":"0.3",' \
#                '"ct":"2021-12-27 09:20:37.547","no2":"16","o3":"37","o3_8h":"34","pm10":"39","pm2_5":"22",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"浦口","so2":"5"},{"aqi":"38","co":"0.4",' \
#                '"ct":"2021-12-27 09:20:37.549","no2":"39","o3":"31","o3_8h":"34","pm10":"38","pm2_5":"26",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"高淳老职中","so2":"12"},{"aqi":"36","co":"0.2",' \
#                '"ct":"2021-12-27 09:20:37.552","no2":"34","o3":"39","o3_8h":"37","pm10":"36","pm2_5":"22",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"奥体中心","so2":"10"},{"aqi":"31","co":"1.1",' \
#                '"ct":"2021-12-27 09:20:37.554","no2":"40","o3":"16","o3_8h":"35","pm10":"31","pm2_5":"19",' \
#                '"primary_pollutant":"-","quality":"优质","site_name":"迈皋桥","so2":"4"}]}}'
#     json_obj = json.loads(json_str)
#     a = 1
#     b = 5
#     print(json_obj['result']["siteList"][a:b])
#     c = [{'field1':'xxxx1'}, {'field1':'xxxx2'}]
#     print("aaaaa.{c[0][field1]}".format(**{"c": c}))

def xml_element_to_dict(elem):
    "Convert XML Element to a simple dict"
    inner = dict(elem.attrib)
    children = map(xml_element_to_dict, elem)
    text = elem.text and elem.text.strip()
    if text:
        inner = text
    if children:
        inner = children

    if type(inner) == str:
        return inner
    else:
        return {elem.tag: inner}


# if __name__ == '__main__':
#     from lxml import etree
#
#     text = """<?xml version="1.0" encoding="UTF-16"?>
# <VitalSigns>
# 	<PatientName>刘淑平</PatientName>
# 	<PatientID>000322337900</PatientID>
# 	<VisitID>2</VisitID>
# 	<BabyFlag>0</BabyFlag>
# 	<Ward>眼科一病房</Ward>
# 	<WardCode>1080102</WardCode>
# 	<BedNumber>2003</BedNumber>
# 	<InhospitalDate>2008/9/5 10:53:00</InhospitalDate>
# 	<VitalSigns>
# 		<Detail>
# 			<MeasureType>体温</MeasureType>
# 			<MeasureValue>39</MeasureValue>
# 			<MeasureDateTime>2012/7/12 2:00:00</MeasureDateTime>
# 			<OperName>嘉和</OperName>
# 		</Detail>
# 		<Detail>
# 			<MeasureType>物理降温</MeasureType>
# 			<MeasureValue>35</MeasureValue>
# 			<MeasureDateTime>2012/7/12 2:00:00</MeasureDateTime>
# 			<OperName>嘉和</OperName>
# 		</Detail>
# 		<Detail>
# 			<MeasureType>体温</MeasureType>
# 			<MeasureValue>42</MeasureValue>
# 			<MeasureDateTime>2012/7/12 6:00:00</MeasureDateTime>
# 			<OperName>嘉和</OperName>
# 		</Detail>
# 		<Detail>
# 			<MeasureType>物理降温</MeasureType>
# 			<MeasureValue>36</MeasureValue>
# 			<MeasureDateTime>2012/7/12 6:00:00</MeasureDateTime>
# 			<OperName>嘉和</OperName>
# 		</Detail>
# 		<Detail>
# 			<MeasureType>表顶注释</MeasureType>
# 			<MeasureValue>手术</MeasureValue>
# 			<MeasureDateTime>2012/11/17 2:00:00</MeasureDateTime>
# 			<OperName>嘉和</OperName>
# 		</Detail>
# 		<Detail>
# 			<MeasureType>表顶注释</MeasureType>
# 			<MeasureValue>手术</MeasureValue>
# 			<MeasureDateTime>2012/12/2 2:00:00</MeasureDateTime>
# 			<OperName>嘉和</OperName>
# 		</Detail>
# 		<Detail>
# 			<MeasureType>表顶注释</MeasureType>
# 			<MeasureValue>手术</MeasureValue>
# 			<MeasureDateTime>2012/12/8 2:00:00</MeasureDateTime>
# 			<OperName>嘉和</OperName>
# 		</Detail>
# 	</VitalSigns>
# </VitalSigns>
#             """
#
#     row = {"encode": "UTF-16"}
#     incode_ = ""
#     if "encode" in row:
#         incode_ = row["encode"]
#
#     root = None
#     if incode_ == "":
#         root = etree.XML(text)
#     else:
#         root = etree.XML(text.encode("{}".format(incode_)))
#
#     items_ = []
#     item_ = {}
#     map_field = "MeasureType"
#     r = root.xpath("/VitalSigns/VitalSigns/Detail[2]/MeasureType")
#     for xml_item in r:
#         if type(xml_item) == _Element:
#             res_xml = etree.tostring(xml_item)
#             xmlparse = xmltodict.parse(res_xml)
#             if type(xmlparse) == OrderedDict:
#                 for key,value in xmlparse.items():
#                     if type(xmlparse) == OrderedDict and len(xmlparse) > 1:
#                         print("{}:{}".format(key, value))
#                         items_.append(value)
#                     else:
#                         item_[key] = value
#         elif map_field is not None:
#             item_[map_field] = r[0]
#         # print(json.dumps(xmlparse))
#
#     db2 = Database("name", "ydhl", "mysql", "192.168.41.135", "3306", "root", encode("123456"))
#     connection2 = db2.getEngine()
#     df = json_normalize(items_)
#     # df = pd.DataFrame(json.loads(res.text)["result"]["siteList"])
#
#     df.to_sql('t_details', con=connection2, index=False, if_exists='append')
#
# if __name__ == '__main__':
#     db = Database("name", "ydhl", "mysql", "192.168.41.139", "3306", "root", encode("123456"))
#     bingqu_id = "105"
#     df = pd.read_sql(
#         "SELECT * FROM `zhuyuan_basic_info` a where a.bingqu_id = '{}' and a.zhuangtai = '住院中'".format(bingqu_id),
#         con=db.getConnection())
#
#     datas = "{\"status_code\": 200, \"data\": " + df.to_json(orient='records', force_ascii=False) + "}"
#
#     print(json.loads(datas))

# if __name__ == '__main__':
#     a = {'vaaa': [{'resultcode': '0'}]}
#     print("param:{vaaa[0][resultcode]}".format(**a))
#
#     # 生成一个字典
#     d = {'name': 'Tom', 'age': 10, 'Tel': 110}
#
#     # 打印返回值，其中d.keys()是列出字典所有的key
#     print('name' in d.keys())
#     print('name' in d)
#
#     if 'name2' not in d.keys():
#         print("不存在")
#     # 两个的结果都是返回True
#
#     zimu = "ABCDEFGHIJKLMNOPQRSTUVWXYZ Deadlock"
#     ex1 = "timed out"
#     ex2 = "Deadlock"
#     if ex1 in zimu or ex2 in zimu:
#         print("True")
#     else:
#         print("False")
#
#     print(str(datetime.datetime.now().strftime('%Y%m%d%H%M%S.%f')))
#     print(str(time.time()))

# if __name__ == '__main__':
#     import argparse
#     import sys
#     from datetime import datetime
#
#     print(sys.argv[3])
#
#     parser = argparse.ArgumentParser()
#
#     # 必填参数
#     # parser.add_argument("host", help="database host")
#     # 可选参数（`--`前缀）
#     parser.add_argument("--create", help="True or False")
#     # 类型要求（type=int）
#     parser.add_argument("--port", help="database port", type=int, default=3306)
#
#
#     def valid_datetime(s):
#         """
#         自定义的日期时间校验方法
#         :param s:
#         :return:
#         """
#         try:
#             return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
#         except ValueError:
#             msg = "Not a valid date: '{0}'.".format(s)
#             raise argparse.ArgumentTypeError(msg)
#
#
#     # 指定类型校验（type=valid_date）：自定义的校验方法valid_date
#     parser.add_argument("--begin", help="数据对账的开始时间（含）", default=None, type=valid_datetime)
#     parser.add_argument("--end", help="数据对账区间的结束时间（不含）", default=None, type=valid_datetime)
#
#     args = parser.parse_args(sys.argv[3:])
#     print(args.create)
#     print(args)
#
# connect = pymssql.connect(
#     server='223.220.200.105\PivasSqlServer',
#     port='1433',
#     user='ydhl',
#     password='ydhl',
#     database='ZHY_PIVAS_AYDEFY'
# )
# if connect:
#     print("连接成功!")
# import threading
# import time
#
#
# def task(n):
#     print(f"Task {n} is running")
#     time.sleep(2)
#     print(f"Task {n} is done")
#
#
# def main():
#     threads = []
#     for i in range(5):  # 假设我们要执行5个任务
#         t = threading.Thread(target=task, args=(i,))
#         threads.append(t)
#         t.start()
#
#     for t in threads:
#         t.join()  # 等待所有线程完成

def get_result():
    db = Database("name", "test", "mysql", "192.168.41.139", "3306", "root", encode("123456"))
    sql = "select '1' as errCode"
    cur = db.getConnection().cursor()
    if db.type_ == "oracle":
        row_count = cur.execute(sql)
    else:
        row_count = cur.execute(sql)
    result = cur.fetchone()
    print(result)


if __name__ == "__main__":
    get_result()
