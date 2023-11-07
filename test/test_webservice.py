import json
import os

import requests
from pandas import json_normalize

from Job import Job
import cx_Oracle
import petl as etl
import pymysql
import cx_Oracle
import datetime
import sys
from xml.dom.minidom import parse
from Database import *
from Job import Job
import logging
import pandas as pd
from datetime import datetime
from datetime import timedelta


# 执行用例
def run_case(url, method, params, doctor):
    values = tuple(params.values())  # 存储传进来的json的value
    client = Client(url, doctor=doctor)
    fName = "client.service." + method
    try:
        # 执行接口方法，方法名为变量，参数值也为变量，这样可用于不同ws接口测试
        result = eval(fName)(*values)
        ret = result
    except Exception as e:
        ret = {"code": "-1", "errMsg": str(e)}
    return ret


if __name__ == '__main__':
    from suds.client import Client
    from suds import sudsobject
    import time

    from suds.xsd.doctor import ImportDoctor, Import

    # imp = Import('http://www.w3.org/2001/XMLSchema', location='http://www.w3.org/2001/XMLSchema.xsd')
    # imp.filter.add('http://WebXml.com.cn/')
    # doctor = ImportDoctor(imp)

    # url = 'http://www.webxml.com.cn/WebServices/IpAddressSearchWebService.asmx?wsdl'
    url = 'http://192.168.41.1:8080/?wsdl'

    client = Client(url)
    res = client.service.get_bingqu_huanzhe("105")
    if str(type(res)) == "<class 'suds.sudsobject.ArrayOfString'>":
        res = sudsobject.asdict(res)

    print(res)

    # r = requests.post('http://httpbin.org/post', data={"name":"mmmmmmiiiiii","con":"hello"})
    # print(r.text)

    # r = requests.post('http://www.webxml.com.cn/WebServices/TranslatorWebService.asmx/getEnCnTwoWayTranslator?Word=string',
    #                   data={"Word": "string"})
    # print(r.text)
