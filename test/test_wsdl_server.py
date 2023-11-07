#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time : 3/3/2022 10:24 AM 
# @Author : lubinsu 
# @File : test_wsdl_server.py
# @desc :


import json
from xml import etree

from spyne import Application, rpc, ServiceBase, Iterable, Integer, Unicode, String
# 如果支持soap的协议需要用到Soap11
from spyne.protocol.soap import Soap11
# 可以创建一个wsgi服务器，做测试用
from spyne.server.wsgi import WsgiApplication
from Database import *
import pandas as pd
import xmltodict


def jsonToXml(json_str):
    xml_str = xmltodict.unparse(json_str)
    return xml_str


class HelloWorldService1(ServiceBase):
    # 输入和输出的类型，这里返回值是stringArray
    @rpc(Unicode, _returns=Unicode)
    def get_bingqu_huanzhe(self, bingqu_id):
        # str为传输的数据类型，data就是传输的数据，方法即为get_bingqu_huanzhe

        # 写你的服务端对应的操作
        # data1 = json.loads(data, strict=False)
        db = Database("name", "ydhl", "mysql", "192.168.41.139", "3306", "root", encode("123456"))
        df = pd.read_sql(
            "SELECT id,patient_id,zhuyuan_id,yiliaobaoxian_id,yiliaofukuanfangshi,zhuyuan_cishu,ruyuan_qingkuang,ruyuan_tujing,waiyuanzhenzhi,DATE_FORMAT(ruyuan_riqi_time,'%Y-%m-%d %H:%i:%s') ruyuan_riqi_time,bingchuang_hao,bingchuang_order,DATE_FORMAT(chuyuan_riqi_time,'%Y-%m-%d %H:%i:%s') chuyuan_riqi_time,chuyuan_fangshi,zhiliao_leibie,zhenduan,zhuangtai,guidang_zhuangtai,zhuzhenyishi,zhuzhenyishi_name,zerenhushi,special_info,hulijibie,his_display_zhuyuanhao,bingchuang_fenzu,blood_type,guominshi,zhusu,DATE_FORMAT(modify_time,'%Y-%m-%d %H:%i:%s') modify_time,yiyuan_id,release_seat,suifang_zhuangtai,suifang_hushi_id,suifang_time,suifang_shichang,bingqu_id,bingqu_name,keshi_id,keshi_name,face_feature,wandai_tiaoma,face_image FROM `zhuyuan_basic_info` a where a.bingqu_id = '{}' and a.zhuangtai = '住院中'".format(bingqu_id),
            con=db.getConnection())

        datas = "{ \"root\" : { \"status_code\": \"200\", \"data\": " + df.to_json(orient='records', force_ascii=False) + "}}"

        # data1=data
        # datasend.datasend(data1)
        return jsonToXml(json.loads(datas))
        # return datas


application = Application([HelloWorldService1], 'http://schemas.xmlsoap.org/soap/envelope',
                          in_protocol=Soap11(validator='lxml'), out_protocol=Soap11())
wsgi_application = WsgiApplication(application)

if __name__ == '__main__':
    import logging
    # wsgiref是python内置的一个简单的、遵循wsgi接口的服务器。
    from wsgiref.simple_server import make_server

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)
    logging.info("listening to http://10.77.77.244:8080")
    logging.info("wsdl is at: http://10.77.77.244:8080/?wsdl")
    # 127.0.0.1改成你的IP，让客户端所在电脑能访问就行
    server = make_server('10.77.77.244', 8080, wsgi_application)
    server.serve_forever()
