#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time : 2022/1/14 13:58 
# @Author : lubinsu 
# @File : json_parser.py
# @desc : json解析工具
import json
from pandas import json_normalize

from Database import Database
from encryption.base64_encode import encode
import re


def get_json_val(json_obj_o, rst):
    item = {}
    items = []
    json_obj = json_obj_o
    match_path = rst["match_path"]
    nodes = match_path.split(".")
    map_field = rst["map_field"]

    n = len(nodes)
    counter = 0
    while counter < n:
        # 数组
        node = nodes[counter]
        # 获取数组, eg: result.siteList[1:2]
        if str(node).find('[') > 0 and str(node).find(':') > 0:
            node_scope = re.findall(r"\d+:?\d*", node)[0].split(":")
            node_name = re.sub('\[.*\]', '', node)
            # ns2 = node.replace("\[*.\]", node)
            json_obj = json_obj[node_name][int(node_scope[0]):int(node_scope[1])]
        # 获取数组, eg: result.siteList[*]
        elif str(node).find('[') > 0 and str(node).find(':') < 0 and str(node).find('[*]') < 0:
            node_scope = re.findall(r"\d+", node)[0]
            node_name = re.sub('\[.*\]', '', node)
            # ns2 = node.replace("\[*.\]", node)
            json_obj = json_obj[node_name][int(node_scope)]
        # 获取数组, eg: result.siteList[*]
        elif str(node).find('[*]') > 0:
            node_name = re.sub('\[.*\]', '', node)
            json_obj = json_obj[node_name]
        # 处理多字段列表 eg: result.siteList[*].aqi,co
        elif str(node).find(',') > 0:
            fields = node.split(",")

            if str(type(json_obj)) == "<class 'dict'>":
                row = {}
                for field in fields:
                    if field in json_obj.keys():
                        row[field] = json_obj[field]
                json_obj = row
            else:
                for index, value in enumerate(json_obj):
                    row = {}
                    for field in fields:
                        row[field] = value[field]
                    json_obj[index] = row

        elif counter <= n - 2:
            json_obj = json_obj[node]
        else:
            if map_field is not None:
                item[map_field] = json_obj[node]
            else:
                item[node] = json_obj[node]

        if counter == n - 1:
            items = [json_obj, item][len(item) > 0]
        counter += 1
    return items


if __name__ == '__main__':

    import pandas as pd
    from sqlalchemy import create_engine

    db1 = Database("name", "etl_interface", "mysql", "192.168.41.135", "3306", "root", encode("123456"))
    db2 = Database("name", "ydhl", "mysql", "192.168.41.135", "3306", "root", encode("123456"))
    connection1 = db1.getEngine()
    connection2 = db2.getEngine()

    paths = pd.read_sql(
        "SELECT j.from_param, j.match_path, j.map_field FROM py_data_parser j where trans_id = {} and state = 'Y' order by `order` ".format(
            "22"), con=connection1)

    json_str = '{"statusCode":"000000","desc":"请求成功","result":{"pm":{"aqi":"65","area":"南京","area_code":"nanjing","co":"1.1","ct":"2022-01-14 13:40:01.954","no2":"46","num":"145","o3":"35","o3_8h":"31","pm10":"73","pm2_5":"47","primary_pollutant":"颗粒物(PM2.5)","quality":"良好","so2":"7"},"ret_code":0,"siteList":[{"aqi":"50","co":"0.4","ct":"2021-12-27 09:20:37.525","no2":"37","o3":"13","o3_8h":"36","pm10":"50","pm2_5":"19","primary_pollutant":"-","quality":"优质","site_name":"六合雄州","so2":"2"},{"aqi":"50","co":"0.8","ct":"2021-12-27 09:20:37.528","no2":"39","o3":"25","o3_8h":"30","pm10":"50","pm2_5":"34","primary_pollutant":"-","quality":"优质","site_name":"瑞金路","so2":"3"},{"aqi":"46","co":"0.6","ct":"2021-12-27 09:20:37.531","no2":"55","o3":"24","o3_8h":"29","pm10":"46","pm2_5":"29","primary_pollutant":"-","quality":"优质","site_name":"山西路","so2":"4"},{"aqi":"45","co":"0.2","ct":"2021-12-27 09:20:37.533","no2":"35","o3":"33","o3_8h":"32","pm10":"45","pm2_5":"29","primary_pollutant":"-","quality":"优质","site_name":"玄武湖","so2":"4"},{"aqi":"44","co":"0.6","ct":"2021-12-27 09:20:37.535","no2":"58","o3":"8","o3_8h":"31","pm10":"44","pm2_5":"19","primary_pollutant":"-","quality":"优质","site_name":"仙林大学城","so2":"8"},{"aqi":"44","co":"0.6","ct":"2021-12-27 09:20:37.538","no2":"33","o3":"19","o3_8h":"31","pm10":"44","pm2_5":"23","primary_pollutant":"-","quality":"优质","site_name":"溧水永阳","so2":"6"},{"aqi":"42","co":"0.6","ct":"2021-12-27 09:20:37.540","no2":"26","o3":"41","o3_8h":"33","pm10":"40","pm2_5":"29","primary_pollutant":"-","quality":"优质","site_name":"江宁彩虹桥","so2":"6"},{"aqi":"42","co":"0.5","ct":"2021-12-27 09:20:37.542","no2":"35","o3":"33","o3_8h":"32","pm10":"39","pm2_5":"29","primary_pollutant":"-","quality":"优质","site_name":"草场门","so2":"6"},{"aqi":"41","co":"0.8","ct":"2021-12-27 09:20:37.545","no2":"29","o3":"40","o3_8h":"34","pm10":"41","pm2_5":"24","primary_pollutant":"-","quality":"优质","site_name":"中华门","so2":"3"},{"aqi":"39","co":"0.3","ct":"2021-12-27 09:20:37.547","no2":"16","o3":"37","o3_8h":"34","pm10":"39","pm2_5":"22","primary_pollutant":"-","quality":"优质","site_name":"浦口","so2":"5"},{"aqi":"38","co":"0.4","ct":"2021-12-27 09:20:37.549","no2":"39","o3":"31","o3_8h":"34","pm10":"38","pm2_5":"26","primary_pollutant":"-","quality":"优质","site_name":"高淳老职中","so2":"12"},{"aqi":"36","co":"0.2","ct":"2021-12-27 09:20:37.552","no2":"34","o3":"39","o3_8h":"37","pm10":"36","pm2_5":"22","primary_pollutant":"-","quality":"优质","site_name":"奥体中心","so2":"10"},{"aqi":"31","co":"1.1","ct":"2021-12-27 09:20:37.554","no2":"40","o3":"16","o3_8h":"35","pm10":"31","pm2_5":"19","primary_pollutant":"-","quality":"优质","site_name":"迈皋桥","so2":"4"}]}}'
    json_obj_o = json.loads(json_str)

    items = []
    item = {}
    for index, rst in paths.iterrows():
        items = get_json_val(json_obj_o, rst, item)

    for ind, val in enumerate(items):
        items[ind] = dict(item, **val)

    # import requests
    #
    # res = requests.post(
    #     url="https://api.apishop.net/common/air/getCityPM25Detail?apiKey=bs1lLN7a0fff2392939eca65e39bc381280937d4b47810b&city=南京市",
    #     data=json.loads('{"apiKey":"bs1lLN7a0fff2392939eca65e39bc381280937d4b47810b","city":"南京市"}'),
    #     headers=[])

    # df = pd.read_json(json.dumps(json.loads(res.text)["result"]["siteList"]))
    df = json_normalize(items)
    # df = pd.DataFrame(json.loads(res.text)["result"]["siteList"])

    df.to_sql('t_site_list', con=connection2, index=False, if_exists='append')
