#!/usr/bin/env python
# _*_ coding: utf-8 _*_
# @Time : 2022/1/14 13:58 
# @Author : lubinsu 
# @File : json_parser.py
# @desc : json解析工具
from collections import OrderedDict

import xmltodict
from lxml import etree
from lxml.etree import _Element


def get_xml_val(xml_obj_o, rst, item, items):
    items = items
    xml_obj = xml_obj_o
    match_path = rst["match_path"]
    map_field = rst["map_field"]

    r = xml_obj.xpath(match_path)

    for xml_item in r:
        if type(xml_item) == _Element:
            res_xml = etree.tostring(xml_item)
            xmlparse = xmltodict.parse(res_xml)
            if type(xmlparse) == OrderedDict:
                for key, value in xmlparse.items():
                    if type(value) == OrderedDict and len(value) > 1:
                        # print("{}:{}".format(key, value))
                        items.append(value)
                    else:
                        item[key] = value
        elif map_field is not None:
            item[map_field] = r[0]

    return items
