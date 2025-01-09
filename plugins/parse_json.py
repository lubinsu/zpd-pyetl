# -*- coding: utf-8 -*-
import json
import logging
import traceback

import pandas as pd
from Database import Database
from encryption.base64_encode import *
import datetime
from pymysql import ProgrammingError


def parse_json(row, databases=None):

    conn = None
    try:
        # json_str = row['v_result_unescape']

        # 返回成功
        # print(row['v_result_unescape'])
        data = json.loads(row['v_result_unescape'])
        exe_status = data['input']['ack_info']['exe_status']
        err_msg = data['input']['ack_info']['err_msg']
        page_count = 1

        try:
            page_count = data['input']['page_count']
        except KeyError as e:
            logging.debug("字段不存在")

        page_no = 0

        # print('exe_status:{},dept_code:{}'.format(exe_status,row['item_id']))
        if exe_status == 'A':

            # 遍历根元素下的所有标签
            # zd_list = data['input'][row['list_node']]
            # 遍历根元素下的所有标签
            try:
                zd_list = data['input'][row['list_node']]
            except Exception as e:
                logging.error("获取节点失败：{},{}".format(row['list_node'],traceback.format_exc()))
                print(row['v_result_unescape'])
                return row

            items = []
            # print(zd_list)
            if(type(zd_list) == dict):
                row_dict = {}
                if 'pv1_info' in data['input']:
                    row_dict['inpno'] = data['input']['pv1_info']['inpno']
                for key, value in zd_list.items():
                    if(type(value) == list):
                        continue
                    else:
                        row_dict[key] = value
                items.append(row_dict)
            else:
                for json_row in zd_list:
                    row_dict = {}
                    if ('page_no' in json_row):
                        if page_no < int(json_row['page_no']):
                            page_no = int(json_row['page_no'])

                    for key, value in json_row.items():
                        # print(key, value)
                        if(type(value) == list):
                            continue
                        else:
                            row_dict[key] = value
                    if row['target_table'] in ['his_emr_file_formats']:
                        row_dict['zhuyuan_id'] = row['zhuyuan_id']
                    if row['target_table'] in ['his_emr_binglijilu_content']:
                        row_dict['emr_id'] = row['emr_id']
                    items.append(row_dict)

            db = databases[row['target_db']].getConnection()
            conn = databases[row['target_db']].getEngine().connect()
            df = pd.DataFrame(items)

            # 首页先清空表
            if int(page_no) == 1 and row['target_table'] != 'his_emr_yizhu_info_new':
                try:
                    logging.info(f"清空表操作: 目标表={row['target_table']}")
                    with db.cursor() as cursor:
                        cursor.execute("truncate table {}".format(row['target_table']))
                    logging.info(f"表 {row['target_table']} 已被清空")
                except ProgrammingError as e:
                    if e.args[0] == 1146:  # 1146 是 MySQL 中 "table doesn't exist" 的错误代码
                        print("表不存在")
                    else:
                        raise e

            df.to_sql(name=row['target_table'], con=conn, index=False, if_exists='append')
            if row['target_table'] != 'his_emr_yizhu_info_new':
                with db.cursor() as cursor:
                    cursor.execute("UPDATE ydhl.t_web_config a SET a.page_count = if({} > a.page_count, {}, a.page_count) "
                                                    "WHERE a.table_name = '{}'".format(page_count, page_count, row['target_table']))
            else:
                with db.cursor() as cursor:
                    cursor.execute("UPDATE ydhl.t_web_config a SET a.page_count = {} "
                                                    "WHERE a.table_name = '{}' AND dept_code = '{}'".format(page_count, page_count, row['target_table'], row['item_id']))
                #print('syn dept_code:{}'.format(row['item_id']))

            row['items'] = items
    except Exception as e:
        logging.error(f"解析数据失败: {traceback.format_exc()}")
        raise e
    finally:
        if conn is not None:
            conn.close()
            databases[row['target_db']].getEngine().dispose()
        return row


def parse_json_fee(row, databases=None):
    conn = None
    try:
        # 返回成功
        #print(row['v_result_unescape'])
        #print(row['row_no'])
        data = json.loads(row['v_result_unescape'])
        exe_status = data['input']['ack_info']['exe_status']
        err_msg = data['input']['ack_info']['err_msg']
        page_count = 1

        try:
            page_count = data['input']['page_count']
        except KeyError as e:
            logging.debug("字段不存在")

        page_no = 0

        if exe_status == 'A':

            # 遍历根元素下的所有标签
            try:
                zd_list = data['input'][row['list_node']]
            except Exception as e:
                #print(row['v_result_unescape'])
                print(e)
                return row

            items = []
            # print(zd_list)
            for json_row in zd_list:
                #row_dict = {}
                if ('page_no' in json_row):
                    if page_no < int(json_row['page_no']):
                        page_no = int(json_row['page_no'])

                for key, value in json_row.items():
                    row_dict = {}
                    if 'pv1_info' in data['input']:
                        inpno = data['input']['pv1_info']['inpno']
                    # print(key, value)
                    if key == 'date':
                        v_date = value
                    if(type(value) == list):
                        for value_item in value:
                            row_dict = {}
                            row_dict['inpno'] = inpno
                            row_dict['date'] = v_date
                            for key, value in value_item.items():
                                row_dict[key] = value
                            items.append(row_dict)

            db = databases[row['target_db']].getConnection()
            conn = databases[row['target_db']].getEngine().connect()
            df = pd.DataFrame(items)

            try:
                df.to_sql(name=row['target_table'], con=conn, index=False, if_exists='append')
                logging.info(f"成功插入数据: 表名={row['target_table']}, 条数={len(df)}")

            except Exception as e:
                logging.error(f"插入数据失败: 表名={row['target_table']}, 错误={traceback.format_exc()}")
                raise e
            with db.cursor() as cursor:
                cursor.execute("UPDATE ydhl.t_web_config a SET a.page_count = if({} > a.page_count, {}, a.page_count) "
                                                "WHERE a.table_name = '{}'".format(page_count, page_count, row['target_table']))
            row['items'] = items
    except Exception as e:
        logging.error(f"解析数据失败: {traceback.format_exc()}")
        raise e
    finally:
        if conn is not None:
            conn.close()
            databases[row['target_db']].getEngine().dispose()
        return row


if __name__ == '__main__':
    row = {'v_result_unescape': '{"input":{"ack_info":{"exe_status":"A","err_msg":""},"pv1_info":{"pid":"999984","inpno":"230023203","idno":"610112201212300511","pat_name":"袁士腾","pat_sex":"男","pvid":"1","dept_id":"1023","dept_name":"脑病十科","inp_ward_id":"1023","inp_ward_name":"脑病十科","inp_bed_no":"9","inp_doctor":"李彩云","inp_pnurs":"王蕊","adta_time":"2024-10-18 16:54:00","out_time":null,"dsctplan_name":"普通","pat_type":"城镇居民基本医疗保险","si_type_id":"888","si_type":"医保接口","pat_inp_days":0,"charge_sign":"0","nobalance_money":6301.47,"prepay_money":5000.0,"remain_money":-1301.47,"arrear_flag":"1","visit_status":"0"},"day_list":[{"date":"2024-10-18","fmlist":[{"fees_item":"化验费","should_money":1103.0,"actual_money":1103.0},{"fees_item":"治疗费","should_money":4.0,"actual_money":4.0},{"fees_item":"诊查费","should_money":20.0,"actual_money":20.0},{"fees_item":"检查费","should_money":1904.5,"actual_money":1904.5},{"fees_item":"西药费","should_money":140.47,"actual_money":140.47},{"fees_item":"护理费","should_money":56.0,"actual_money":56.0},{"fees_item":"特殊材料费","should_money":65.8,"actual_money":65.8},{"fees_item":"床位费","should_money":80.0,"actual_money":80.0}],"money":3373.77},{"date":"2024-10-19","fmlist":[{"fees_item":"治疗费","should_money":359.0,"actual_money":359.0},{"fees_item":"诊查费","should_money":20.0,"actual_money":20.0},{"fees_item":"西药费","should_money":137.9,"actual_money":137.9},{"fees_item":"护理费","should_money":38.0,"actual_money":38.0},{"fees_item":"中成药","should_money":204.0,"actual_money":204.0},{"fees_item":"特殊材料费","should_money":19.6,"actual_money":19.6},{"fees_item":"床位费","should_money":80.0,"actual_money":80.0}],"money":858.5},{"date":"2024-10-20","fmlist":[{"fees_item":"诊查费","should_money":20.0,"actual_money":20.0},{"fees_item":"治疗费","should_money":172.0,"actual_money":172.0},{"fees_item":"西药费","should_money":137.9,"actual_money":137.9},{"fees_item":"其他","should_money":7.0,"actual_money":7.0},{"fees_item":"护理费","should_money":38.0,"actual_money":38.0},{"fees_item":"特殊材料费","should_money":9.8,"actual_money":9.8},{"fees_item":"床位费","should_money":80.0,"actual_money":80.0},{"fees_item":"中草药费","should_money":43.3,"actual_money":43.3}],"money":508},{"date":"2024-10-21","fmlist":[{"fees_item":"诊查费","should_money":20.0,"actual_money":20.0},{"fees_item":"治疗费","should_money":17.0,"actual_money":17.0},{"fees_item":"西药费","should_money":137.9,"actual_money":137.9},{"fees_item":"护理费","should_money":38.0,"actual_money":38.0},{"fees_item":"床位费","should_money":60.0,"actual_money":60.0}],"money":272.9},{"date":"2024-10-22","fmlist":[{"fees_item":"治疗费","should_money":172.0,"actual_money":172.0},{"fees_item":"诊查费","should_money":20.0,"actual_money":20.0},{"fees_item":"西药费","should_money":137.9,"actual_money":137.9},{"fees_item":"护理费","should_money":38.0,"actual_money":38.0},{"fees_item":"特殊材料费","should_money":11.0,"actual_money":11.0},{"fees_item":"床位费","should_money":60.0,"actual_money":60.0}],"money":438.9},{"date":"2024-10-23","fmlist":[{"fees_item":"治疗费","should_money":172.0,"actual_money":172.0},{"fees_item":"诊查费","should_money":20.0,"actual_money":20.0},{"fees_item":"西药费","should_money":203.9,"actual_money":203.9},{"fees_item":"护理费","should_money":38.0,"actual_money":38.0},{"fees_item":"特殊材料费","should_money":20.8,"actual_money":20.8},{"fees_item":"床位费","should_money":60.0,"actual_money":60.0}],"money":514.7},{"date":"2024-10-24","fmlist":[{"fees_item":"治疗费","should_money":172.0,"actual_money":172.0},{"fees_item":"西药费","should_money":137.9,"actual_money":137.9},{"fees_item":"护理费","should_money":15.0,"actual_money":15.0},{"fees_item":"特殊材料费","should_money":9.8,"actual_money":9.8}],"money":334.7}],"head":{"bizno":"S3015","sysno":"TRTJ","tarno":"ZLHIS","time":"2024-10-24 16:26:35","action_no":"ydhl_20241024162322.802692"}}}'
        , 'target_host': '192.168.41.139'
        , 'target_db': 'his_interface'
        , 'target_user': 'root'
        , 'list_node': 'day_list'
        , 'target_pwd': "123456"
        , 'target_table': 'his_emr_feiyong_zongji'}

    #print(parse_json_fee(row))
