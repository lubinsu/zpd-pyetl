from lxml import etree
import pandas as pd
from Database import Database
from encryption.base64_encode import *
from lxml.etree import _Element
from collections import OrderedDict
import xmltodict


def parse_xml(row):
    xml_str = row['v_result_unescape']\
        .replace('xmlns:xsd="http://www.w3.org/2001/XMLSchema"', '')\
        .replace('<?xml version="1.0" encoding="UTF-8"?>', '')\
        .replace('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"', '')

    # print(xml_str)
    root = etree.XML(bytes(xml_str, encoding='utf8'))
    # root = tree.getroot()
    # {
    #     'col1': 'value1',
    #     'col2': 'value2'
    # }

    # 返回成功
    IsSuccess = root.xpath("/Response/IsSuccess")[0].text
    if IsSuccess == '1':

        # 遍历根元素下的所有标签
        r = root.xpath("/Response/Patients/Patient")

        items = []

        for patient in r:
            col_o = {}
            jiancha_items = None
            # 获取基本信息
            for tag in patient:
                key = tag.tag
                if type(tag) == _Element and key != 'Sparms':
                    col_o[key] = None if tag.text == 'null' else tag.text
                elif key == 'Sparms':
                    # 如果是检查项，则保存，放后面循环获取
                    jiancha_items = tag

            # 获取该患者的各个检查项
            for t in jiancha_items:
                res_xml = etree.tostring(t)
                xmlparse = xmltodict.parse(res_xml)
                col_i = {}
                if type(xmlparse) == OrderedDict:
                    for key, value in xmlparse.items():
                        if type(value) == OrderedDict and len(value) > 1:
                            # print("{}:{}".format(key, value))
                            col_i = value
                        else:
                            col_i[key] = value
                    single_row = dict(col_i, **col_o)
                    items.append(single_row)

        db = Database("name", row['target_db'], "mysql", row['target_host'], "8100", row['target_user']
                      , row['target_pwd'])
        df = pd.DataFrame(items)
        # db.getConnection().cursor().execute("truncate table {}".format("his_emr_fuzhujiancha"))
        df.to_sql(name=row['target_table'], con=db.getEngine(),
                  index=False, if_exists='append')
        db.close()
        row['items'] = items
    return row


if __name__ == '__main__':
    row = {'v_result': '''<Response xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	<IsSuccess>1</IsSuccess>
	<Message/>
	<Patients>
		<Patient>
			<Patient_id>3865512</Patient_id>
			<Icno>00551687</Icno>
			<Health_card_no/>
			<ID_no>441821199009181549</ID_no>
			<Patient_no>152024</Patient_no>
			<Intimes>2</Intimes>
			<Inpatient_no>202408787</Inpatient_no>
			<Outpatient_no/>
			<Health_checkup_no/>
			<Visit_type>2</Visit_type>
			<Request_no>ZY00535932</Request_no>
			<Sparms>
				<Sparm>
					<Request_no>ZY00535932</Request_no>
					<Sparm_pacsid>652619</Sparm_pacsid>
					<Sparm_Studytime>2024-04-22 14:42:32</Sparm_Studytime>
					<Sparm_ReportTime>2024-04-22 15:08:50</Sparm_ReportTime>
					<Sparm_ReportDoc>朱志铿</Sparm_ReportDoc>
					<Sparm_AuditTime>2024-04-22 15:08:52</Sparm_AuditTime>
					<Sparm_AuditDoc>朱志铿</Sparm_AuditDoc>
					<Sparm_ExamSee>1.头颅CT平扫未见明显异常，请结合临床必要时MRI检查。
2.胸部CT平扫未见明显异常。
3.考虑脂肪肝，请结合唱临床。
4.肝右叶钙化灶与胆管结石相鉴别，请结合临床。</Sparm_ExamSee>
					<Sparm_Diagnose>脑组织未见明显异常密度影，脑室、脑池系统及脑沟形态、大小和位置未见明显异常，中线结构未见偏移。
   双肺纹理清晰，走行自然，肺野透过度良好，双肺未见异常密度影，双肺门不大。气管及双侧主支气管通畅。纵隔无偏移，心脏及大血管显示形态正常，纵隔内未见肿块及肿大淋巴结。未见胸腔积液及胸膜肥厚。
   肝实质密度均匀降低，CT值约30Hu，肝右叶钙化灶，肝内、外胆管无扩张，肝门结构清，未见异常密度影。胆囊不大，未见阳性结石。脾脏形态、实质密度均匀，未见异常密度灶。胰腺形态、密度未见异常。腹膜腔未见积液。双肾未见明显异常。</Sparm_Diagnose>
					<Sparm_positive>阳性</Sparm_positive>
					<Sparm_filepath>http://192.168.174.203:9000/DocumentService/DownLoad/BusinessDocumentPdf?businessIDs=4a76539f-2861-4ff4-accf-6442bfbd60ed&amp;amp;token=</Sparm_filepath>
					<Sparm_item_code>A04010189</Sparm_item_code>
					<Sparm_item>X线计算机体层(CT)扫描加收(使用螺旋扫描),上腹CT平扫,头部CT平扫,胸部CT平扫</Sparm_item>
					<Sparm_req_doc>邝文超</Sparm_req_doc>
					<Sparm_req_time>2024-04-22 12:15:53</Sparm_req_time>
					<Sparm_class>CT</Sparm_class>
					<Sparm_exe_doc/>
				</Sparm>
			</Sparms>
		</Patient>
	</Patients>
</Response>
'''
        , 'target_host': '192.168.41.139'
        , 'target_db': 'ydhl', 'target_user': 'root'
        , 'target_pwd': encode("123456")
        , 'target_table': 'his_emr_pacs_jiancha_info'}

    print(parse_xml(row)['items'])
