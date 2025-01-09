from lxml import etree
import pandas as pd
from Database import Database
from encryption.base64_encode import *
from lxml.etree import _Element
from collections import OrderedDict
import xmltodict


def parse_xml(row):
    xml_str = '''<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<Response>
	<IsSuccess>成功标识</IsSuccess>
	<Message>提示消息</Message>
	<Patients>
		<Patient>
			<Patient_id>患者ID</Patient_id>
			<Icno>就诊卡号</Icno>
			<Health_card_no>健康卡号</Health_card_no>
			<ID_no>身份证号</ID_no>
			<Patient_no>住院号</Patient_no>
			<Intimes>住院次数</Intimes>
			<Inpatient_no>住院流水号</Inpatient_no>
			<Outpatient_no>门诊号</Outpatient_no>
			<Health_checkup_no>体检号</Health_checkup_no>
			<Visit_type>就诊类型</Visit_type>
			<Sparms>
				<Sparm>
					<Request_no>申请单号</Request_no>
					<Sparm_pacsid>检查单号</Sparm_pacsid>
					<Sparm_Studytime>检查时间</Sparm_Studytime>
					<Sparm_ReportTime>报告日期</Sparm_ReportTime>
					<Sparm_ReportDoc>报告医生</Sparm_ReportDoc>
					<Sparm_AuditTime>审核日期</Sparm_AuditTime>
					<Sparm_AuditDoc>审核医生</Sparm_AuditDoc>
					<Sparm_ExamSee>检查所见</Sparm_ExamSee>
					<Sparm_Diagnose>检查结论</Sparm_Diagnose>
					<Sparm_positive>阴阳性</Sparm_positive>
					<Sparm_filepath>图文报告路径</Sparm_filepath>
					<Sparm_item_code>检查项目代码</Sparm_item_code>
					<Sparm_item>检查项目</Sparm_item>
					<Sparm_req_doc>申请医师</Sparm_req_doc>
					<Sparm_req_time>申请时间</Sparm_req_time>
					<Sparm_class>检查类别名称</Sparm_class>
					<Sparm_exe_doc>执行医师</Sparm_exe_doc>
				</Sparm>
			</Sparms>
		</Patient>
	</Patients>
</Response>'''

    # print(xml_str)
    root = etree.XML(bytes(xml_str, encoding='utf8'))
    # root = tree.getroot()
    # 遍历根元素下的所有标签
    IsSuccess = root.xpath("/Response/IsSuccess")[0].text

    # 返回成功
    if IsSuccess == '0':

        r = root.xpath("/Response/Patients/Patient")

        items = []

        for patient in r:
            col_o = {}
            for tag in patient:
                key = tag.tag
                if type(tag) == _Element and key != 'Items':
                    col_o[key] = None if tag.text == 'null' else tag.text

            r2 = root.xpath("/Response/Patients/Patient/Sparms/Sparm")
            for item in r2:
                col_i = {}
                for tag in item:
                    if type(tag) == _Element:
                        key = tag.tag
                        col_i[key] = None if tag.text == 'null' else tag.text
                row = dict(col_i,**col_o)
                items.append(row)

        db = Database("name", "ydhl", "mysql", "192.168.41.139", "3306", "root", encode("123456"))
        df = pd.DataFrame(items)
        db.getConnection().cursor().execute("truncate table {}".format("his_emr_pacs_jiancha_info"))
        df.to_sql(name="his_emr_pacs_jiancha_info", con=db.getEngine(),
                  index=False, if_exists='append')
        db.close()
    return row


if __name__ == '__main__':
    print(parse_xml("a"))
