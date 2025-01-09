import json
from lxml import etree
import pandas as pd
from Database import Database
from encryption.base64_encode import *


def parseXml(row):
    xml_str = '<message>' \
              '	<result type="True"/>' \
              '	<data>' \
              '		<row>' \
              '			<col hosp="SN">0301</col>' \
              '			<col hosp="HOSP_CODE">G8641769-7</col>' \
              '			<col hosp="DEPT_CODE">0301</col>' \
              '			<col hosp="DEPT_NAME">普外科</col>' \
              '			<col hosp="HOS_SN">15894466995260010753534989</col>' \
              '			<col hosp="PY_CODE">PWKBO</col>' \
              '			<col hosp="WB_CODE">UQTUA</col>' \
              '			<col hosp="DEPT_TYPE">1</col>' \
              '			<col hosp="FLAG">0</col>' \
              '			<col hosp="DEPT_ABBR">普外科</col>' \
              '			<col hosp="PARENT_CODE">15896272233940011012865698</col>' \
              '			<col hosp="OUT_DEPT_CODE">0301</col>' \
              '			<col hosp="CREATE_DATE">2020-10-21 11:33:53.0</col>' \
              '			<col hosp="CREATE_USER_SN">system</col>' \
              '			<col hosp="MODIFY_DATE">2020-10-21 11:33:53.0</col>' \
              '			<col hosp="MODIFY_USER_SN">system</col>' \
              '			<col hosp="DEPT_PROFILE">null</col>' \
              '			<col hosp="REG_DEPT_FLAG">1</col>' \
              '			<col hosp="CLASSIFY">0</col>' \
              '			<col hosp="MEDICAL_CATEGORY">null</col>' \
              '			<col hosp="DEPT_LOC">null</col>' \
              '			<col hosp="ICU_FLAG">0</col>' \
              '			<col hosp="OPERATION_FLAG">0</col>' \
              '			<col hosp="PEDIATRICS_FLAG">0</col>' \
              '			<col hosp="EMERGENCY_FLAG">0</col>' \
              '			<col hosp="SUBJECT_TYPE">null</col>' \
              '			<col hosp="HOSPITALAREA">null</col>' \
              '			<col hosp="ORDER_NUMBER">null</col>' \
              '			<col hosp="NS_CODE">null</col>' \
              '			<col hosp="ONLINE_FLAG">0</col>' \
              '			<col hosp="REAL_DEPT_CODE">null</col>' \
              '		</row>' \
              '	</data>' \
              '</message>'
    # root = ET.fromstring(xml_str)
    root = etree.XML(xml_str)
    # root = tree.getroot()
    # 遍历根元素下的所有标签
    r = root.xpath("/message/data/row")

    items = []

    for item in r:
        col = {}
        for tag in item:
            att = tag.attrib
            for a in tag.attrib:
                if a == 'hosp':
                    key = att['hosp']
            col[key] = None if tag.text == 'null' else tag.text
        items.append(col)

    db = Database("name", "ydhl", "mysql", "192.168.41.139", "3306", "root", encode("123456"))
    df = pd.DataFrame(items)
    db.getConnection().cursor().execute("truncate table {}".format("tmp_table"))
    df.to_sql(name="tmp_table", con=db.getEngine(),
              index=False, if_exists='append')
    db.close()
    # return row


if __name__ == '__main__':
    print(parseXml("a"))
