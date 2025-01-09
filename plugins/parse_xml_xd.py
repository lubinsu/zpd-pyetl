import json
from lxml import etree
import xmltodict
import pandas as pd
from Database import Database
# XML数据
def parse_xml_rows_xd(row):
    root = etree.XML(row['http_response_unescape'].encode("{}".format('UTF-8')))
    r = root.xpath('/YHYL/Result/Entry')
    items = []
    for xml_item in r:
        col ={}
        res_xml = etree.tostring(xml_item)
        xmlparse = xmltodict.parse(res_xml)
        for field in xmlparse['Entry']['Field']:
            key = field['@Name']
            value = None if field['@Value']=='null' else field['@Value']
            col[key] = value
        items.append(col)
    db = Database("name","his_interface","mysql",row['host_name'],"8100",row['user'],row['password'])
    df = pd.DataFrame(items)
    df.to_sql(name =row['target_table'],con=db.getEngine(),index=False,if_exists='append')
    db.close()
    return row

def parse_xml_page_xd(row):
    root = etree.XML(row['http_response_unescape'].encode("{}".format('UTF-8')))
    count = root.find('.//Result').get('Count')
    db = Database("name","his_interface","mysql",row['host_name'],"8100",row['user'],row['password'])
    db.getConnection().cursor().execute("truncate table {}".format(row['target_table']))
    db.getConnection().cursor().execute("update data_his_etl_page set count = {0},val1 = now() where table_name = '{1}'".format(count,row['target_table']))
    return row
