import json


def print_jsong(v_str):
    print(v_str)

if __name__ == '__main__':
    j_str = '{"CallESB": "<ESBEnvelope xmlns=\\"http://ESB.TopSchemaV2\\"><ESBHeader><HeaderControl AppCode=\\"ydyyhxt\\" Password=\\"34d345ed0b190804\\" MessageCategory=\\"YLJ_LIS_001\\" Version=\\"1\\" CreateTime=\\"2023/4/13 18:01:12\\" /></ESBHeader><ESBBody><BodyControl CallType=\\"\\"/><BusinessRequest><![CDATA[<Request><Patient_id>3556952</Patient_id><Icno></Icno><Health_card_no></Health_card_no><ID_no></ID_no><Patient_no>164632</Patient_no><Intimes>6</Intimes><Inpatient_no></Inpatient_no><Outpatient_no></Outpatient_no><Health_checkup_no></Health_checkup_no><Visit_type>2</Visit_type><Request_no></Request_no><Start_time>2024-04-19 00:23:45</Start_time><End_time>2024-04-19 14:49:49</End_time><Input_parameter_type>2</Input_parameter_type></Request>]]></BusinessRequest></ESBBody></ESBEnvelope>"}'
    print(j_str)
    a = json.loads(j_str)
    print(a)
