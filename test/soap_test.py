import urllib
import urllib.request
import urllib.parse
from suds.client import Client

def InvokeWebservice():
    texturl = 'http://192.168.0.225:80/soap/JHIPLIB.SOAP.BS.Service.cls?CfgItem=JH0435条码采集时间新增服务'
    postcontent = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
                  'xmlns:good="http://goodwillcis.com" xmlns:bjg="http://bjgoodwillcis.com">'
    postcontent += '<soapenv:Header>'
    postcontent += '<good:JiaheSecurity>'
    postcontent += '<good:UserName>?</good:UserName>'
    postcontent += '<good:Password>?</good:Password>'
    postcontent += '<good:Timestamp>?</good:Timestamp>'
    postcontent += '<good:FromSYS>?</good:FromSYS>'
    postcontent += '<good:IV>?</good:IV>'
    postcontent += '</good:JiaheSecurity>'
    postcontent += '</soapenv:Header>'
    postcontent += '<soapenv:Body>'
    postcontent += '<bjg:Send>'
    postcontent += '<bjg:pInput>{"barcode":"240506000255","labCode":"1001","operaterID":"1908007","operater":"马天蓉","statusdate":"2024/05/06 "17:00:48","sicktype":"住院"}</bjg:pInput>'
    postcontent += '</bjg:Send>'
    postcontent += '</soapenv:Body>'
    postcontent += '<soapenv:Envelope>'

    headers = {'Content-Type': 'text/xml'}

    # 未验证
    url = 'http://192.168.0.225:80/soap/JHIPLIB.SOAP.BS.Service.cls?CfgItem=JH0435条码采集时间新增服务'
    client = Client(url)
    token = client.factory.create('JiaheSecurity')
    token.UserName = 'UserName'
    token.Password = 'Password'
    token.Timestamp = 'Timestamp'
    token.FromSYS = 'FromSYS'
    token.IV = 'IV'
    client.set_options(soapheaders=token)
    print(client)
    print(client.service.Send('{"barcode":"240506000255","labCode":"1001","operaterID":"1908007","operater":"马天蓉","statusdate":"2024/05/06 "17:00:48","sicktype":"住院"}'))

    # req = urllib.request.Request(texturl, data=postcontent.encode('utf-8'), headers={'Content-Type': 'text/xml'})
    # urllib.request.urlopen(req)


if __name__ == '__main__':

    InvokeWebservice()
