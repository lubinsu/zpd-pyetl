# @Time : 7/07/2021 09:14 AM
# @Author : lubinsu
# @File : base64_encode.py
# @desc : 数据库密码，编码解码程序
import base64

# 编码
def encode(str):
    bs = base64.b64encode(str.encode("utf8"))
    bs2 = base64.b64encode(bs)
    return bs2.decode("utf8")


# 解码
def decode(str):
    decode1 = base64.b64decode(str.encode("utf8"))
    decode2 = base64.b64decode(decode1)
    return decode2.decode("utf8")


if __name__ == '__main__':
    var = 1
    while var == 1:
        int = input("选择操作编号 1:编码、2:解码、3:退出 :\n")
        if int == '1':
            str_encrypt = input("编码要加密的字符串:\n")
            print("编码后字符串:\n{}".format(encode(str_encrypt)))
        elif int == '2':
            str_encrypt = input("输入要解码的字符串:\n")
            print("解码后字符串:\n{}".format(decode(str_encrypt)))
        else:
            exit(0)
