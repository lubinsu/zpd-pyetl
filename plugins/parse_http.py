import re

# 检查文本类型是否为字符串
# @param text 文本
# @return True：是，False：不是
def check_string(text):
    return True if isinstance(text, str) else False


# 检查文本类型是否为浮点型
# @param text 文本
# @return True：是，False：不是
def check_float(text):
    if check_string(text):
        try:
            return True if float("{0}".format(text)) else False
        except Exception:
            return False
    else:
        return True if isinstance(text, float) else False


# 检查文本类型是否为浮点型
# @param text 文本
# @return True：是，False：不是
def check_int(text):
    if check_string(text):
        try:
            return True if float("{0}".format(text)) else False
        except Exception:
            return False
    else:
        return True if isinstance(text, float) else False


# 检查文本类型是否为数字型
# @param text 文本
# @return True：是，False：不是
def check_is_number(text):
    return True if re.search("[^0-9]", text) == None else False


