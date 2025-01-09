import re


def replace_placeholders(query, replacements):
    """
    替换 SQL 字符串中的占位符
    :param query: 包含占位符的 SQL 字符串
    :param replacements: 包含替换值的字典
    :return: 替换后的字符串
    """
    # 匹配 {key} 样式的占位符
    pattern = r'\{(\w+)\s*\}'  # 匹配花括号中的键名，并忽略多余空格

    def replacer(match):
        key = match.group(1).strip()  # 提取占位符中的键名
        if key in replacements:
            return str(replacements[key])  # 返回替换后的值
        return match.group(0)  # 如果未找到替换值，保持原样

    # 使用正则替换占位符
    return re.sub(pattern, replacer, query)


# 示例字符串
query = """
SELECT help_topic_id + 1 AS page_no, a.table_name AS target_table, 'his_interface' AS target_db, '192.168.5.46' AS target_host, 'root' AS target_user
    , 'Zpd_!qaz@wsx' AS target_pwd
    , concat('{"input":{"head":{"bizno":"', service_code, '","sysno":"TRTJ","tarno":"ZLHIS","time":"', now(), '","action_no":"ydhl_', DATE_FORMAT(NOW(6), '%Y%m%d%H%i%s.%f'), '"},"req_info":{"query_key":"0","query_content":"","page_no":"', help_topic_id + 1, '"}}}') AS body
    , '{"Content-Type" : "application/json"}' AS headers, 'utf-8' as encode, 'parse_json' as dy_function, 'zd_list' as list_node
FROM t_web_config a
    JOIN mysql.help_topic
    JOIN (
        SELECT @i := 1
    ) c
WHERE help_topic_id + 1 <= a.page_count 
    AND a.table_name = '{table_name}' 
ORDER BY help_topic_id
"""

# 示例替换值
replacements = {"table_name": "my_table"}

# 执行替换
result = replace_placeholders(query, replacements)
print(result)
