# @Time : 7/26/2024
# @Author : lubinsu
# @File : stream.py
# @desc : 拆分Job中的功能
import logging
import re
import time
from functools import wraps


def retry(retries_param, delay_param, backoff_param, patterns_param, sql_param):
    """
    通用重试装饰器，根据函数调用时传入的重试配置参数进行重试
    :param retries_param: 重试次数的参数名称
    :param delay_param: 初始延迟时间的参数名称
    :param backoff_param: 延迟时间的指数增长因子的参数名称
    :param patterns_param: 异常消息的正则表达式列表参数名称
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 获取重试配置参数
            retries = kwargs.get(retries_param)
            delay = kwargs.get(delay_param)
            backoff = kwargs.get(backoff_param)
            exception_patterns = kwargs.get(patterns_param)
            sql = kwargs.get(sql_param)

            if retries is None or delay is None or backoff is None or exception_patterns is None:
                raise ValueError("Missing retry configuration parameters")

            _retries = retries
            _delay = delay

            actual_retries = 0  # 记录实际重试次数

            while _retries > 0:
                try:
                    result = func(*args, **kwargs)
                    return result, actual_retries
                except Exception as e:
                    error_message = str(e)
                    if any(re.search(pattern, error_message) for pattern in exception_patterns):
                        _retries -= 1
                        actual_retries += 1
                        # print(f"Caught exception in {func.__name__}: {e}. Retrying in {_delay} seconds...")
                        logging.error("SQL执行失败{}，将在{}s内重试...".format(sql, _delay))
                        time.sleep(_delay)
                        _delay *= backoff
                    else:
                        raise
            try:
                result = func(*args, **kwargs)
                return result, actual_retries
            except Exception as e:
                logging.error("SQL执行失败：{}, SQL:{}".format(e, sql))
                return e, actual_retries  # 最后一次尝试失败时返回None和实际重试次数
        return wrapper
    return decorator


# 执行SQL
@retry(retries_param='retries', delay_param='delay', backoff_param='backoff', patterns_param='patterns', sql_param='sql')
def exec_sql(sql, row, target, databases, retries=None, delay=None, backoff=None, patterns=None):

    if databases[target.conName].type_ == "oracle":
        row_count = target.get_conn().cursor().execute(sql)
    else:
        row_count = target.get_conn().cursor().execute(sql)
    target.get_conn().commit()
    return '执行成功:更新条数为：{}'.format(str(row_count))

"""
# 示例使用
@retry(retries_param='retries', delay_param='delay', backoff_param='backoff', patterns_param='patterns')
def flaky_function(sql, retries=None, delay=None, backoff=None, patterns=None):
    print("执行")
    print(sql)
    import random
    errors = ["temporary error occurred", "network issue detected", "permanent failure", "all good"]
    result = random.choice(errors)
    print(result)
    if result != "all good":
        raise Exception(result)
    return "Success!"


# 测试

if __name__ == "__main__":
    try:
        result, retry_count = flaky_function('1111',
            retries=1,
            delay=2,
            backoff=1,
            patterns=['temporary error', 'network issue']
        )
        if result is None:
            print(f"重试失败, Retry Count: {retry_count}")
        else:
            print(f"Result: {result}, Retry Count: {retry_count}")
    except Exception as e:
        print(e)
"""