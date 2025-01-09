import threading

import pymysql
from pymssql import OperationalError
import time

db_config = {
    'host': '192.168.41.139',  # MySQL 服务器地址
    'user': 'root',  # 数据库用户名
    'password': '123456',  # 数据库密码
    'database': 'ydhl'
}


def get_db_connection():
    """创建 MySQL 数据库连接"""
    return pymysql.connect(**db_config)


def etl_task(lock_name):
    """
    模拟 ETL 任务
    :param lock_name: 分布式锁的名称
    """
    conn = get_db_connection()
    lock = mysql_distributed_lock(conn, lock_name, timeout=10)

    print(f"Trying to acquire lock: {lock_name}")
    if lock.acquire():
        try:
            print("Lock acquired, processing ETL task...")
            # 模拟 ETL 任务逻辑
            time.sleep(2)
            print("ETL task completed.")
        finally:
            lock.release()
            print("Lock released.")
    else:
        print("Failed to acquire lock, task skipped.")

    conn.close()


class mysql_distributed_lock:
    def __init__(self, conn, lock_name, timeout=2):
        """
        初始化分布式锁
        :param conn: MySQL 数据库连接对象
        :param lock_name: 锁的名称
        :param timeout: 获取锁的超时时间（秒）
        """
        self.conn = conn
        self.lock_name = lock_name
        self.timeout = timeout

    def acquire(self):
        """
        尝试获取锁
        :return: True if lock acquired, False otherwise
        """
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(f"SELECT GET_LOCK(md5('{self.lock_name}'), {self.timeout})")
                result = cursor.fetchone()
                return result[0] == 1  # 返回 1 表示锁成功
            except OperationalError as e:
                print(f"Error acquiring lock: {e}")
                return False

    def release(self):
        """
        释放锁
        :return: True if lock released, False otherwise
        """
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(f"SELECT RELEASE_LOCK(md5('{self.lock_name}'))")
                result = cursor.fetchone()
                return result[0] == 1  # 返回 1 表示锁释放成功
            except OperationalError as e:
                print(f"Error releasing lock: {e}")
                return False

def timed_task():
    print("定时任务执行了！")
    # 重新设置定时器，使其再次执行
    lock_name = "etl_task_lock"
    timer = threading.Timer(5, timed_task, args=(lock_name,))
    timer.start()


from multiprocessing import Process

# if __name__ == "__main__":
#     lock_name = "etl_task_lock"
#
#     # 启动多个任务实例
#     processes = [Process(target=etl_task, args=(lock_name,)) for _ in range(3)]
#
#     for process in processes:
#         process.start()
#
#     for process in processes:
#         process.join()



# 设置定时器，5秒后执行timed_task函数，并设置为重复执行

if __name__ == '__main__':
    timer = threading.Timer(5, timed_task)

    # 启动定时器
    timer.start()

    print("定时器已启动，将每5秒执行一次定时任务。")