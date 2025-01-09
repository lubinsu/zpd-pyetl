import pymysql
import logging
import os
import xml.etree.ElementTree as ET
import base64
from Database import Database  # 导入 Database 类

# -----------------------------
# 公共配置和日志记录
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MySQL_Recovery")


def read_db_config(config_path):
    """ 读取数据库配置文件并返回数据库连接参数 """
    try:
        tree = ET.parse(config_path)
        root = tree.getroot()
        connection = root.find("connection")
        host = connection.find("host").text
        port = int(connection.find("port").text)
        user = connection.find("user").text
        password = base64.b64decode(connection.find("password").text).decode("utf-8")
        database = connection.find("db").text
        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database
        }
    except Exception as e:
        logger.error(f"无法读取数据库配置文件: {e}")
        raise


class BaseMySQLUtility:
    """公共基础工具类，用于管理数据库连接和日志表"""

    def __init__(self, db_instance):
        self.db_instance = db_instance
        self.connection = self.db_instance.getConnection()
        self.log_table = "mysql_repair_log"
        self._connect_to_db()

    def _connect_to_db(self):
        try:
            if self.connection:
                logger.info("成功使用 Database 对象连接到数据库")
                self._create_log_table()
            else:
                raise Exception("Database 连接未初始化")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise

    def _create_log_table(self):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {self.log_table} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        table_name VARCHAR(255),
                        action VARCHAR(50),
                        status VARCHAR(50),
                        message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                ''')
                self.connection.commit()
                logger.info("日志表检查/创建成功")
        except Exception as e:
            logger.error(f"日志表创建失败: {e}")
            raise

    def log_to_db(self, table_name, action, status, message):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f'''
                    INSERT INTO {self.log_table} (table_name, action, status, message)
                    VALUES (%s, %s, %s, %s);
                ''', (table_name, action, status, message))
                self.connection.commit()
                logger.info(f"日志记录成功: {action} -> {table_name}")
        except Exception as e:
            logger.error(f"日志记录失败: {e}")


class MySQLTableChecker(BaseMySQLUtility):
    """检查MySQL表是否损坏"""

    def check_and_log_damaged_tables(self):
        damaged_tables = []
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW TABLES;")
                tables = cursor.fetchall()
                logger.info("开始检查所有表的完整性...")
                for table_tuple in tables:
                    table_name = table_tuple[0]
                    try:
                        cursor.execute(f"CHECK TABLE `{table_name}`;")
                        results = cursor.fetchall()
                        for row in results:
                            table, op, msg_type, msg_text = row
                            if msg_type.lower() == 'error' or 'corrupt' in msg_text.lower():
                                damaged_tables.append(table_name)
                                self.log_to_db(table_name, "CHECK_TABLE", "DAMAGED", msg_text)
                                logger.warning(f"表损坏: {table_name} -> {msg_text}")
                    except Exception as e:
                        logger.error(f"无法检查表 '{table_name}': {e}")
        except Exception as e:
            logger.error(f"检查表失败: {e}")
        return damaged_tables


class MySQLTableRepairer(BaseMySQLUtility):
    """修复损坏的MySQL表"""

    def repair_damaged_tables(self, damaged_tables):
        for table in damaged_tables:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"REPAIR TABLE `{table}`;")
                    results = cursor.fetchall()
                    for row in results:
                        table_name, op, msg_type, msg_text = row
                        status = "SUCCESS" if msg_type.lower() == "status" else "FAILED"
                        self.log_to_db(table_name, "REPAIR_TABLE", status, msg_text)
                        logger.info(f"修复表: {table_name} -> {status}: {msg_text}")
            except Exception as e:
                logger.error(f"修复表 '{table}' 失败: {e}")
                self.log_to_db(table, "REPAIR_TABLE", "FAILED", str(e))


class MySQLIbdFileRecoverer(BaseMySQLUtility):
    """恢复InnoDB的ibd文件"""

    def recover_ibd_files(self, ibd_files_dir, destination_db):
        try:
            for file in os.listdir(ibd_files_dir):
                if file.endswith(".ibd"):
                    table_name = os.path.splitext(file)[0]
                    ibd_file_path = os.path.join(ibd_files_dir, file)
                    logger.info(f"开始恢复表: {table_name}")
                    try:
                        with self.connection.cursor() as cursor:
                            cursor.execute(
                                f"CREATE TABLE `{destination_db}`.`{table_name}` LIKE `{self.db_instance.db}`.`{table_name}`;")
                            cursor.execute(f"ALTER TABLE `{destination_db}`.`{table_name}` DISCARD TABLESPACE;")
                        os.system(f"cp {ibd_file_path} /var/lib/mysql/{destination_db}/{table_name}.ibd")
                        with self.connection.cursor() as cursor:
                            cursor.execute(f"ALTER TABLE `{destination_db}`.`{table_name}` IMPORT TABLESPACE;")
                        self.log_to_db(table_name, "RECOVER_IBD", "SUCCESS", "ibd文件恢复成功")
                        logger.info(f"ibd文件恢复成功: {table_name}")
                    except Exception as e:
                        logger.error(f"ibd文件恢复失败: {table_name} -> {e}")
                        self.log_to_db(table_name, "RECOVER_IBD", "FAILED", str(e))
        except Exception as e:
            logger.error(f"恢复ibd文件时发生错误: {e}")


if __name__ == "__main__":
    # 读取数据库配置文件
    config_path = "resources/db.xml"
    db_config = read_db_config(config_path)

    # 使用 Database 类初始化连接
    db_instance = Database(
        name="MyDatabase",
        db=db_config["database"],
        type_="mysql",
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"]
    )

    # 1. 检查并记录损坏的表
    checker = MySQLTableChecker(db_instance)
    damaged_tables = checker.check_and_log_damaged_tables()
    print("损坏的表: ", damaged_tables)

    # 2. 修复损坏的表
    if damaged_tables:
        repairer = MySQLTableRepairer(db_instance)
        repairer.repair_damaged_tables(damaged_tables)

    # 3. 恢复InnoDB的ibd文件
    ibd_files_dir = "/path/to/ibd_files"
    destination_db = "recovered_db"
    recoverer = MySQLIbdFileRecoverer(db_instance)
    recoverer.recover_ibd_files(ibd_files_dir, destination_db)
