# CREATE TABLE replication_monitor (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     host VARCHAR(255),
#     port INT,
#     delay_seconds INT
# );
import pymysql
import time


def check_replication(host, port, user, password, interval=10, max_delay=500):
    while True:
        try:
            # 连接MySQL数据库
            connection = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )

            # 查询从库状态
            with connection.cursor() as cursor:
                cursor.execute("SHOW SLAVE STATUS")
                result = cursor.fetchone()
                if result is not None and result['Slave_IO_Running'] == 'Yes' and result['Slave_SQL_Running'] == 'Yes':
                    print("Replication is running smoothly.")
                    if result['Seconds_Behind_Master'] is not None and result['Seconds_Behind_Master'] > max_delay:
                        print("Replication delay is too high: {} seconds.".format(result['Seconds_Behind_Master']))
                        # 记录异常情况到表中
                        cursor.execute(
                            "INSERT INTO replication_monitor (host, port, delay_seconds) VALUES (%s, %s, %s)",
                            (host, port, result['Seconds_Behind_Master']))
                        connection.commit()
                else:
                    print("Replication is not running.")
                    print(
                        "IO Thread: {}, SQL Thread: {}".format(result['Slave_IO_Running'], result['Slave_SQL_Running']))

        except pymysql.Error as e:
            print("Error:", e)

        finally:
            if 'connection' in locals() and connection.open:
                connection.close()

        time.sleep(interval)


if __name__ == "__main__":
    # MySQL从库的连接信息
    host = "your_slave_host"
    port = 3306  # MySQL默认端口
    user = "your_mysql_user"
    password = "your_mysql_password"

    check_replication(host, port, user, password)
