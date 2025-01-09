import os
import pymysql


def export_stored_procedures(host, port, user, password, database, output_dir):
    """
    导出指定数据库中的所有存储过程到独立的 .sql 文件
    :param host: 数据库主机地址
    :param port: 数据库端口
    :param user: 数据库用户名
    :param password: 数据库密码
    :param database: 目标数据库名称
    :param output_dir: 导出的存储过程文件保存的目录
    """
    try:
        # 连接到数据库
        connection = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
        cursor = connection.cursor()

        # 查询所有存储过程的名字
        cursor.execute(
            "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = %s AND ROUTINE_TYPE = 'PROCEDURE';",
            (database,))
        procedures = cursor.fetchall()

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        print(f"找到 {len(procedures)} 个存储过程，开始导出...")
        for procedure in procedures:
            procedure_name = procedure[0]

            # 查询存储过程的定义
            cursor.execute(f"SHOW CREATE PROCEDURE `{procedure_name}`;")
            result = cursor.fetchone()
            if result:
                create_statement = result[2]  # SHOW CREATE PROCEDURE 的第三列包含存储过程定义

                # 保存到文件
                file_path = os.path.join(output_dir, f"{procedure_name}.sql")
                with open(file_path, 'w', encoding='utf-8') as file:
                    file.write(f"-- 创建存储过程: {procedure_name}\n")
                    file.write(create_statement)
                    file.write(";\n")

                print(f"存储过程 {procedure_name} 已导出到文件: {file_path}")

        print("所有存储过程导出完成！")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()


# 示例调用
if __name__ == "__main__":
    # 数据库连接配置
    host = "127.0.0.1"
    port = 54197
    user = "root"
    password = "Zpd_!qaz@wsx"
    database = "ydhl"  # 替换为你的数据库名称
    output_dir = "./procedures"  # 导出文件保存路径

    # 导出存储过程
    export_stored_procedures(host, port, user, password, database, output_dir)
