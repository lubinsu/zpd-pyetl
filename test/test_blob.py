import petl as etl
import pymysql
import cx_Oracle
import datetime
import sys
import logging

import pymssql

if __name__ == '__main__':

    connection1 = pymysql.connect(password="zpd_ydhl@123", database="his_interface", user="ydhl", host="10.20.1.146",
                                  port=8100)
    o_cnn = cx_Oracle.Connection("{}/{}@{}:{}/{}".format("ydhl", "ydhl", "10.20.0.75", int(1521), "orcl"))
    o_cursor = o_cnn.cursor()
    # connection2 = CursorProxy(cursor)
    # table = etl.fromdb(connection1, "select ID, RUN_MSG, STEP_MSG from etl_run_log")

    mysqlCursor = connection1.cursor()
    o_cursor.execute("SELECT a.PK_PSNJOB, a.PHOTO FROM UAP631.T_PERSONNEL_INFO a WHERE photo IS NOT NULL AND rylb = '护理' AND rzlx = '主职'")

    insert_sql = "insert into his_interface.nc_emr_t_personnel_info (PK_PSNJOB, photo) values (%s,%s);"

    for row in o_cursor:
        # mysqlCursor.execute(insert_sql, (row["PK_PSNJOB"], row["PHOTO"]))
        pk_psnjob = row[0]
        if len(row[1]) > 0:
            image = row[1].read()
        else:
            image = None

        print(pk_psnjob)
        mysqlCursor.execute(insert_sql, (pk_psnjob, image))
        connection1.commit()

    o_cursor.close()
    o_cnn.close()
    mysqlCursor.close()
    connection1.close()
