import os
import pymysql
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from pandas import DataFrame
from datetime import datetime

c_info = {
    "host": os.getenv('USER_SERVICE_HOST'),
    "user": os.getenv('USER_SERVICE_USER'),
    "password": os.getenv("USER_SERVICE_PASSWORD"),
    "port": int(os.getenv("USER_SERVICE_PORT")),
    "cursorclass": pymysql.cursors.DictCursor,
}


def get_time():
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    print(" [DEVELOPMENT] date and time =", dt_string)
    return dt_string


def build_connection():
    # todo: exception handling
    mydb = mysql.connector.connect(
        host=c_info['host'],
        user=c_info['user'],
        password=c_info['password'],
        database="signals"
    )
    mycursor = mydb.cursor()
    print(" [DEVELOPMENT] connection established.")
    return mydb, mycursor


def get_user_name_by_user_id(user_id):
    sql = u'''SELECT * FROM signals.users where user_id = {}'''.format(user_id)
    (mydb, mycursor) = build_connection()
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    return myresult[0][1]


def get_all_signal_id():
    sql = u'''SELECT * FROM signals.signals'''
    (mydb, mycursor) = build_connection()
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    # print(myresult)
    signal_id = [];
    for row in myresult:
        signal_id.append(row[0])
    print(signal_id)
    return signal_id


def is_csv_needed(s3_filename):
    sql = u'''SELECT * FROM signals.signals where s3_filename = \'{}\''''.format(s3_filename)
    (mydb, mycursor) = build_connection()
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    mydb.close()
    print(s3_filename)
    print(myresult)
    if len(myresult) != 0:
        return True
    return False


def first_missing_positive(nums):
    n = len(nums)

    if 0 not in nums:
        return 0

    if 1 not in nums:
        return 1

    for i in range(n):
        if nums[i] <= 0 or nums[i] > n:
            nums[i] = 1

    for i in range(n):
        a = abs(nums[i])
        if a == n:
            nums[0] = - abs(nums[0])
        else:
            nums[a] = - abs(nums[a])

    for i in range(1, n):
        if nums[i] > 0:
            return i

    if nums[0] > 0:
        return n

    return n + 1


def is_signal_exist(user_id, signal_id):
    sql = "SELECT * FROM signals.signals where user_id = %s and signal_id = %s"
    val = (user_id, signal_id)
    (mydb, mycursor) = build_connection()
    mycursor.execute(sql, val)
    myresult = mycursor.fetchall()
    mydb.close()
    print(user_id)
    print(signal_id)
    print(len(myresult))
    if len(myresult) != 0:
        return True
    return False


def insert_signal(user_id, signal_id, signal_description, s3):
    sql = "INSERT INTO signals.signals (signal_id, signal_description, user_id, s3_filename, datetime) \
    VALUES (%s, %s, %s, %s, %s)"
    dt_string = get_time()
    val = (signal_id, signal_description, user_id, s3, dt_string)
    (mydb, mycursor) = build_connection()
    mycursor.execute(sql, val)
    mydb.commit()
    mydb.close()
    print(sql, val)
    print(mycursor.rowcount, "record inserted.")


def update_signal(user_id, signal_id, signal_description, s3):
    if s3 is None or s3 == "":
        print("s3 is not updated")
        sql = "UPDATE signals.signals SET signal_description = %s, datetime = %s where user_id = %s and signal_id =%s"
        val = (signal_description, get_time(), user_id, signal_id)
    else:
        print("s3 is updated")
        sql = "UPDATE signals.signals SET signal_description = %s, s3_filename = %s, datetime = %s where user_id = %s " \
              "and signal_id =%s "
        val = (signal_description, s3, get_time(), user_id, signal_id)

    (mydb, mycursor) = build_connection()
    mycursor.execute(sql, val)
    mydb.commit()
    mydb.close()
    print(sql, val)
    print(mycursor.rowcount, "record updated")


def read_signal(user_id, signal_id):
    sql = "SELECT * FROM signals.signals where user_id = %s and signal_id = %s"
    val = (user_id, signal_id)
    (mydb, mycursor) = build_connection()
    mycursor.execute(sql, val)
    myresult = mycursor.fetchall()
    mydb.close()
    return myresult[0][3]


def delete_signal(user_id, signal_id):
    sql = "DELETE FROM signals.signals WHERE user_id = %s and signal_id = %s"
    val = (user_id, signal_id)
    (mydb, mycursor) = build_connection()
    mycursor.execute(sql, val)
    mydb.commit()
    mydb.close()
    print(sql, val)
    print(mycursor.rowcount, "record deleted")
