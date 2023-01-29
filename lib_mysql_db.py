#!/usr/bin/env python3
import os
import mysql.connector
from dotenv import load_dotenv
from lib_logger import logger
from lib_helper import days_ago

load_dotenv()

def get_mysql():
    mysql_conn = mysql.connector.connect(
      host=os.getenv("mysql_hostname"),
      user=os.getenv("mysql_username"),
      passwd=os.getenv("mysql_password"),
      database=os.getenv("mysql_db")
    )
    mysql_cursor = mysql_conn.cursor()
    return mysql_conn, mysql_cursor


def get_swaps_data_from_mysql(day_since):
    conn, cursor = get_mysql()
    cursor.execute(f"SELECT * FROM swaps WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    return cursor.fetchall()


def get_failed_swaps_data_from_mysql(day_since):
    conn, cursor = get_mysql()
    cursor.execute(f"SELECT * FROM swaps_failed WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    return cursor.fetchall()
