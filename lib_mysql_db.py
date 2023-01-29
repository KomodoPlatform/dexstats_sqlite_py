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


def create_sqlite_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        logger.error(e)


sql_create_swaps_table = """CREATE TABLE IF NOT EXISTS swaps (
                                id integer PRIMARY KEY,
                                started_at text NOT NULL,
                                uuid text NOT NULL UNIQUE,
                                taker_coin integer NOT NULL,
                                taker_amount integer NOT NULL,
                                taker_gui text,
                                taker_version text,
                                taker_pubkey text,
                                maker_coin integer NOT NULL,
                                maker_amount integer NOT NULL,
                                maker_gui text,
                                maker_version text,
                                maker_pubkey text,
                                epoch int NOT NULL
                            );"""


sql_create_failed_swaps_table = """CREATE TABLE IF NOT EXISTS failed_swaps (
                                id integer PRIMARY KEY,
                                started_at text NOT NULL,
                                uuid text NOT NULL UNIQUE,
                                taker_coin integer NOT NULL,
                                taker_amount integer NOT NULL,
                                taker_error_type text,
                                taker_error_msg text,
                                taker_gui text,
                                taker_version text,
                                taker_pubkey text,
                                maker_coin integer NOT NULL,
                                maker_amount integer NOT NULL,
                                maker_error_type text,
                                maker_error_msg text,
                                maker_gui text,
                                maker_version text,
                                maker_pubkey text,
                                epoch int NOT NULL
                            );"""

def get_swaps_data_from_mysql(day_since):
    conn, cursor = get_mysql()
    cursor.execute(f"SELECT * FROM swaps WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    return cursor.fetchall()


def get_failed_swaps_data_from_mysql(day_since):
    conn, cursor = get_mysql()
    cursor.execute(f"SELECT * FROM swaps_failed WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    return cursor.fetchall()
