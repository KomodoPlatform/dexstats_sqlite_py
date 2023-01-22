#!/usr/bin/env python3
import os
import sqlite3
from sqlite3 import Error
import mysql.connector
from dotenv import load_dotenv

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

def get_sqlite(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    return conn


def create_sqlite_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


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
                                time_stamp int NOT NULL
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
                                time_stamp int NOT NULL
                            );"""
