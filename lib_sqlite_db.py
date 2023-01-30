#!/usr/bin/env python3
import os
import sqlite3
from dotenv import load_dotenv
from datetime import timezone
from lib_logger import logger
from lib_helper import days_ago

load_dotenv()

def get_sqlite(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.error(e)
    return conn


def import_mysql_swaps_into_sqlite(day_since, mysql_swaps_data):
    sqlite_conn = get_sqlite('seednode_swaps.db')
    with sqlite_conn:
        imported = 0
        for x in mysql_swaps_data:
            x = list(x)
            x.append(int(x[2].replace(tzinfo=timezone.utc).timestamp()))
            sql = ''' REPLACE INTO swaps(id,uuid,started_at,taker_coin,taker_amount,
                                        taker_gui,taker_version,taker_pubkey,maker_coin,
                                        maker_amount,maker_gui,maker_version,maker_pubkey,
                                        epoch)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            cur = sqlite_conn.cursor()
            cur.execute(sql, x)
            sqlite_conn.commit()
            imported += 1
    return imported

            
def import_mysql_failed_swaps_into_sqlite(day_since, mysql_failed_swaps_data):
    sqlite_conn = get_sqlite('seednode_failed_swaps.db')
    with sqlite_conn:
        # get existing uuids
        imported = 0
        for x in mysql_failed_swaps_data:
            x = list(x)
            x.append(int(x[1].replace(tzinfo=timezone.utc).timestamp()))            
            sql = ''' REPLACE INTO failed_swaps(uuid,started_at,taker_coin,taker_amount,
                                        taker_error_type,taker_error_msg,taker_gui,taker_version,taker_pubkey,maker_coin,
                                        maker_amount,maker_error_type,maker_error_msg,maker_gui,maker_version,maker_pubkey,
                                        epoch)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            cur = sqlite_conn.cursor()
            cur.execute(sql, x)
            sqlite_conn.commit()
            imported += 1
    return imported



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


def get_all_swaps():
    conn = sqlite3.connect('seednode_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        sql_cursor.execute("SELECT * FROM swaps;")
        return sql_cursor.fetchall()


def get_swaps_since(days):
    conn = sqlite3.connect('seednode_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ?;", (days_ago(days),))
        return sql_cursor.fetchall()


def get_swaps_between(start_epoch, end_epoch):
    conn = sqlite3.connect('seednode_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        t = (start_epoch, end_epoch,)
        sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND time_stamp < ?;", t)
        return sql_cursor.fetchall()


def get_pair_swaps_since(epoch, maker_coin, taker_coin):
    conn = sqlite3.connect('seednode_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        t = (epoch, maker_coin, taker_coin,)
        sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND taker_coin=? AND maker_coin=?;", t)
        return sql_cursor.fetchall()


def get_pair_swaps_between(start_epoch, end_epoch, maker_coin, taker_coin):
    conn = sqlite3.connect('seednode_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        t = (start_epoch, end_epoch, maker_coin, taker_coin,)
        sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND time_stamp < ? AND taker_coin=? AND maker_coin=?;", t)
        return sql_cursor.fetchall()


def get_all_failed_swaps():
    conn = sqlite3.connect('seednode_failed_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        sql_cursor.execute("SELECT * FROM failed_swaps;")
        return sql_cursor.fetchall()


def get_swaps_failed_since(days):
    conn = sqlite3.connect('seednode_failed_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        sql_cursor.execute("SELECT * FROM failed_swaps WHERE time_stamp > ?;", (days_ago(days),))
        return sql_cursor.fetchall()


def get_swaps_between(start_epoch, end_epoch):
    conn = sqlite3.connect('seednode_failed_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        t = (start_epoch, end_epoch,)
        sql_cursor.execute("SELECT * FROM failed_swaps WHERE time_stamp > ? AND time_stamp < ?;", t)
        return sql_cursor.fetchall()


def get_pair_failed_swaps_since(epoch, maker_coin, taker_coin):
    conn = sqlite3.connect('seednode_failed_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        t = (epoch, maker_coin, taker_coin,)
        sql_cursor.execute("SELECT * FROM failed_swaps WHERE time_stamp > ? AND taker_coin=? AND maker_coin=?;", t)
        return sql_cursor.fetchall()


def get_pair_failed_swaps_between(start_epoch, end_epoch, maker_coin, taker_coin):
    conn = sqlite3.connect('seednode_failed_swaps.db')
    sql_cursor = conn.cursor()
    with conn:
        t = (start_epoch, end_epoch, maker_coin, taker_coin,)
        sql_cursor.execute("SELECT * FROM failed_swaps WHERE time_stamp > ? AND time_stamp < ? AND taker_coin=? AND maker_coin=?;", t)
        return sql_cursor.fetchall()


# tuple, integer -> list (with swap status dicts)
# select from DB swap statuses for desired pair with timestamps > than provided
def get_swaps_since_timestamp_for_pair(pair, timestamp):
    pair_swaps_24hr = get_pair_swaps_since(timestamp, pair[0], pair[1])
    swap_statuses_a_b = [dict(row) for row in pair_swaps_24hr]
    for swap in swap_statuses_a_b:
        swap["trade_type"] = "buy"
    reversed_pair_swaps_24hr = get_pair_swaps_since(timestamp, pair[1], pair[0])
    swap_statuses_b_a = [dict(row) for row in reversed_pair_swaps_24hr]
    for swap in swap_statuses_b_a:
        swap["trade_type"] = "sell"
    swap_statuses = swap_statuses_a_b + swap_statuses_b_a
    return swap_statuses


# tuple, integer -> list (with swap status dicts)
# select from json cache swap statuses for desired pair with timestamps > than provided
def get_24hr_swaps_for_pair(pair):
    return get_swaps_since_timestamp_for_pair(pair, lib_helper.days_ago(1))


def get_swaps_between_timestamps_for_pair(pair, start_epoch, end_epoch):
    pair_results = get_pair_swaps_between(start_epoch, end_epoch, pair[0], pair[1])
    swap_statuses_a_b = [dict(row) for row in pair_results]
    for swap in swap_statuses_a_b:
        swap["trade_type"] = "buy"
    reversed_pair_results = get_pair_swaps_between(start_epoch, end_epoch, pair[1], pair[0])
    swap_statuses_b_a = [dict(row) for row in reversed_pair_results]
    for swap in swap_statuses_b_a:
        swap["trade_type"] = "sell"
    swap_statuses = swap_statuses_a_b + swap_statuses_b_a
    return swap_statuses

