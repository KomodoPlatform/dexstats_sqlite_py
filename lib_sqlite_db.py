#!/usr/bin/env python3
import os
import sqlite3
from dotenv import load_dotenv
from datetime import timezone
from lib_logger import logger
from lib_helper import days_ago

load_dotenv()

SEEDNODE_DB = os.getenv("SEEDNODE_DB")
if not SEEDNODE_DB:
    SEEDNODE_DB = "MM2.db"


def get_sqlite(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.error(e)
    return conn


def get_all_swaps(success_only=True):
    conn = sqlite3.connect(SEEDNODE_DB)
    sql_cursor = conn.cursor()
    with conn:
        if success_only:
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE is_success=?;", (1,))
        else:
            sql_cursor.execute("SELECT * FROM stats_swaps;")
        return sql_cursor.fetchall()


def get_swaps_since(days, success_only=True):
    conn = sqlite3.connect(SEEDNODE_DB)
    sql_cursor = conn.cursor()
    with conn:
        if success_only:
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE finished_at > ? AND  is_success=?;", (lib_helper.days_ago(days), 1,))
        else:
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE finished_at > ?;", (lib_helper.days_ago(days),))
        return sql_cursor.fetchall()


def get_swaps_between(start_epoch, end_epoch, success_only=True):
    conn = sqlite3.connect(SEEDNODE_DB)
    sql_cursor = conn.cursor()
    with conn:
        if success_only:
            t = (start_epoch, end_epoch, 1,)
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE finished_at > ? AND finished_at < ? AND is_success=?;", t)
        else:
            t = (start_epoch, end_epoch,)
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE finished_at > ? AND finished_at < ?;", t)
        return sql_cursor.fetchall()


def get_pair_swaps_since(epoch, maker_coin, taker_coin, row_factory=False):
    conn = sqlite3.connect(SEEDNODE_DB)
    if row_factory:
        conn.row_factory = sqlite3.Row
    sql_cursor = conn.cursor()
    with conn:
        if success_only:
            t = (epoch, maker_coin, taker_coin,1,)
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE finished_at > ? AND taker_coin=? AND maker_coin=? AND is_success=?;", t)
        else:
            t = (epoch, maker_coin, taker_coin,)
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE finished_at > ? AND taker_coin=? AND maker_coin=?;", t)
        return sql_cursor.fetchall()


def get_pair_swaps_between(start_epoch, end_epoch, maker_coin, taker_coin):
    conn = sqlite3.connect(SEEDNODE_DB)
    sql_cursor = conn.cursor()
    with conn:
        if success_only:
            t = (start_epoch, end_epoch, maker_coin, taker_coin,1,)
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE finished_at > ? AND finished_at < ? AND taker_coin=? AND maker_coin=? AND is_success=?;", t)
        else:
            t = (start_epoch, end_epoch, maker_coin, taker_coin,)
            sql_cursor.execute("SELECT * FROM stats_swaps WHERE finished_at > ? AND finished_at < ? AND taker_coin=? AND maker_coin=?;", t)
        return sql_cursor.fetchall()


# tuple, integer -> list (with swap status dicts)
# select from DB swap statuses for desired pair with timestamps > than provided
def get_swaps_since_timestamp_for_pair(pair_tickers, timestamp):
    pair_swaps_24hr = get_pair_swaps_since(timestamp, pair_tickers[0], pair_tickers[1])
    logger.info(pair_swaps_24hr)
    swap_statuses_a_b = [dict(row) for row in pair_swaps_24hr]
    for swap in swap_statuses_a_b:
        swap["trade_type"] = "buy"
    reversed_pair_swaps_24hr = get_pair_swaps_since(timestamp, pair_tickers[1], pair_tickers[0])
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


if __name__ == '__main__':
    conn = sqlite3.connect(SEEDNODE_DB)
    sql_cursor = conn.cursor()
    with conn:
        t = ("486b21d8-a90f-4833-8bd6-d90045a641bc",)
        sql_cursor.execute("SELECT * FROM stats_swaps WHERE uuid=?;", t)
        logger.info(sql_cursor.fetchall())

