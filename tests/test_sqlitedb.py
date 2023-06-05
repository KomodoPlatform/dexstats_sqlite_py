#!/usr/bin/env python3
import sys
import time
import sqlite3
import pytest
from decimal import Decimal
from logger import logger
import models
import const
from fixtures import setup_swaps_test_data, setup_database, setup_time, logger

now = int(time.time())
hour_ago = now - 3600
two_hours_ago = now - 7200
day_ago = now - 86400
week_ago = now - 604800
month_ago = now - 2592000
two_months_ago = now - 5184000


def test_get_pairs(setup_swaps_test_data):
    # Confirm pairs response is correct
    DB = setup_swaps_test_data
    pairs = DB.get_pairs()
    assert ("MCL", "KMD") not in pairs
    assert ("DGB", "LTC") not in pairs
    assert ("KMD", "BTC") in pairs
    assert ("BTC", "KMD") not in pairs
    assert len(pairs) == 6
    pairs = DB.get_pairs(45)
    assert ("MCL", "KMD") not in pairs
    assert ("DGB", "LTC") in pairs
    assert ("DOGE", "LTC") not in pairs
    pairs = DB.get_pairs(90)
    assert ("DOGE", "LTC") in pairs


def test_get_swaps_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.conn.row_factory = sqlite3.Row
    DB.sql_cursor = DB.conn.cursor()

    swaps = DB.get_swaps_for_pair(("MCL", "KMD"), day_ago)
    assert len(swaps) == 0

    swaps = DB.get_swaps_for_pair(("DOGE", "BTC"), day_ago)
    assert len(swaps) == 1
    assert swaps[0]["trade_type"] == "sell"

    swaps = DB.get_swaps_for_pair(("DGB", "LTC"), two_months_ago)
    assert len(swaps) == 2
    assert swaps[0]["trade_type"] == "buy"


def test_last_price_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.conn.row_factory = sqlite3.Row
    DB.sql_cursor = DB.conn.cursor()

    last_price = DB.get_last_price_for_pair("DGB", "LTC")
    assert last_price == Decimal('0.00001')

    last_price = DB.get_last_price_for_pair("LTC", "DGB")
    assert last_price == 100000

    last_price = DB.get_last_price_for_pair("KMD", "BTC")
    assert last_price == Decimal('0.000001')

    last_price = DB.get_last_price_for_pair("BTC", "KMD")
    assert last_price == 1000000

    last_price = DB.get_last_price_for_pair("x", "y")
    assert last_price == 0


def test_timespan_swaps(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.sql_cursor = DB.conn.cursor()

    swaps = DB.get_timespan_swaps()
    assert len(swaps) == 7

    swaps = DB.get_timespan_swaps(7)
    logger.info(swaps)
    assert len(swaps) == 8

    swaps = DB.get_timespan_swaps(30)
    assert len(swaps) == 10

    swaps = DB.get_timespan_swaps(60)
    assert len(swaps) == 11

    swaps = DB.get_timespan_swaps(9999)
    assert len(swaps) == 12


def test_get_atomicdexio(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.sql_cursor = DB.conn.cursor()
    resp = DB.get_atomicdexio()
    assert resp == {
        "swaps_all_time": 12,
        "swaps_24h": 7,
        "swaps_30d": 10
    }


def get_actual_db_data(maker_coin: str = "BTC", limit: int = 5):
    '''
    This is just here for convienince to get data from
    actual DB for use in testing fixtures
    '''
    DB = models.SqliteDB(const.MM2_DB_PATH)
    DB.sql_cursor.execute('select * from stats_swaps where maker_coin = "BTC" limit 5')
    data = []
    for r in DB.sql_cursor.fetchall():
        data.append(r)
    DB.close()
    return data


def test_get_actual_db_data():
    r = get_actual_db_data()
    assert len(r) <= 5
