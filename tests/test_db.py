#!/usr/bin/env python3
import sys
import time
import sqlite3
import pytest
from decimal import Decimal
sys.path.append("../dexstats_sqlite_py")
from logger import logger
import models
import const

now = int(time.time())
hour_ago = now - 3600
day_ago = now - 86400
week_ago = now - 604800
month_ago = now - 2592000
two_months_ago = now - 5184000


@pytest.fixture
def setup_database():
    """ Fixture to set up the in-memory database with test data """
    DB = models.sqliteDB(':memory:', False, "tests/test_gecko_cache.json")
    DB.sql_cursor.execute('''
        CREATE TABLE stats_swaps (
            id INTEGER NOT NULL PRIMARY KEY,
            maker_coin VARCHAR(255) NOT NULL,
            taker_coin VARCHAR(255) NOT NULL,
            uuid VARCHAR(255) NOT NULL UNIQUE,
            started_at INTEGER NOT NULL,
            finished_at INTEGER NOT NULL,
            maker_amount DECIMAL NOT NULL,
            taker_amount DECIMAL NOT NULL,
            is_success INTEGER NOT NULL,
            maker_coin_ticker VARCHAR(255) NOT NULL DEFAULT '',
            maker_coin_platform VARCHAR(255) NOT NULL DEFAULT '',
            taker_coin_ticker VARCHAR(255) NOT NULL DEFAULT '',
            taker_coin_platform VARCHAR(255) NOT NULL DEFAULT '',
            maker_coin_usd_price DECIMAL,
            taker_coin_usd_price DECIMAL
        );
    ''')
    yield DB


@pytest.fixture
def setup_swaps_test_data(setup_database):
    DB = setup_database
    sample_data = [
        (9, 'KMD', 'MORTY', '01fe4251-ffe1-4c7a-ad7f-04b1df6323b6', hour_ago,
         hour_ago + 20, 1, 1, 1, 'KMD', '', 'RICK', '', None, None),
        (11, 'DGB-segwit', 'KMD-BEP20', '50fe4211-fd33-4dc4-2a7f-f6320b1d3b64', hour_ago,
         hour_ago + 20, 1000, 1, 1, 'DGB', 'segwit', 'KMD', 'BEP20', None, None),
        (22, 'MCL', 'KMD', '4d1dc872-7262-46b7-840d-5e9b1aad243f', hour_ago,
         hour_ago + 20, 1, 1, 0, 'KMD', '', 'USDC', '', None, None),
        (27, 'BTC', 'MATIC', '8724d1dc-2762-4633-8add-6ad2e9b1a4e7', hour_ago,
         hour_ago + 20, 1, 1, 1, 'BTC', '', 'MATIC', '', None, None),
        (47, 'KMD', 'BTC', '24d1dc87-7622-6334-add8-9b1a4e76ad2e', hour_ago - 10,
         hour_ago + 10, 1000000, 1, 1, 'KMD', '', 'BTC', '', None, None),
        (36, 'KMD-BEP20', 'BTC', '03d3afc2-273f-40a5-bcd4-31efdb6bcc8b', day_ago,
         day_ago + 20, 2000000, 1, 1, 'KMD', 'BEP20', 'BTC', '', None, None),
        (44, 'BTC', 'LTC', 'acf3e087-ac6f-4649-b420-5eb8e2924bf2', week_ago,
         week_ago + 20, 5, 1, 1, 'BTC', '', 'LTC', '', None, None),
        (52, 'DGB', 'LTC-segwit', 'f3e0ac87-40a5-4649-b420-5eb8e2924bf2', week_ago,
         week_ago + 20, 100000, 1, 1, 'DGB', '', 'LTC', 'segwit', None, None),
        (55, 'DGB-segwit', 'LTC', 'cf3e0387-ac6f-a2fb-b360-4bf25fed4292', month_ago,
         month_ago + 20, 200000, 1, 1, 'DGB', 'segwit', 'LTC', '', None, None),
        (66, 'BTC-BEP20', 'DOGE', '50d8e2e4-ee4b-494f-a2fb-48467614b613', two_months_ago,
         two_months_ago + 20, 1, 1, 1, 'BTC', 'BEP20', 'DOGE', '', None, None),
    ]
    sql = 'INSERT INTO stats_swaps VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    DB.sql_cursor.executemany(sql, sample_data)
    yield DB


def test_get_pairs(setup_swaps_test_data):
    # Confirm pairs response is correct
    DB = setup_swaps_test_data
    pairs = DB.get_pairs()
    assert ("MCL", "KMD") not in pairs
    assert ("DGB", "LTC") not in pairs
    assert ("KMD", "BTC") in pairs
    assert ("BTC", "KMD") not in pairs
    pairs = DB.get_pairs(45)
    assert ("MCL", "KMD") not in pairs
    assert ("DGB", "LTC") in pairs
    assert ("DOGE", "BTC") not in pairs
    pairs = DB.get_pairs(90)
    assert ("DOGE", "BTC") in pairs


def test_get_swaps_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.conn.row_factory = sqlite3.Row
    DB.sql_cursor = DB.conn.cursor()

    swaps = DB.get_swaps_for_pair(("MCL", "KMD"), day_ago)
    assert len(swaps) == 0

    swaps = DB.get_swaps_for_pair(("MATIC", "BTC"), day_ago)
    assert len(swaps) == 1
    assert swaps[0]["trade_type"] == "sell"

    swaps = DB.get_swaps_for_pair(("DGB", "LTC"), two_months_ago)
    assert len(swaps) == 2
    assert swaps[0]["trade_type"] == "buy"


def test_last_price_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.conn.row_factory = sqlite3.Row
    DB.sql_cursor = DB.conn.cursor()

    last_price = DB.get_last_price_for_pair(("DGB", "LTC"))
    assert last_price == Decimal('0.00001')

    last_price = DB.get_last_price_for_pair(("LTC", "DGB"))
    assert last_price == 100000

    last_price = DB.get_last_price_for_pair(("KMD", "BTC"))
    assert last_price == Decimal('0.000001')

    last_price = DB.get_last_price_for_pair(("BTC", "KMD"))
    assert last_price == 1000000

    last_price = DB.get_last_price_for_pair(("x", "y"))
    assert last_price == 0


def test_timespan_swaps(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.sql_cursor = DB.conn.cursor()

    swaps = DB.get_timespan_swaps()
    assert len(swaps) == 4

    swaps = DB.get_timespan_swaps(7)
    logger.info(swaps)
    assert len(swaps) == 5

    swaps = DB.get_timespan_swaps(30)
    assert len(swaps) == 7

    swaps = DB.get_timespan_swaps(60)
    assert len(swaps) == 8

    swaps = DB.get_timespan_swaps(9999)
    assert len(swaps) == 9


def test_get_adex_summary(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.sql_cursor = DB.conn.cursor()
    resp = DB.get_adex_summary()
    assert resp == {
        "swaps_all_time": 9,
        "swaps_24h": 4,
        "swaps_30d": 7
    }


def get_actual_db_data(maker_coin: str = "BTC", limit: int = 5):
    '''
    This is just here for convienince to get data from
    actual DB for use in testing fixtures
    '''
    DB = models.sqliteDB(const.MM2_DB_PATH)
    DB.sql_cursor.execute('select * from stats_swaps where maker_coin = "BTC" limit 5')
    data = []
    for r in DB.sql_cursor.fetchall():
        data.append(r)
    DB.close()
    return data


def test_get_actual_db_data():
    r = get_actual_db_data()
    assert len(r) <= 5
