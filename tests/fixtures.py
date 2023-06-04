#!/usr/bin/env python3
import sys
import pytest
sys.path.append('../dexstats_sqlite_py')
import models
from logger import logger


@pytest.fixture
def setup_orderbook(setup_swaps_test_data, setup_kmd_ltc_str_pair_with_db):
    pair = setup_kmd_ltc_str_pair_with_db
    yield models.Orderbook(pair=pair, testing=True)


@pytest.fixture
def setup_dexapi():
    yield models.DexAPI()


@pytest.fixture
def setup_time():
    yield models.Time()


@pytest.fixture
def setup_gecko():
    yield models.Gecko(testing=True)


@pytest.fixture
def setup_gecko_coin_ids(setup_gecko):
    gecko = setup_gecko
    return gecko.get_gecko_coin_ids_list()


@pytest.fixture
def setup_gecko_info(setup_gecko):
    gecko = setup_gecko
    return gecko.get_gecko_info_dict()


@pytest.fixture
def setup_swaps(setup_swaps_test_data):
    yield models.Swaps(pair="", testing=True)


@pytest.fixture
def setup_cache(setup_swaps_test_data):
    yield models.Cache(testing=True, DB=setup_swaps_test_data)


@pytest.fixture
def setup_templates():
    templates = models.Templates()
    yield templates


@pytest.fixture
def setup_rick_morty_tuple_pair():
    yield models.Pair(("RICK", "MORTY"))


@pytest.fixture
def setup_rick_morty_str_pair():
    yield models.Pair("RICK_MORTY")


@pytest.fixture
def setup_dgb_kmd_str_pair_with_db(setup_swaps_test_data):
    yield models.Pair("DGB_KMD", DB=setup_swaps_test_data)


@pytest.fixture
def setup_kmd_ltc_str_pair_with_db(setup_swaps_test_data):
    yield models.Pair("KMD_LTC", DB=setup_swaps_test_data)


@pytest.fixture
def setup_kmd_dgb_tuple_pair_with_db(setup_swaps_test_data):
    yield models.Pair(("KMD", "DGB"), DB=setup_swaps_test_data)


@pytest.fixture
def setup_not_a_real_pair():
    yield models.Pair("NotARealPair")


@pytest.fixture
def setup_not_existing_pair():
    yield models.Pair("XYZ_123")


@pytest.fixture
def setup_utils():
    yield models.Utils()


@pytest.fixture
def setup_orderbook_data(setup_utils):
    utils = setup_utils
    orderbook = utils.load_jsonfile("tests/fixtures/orderbook_BTC_DOGE.json")
    yield orderbook


@pytest.fixture
def setup_database():
    """ Fixture to set up the in-memory database with test data """
    DB = models.SqliteDB(':memory:', False, True)
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
def setup_swaps_test_data(setup_database, setup_time):
    DB = setup_database
    time = setup_time
    sample_data = [
        (1, 'KMD', 'MORTY', '01fe4251-ffe1-4c7a-ad7f-04b1df6323b6', time.hours_ago(1) - 1,
         time.hours_ago(1) + 1, 1, 1, 1, 'KMD', '', 'MORTY', '', None, None),
        (2, 'DGB-segwit', 'KMD-BEP20', '50fe4211-fd33-4dc4-2a7f-f6320b1d3b64',
         time.hours_ago(1) - 2, time.hours_ago(1) + 2, 1000, 1, 1, 'DGB', 'segwit',
         'KMD', 'BEP20', None, None),
        (3, 'MCL', 'KMD', '4d1dc872-7262-46b7-840d-5e9b1aad243f', time.hours_ago(1) - 3,
         time.hours_ago(1) + 3, 1, 1, 0, 'MCL', '', 'KMD', '', None, None),
        (4, 'DGB', 'DOGE', '0d232e54-ee4b-494f-a2fb-48467614b613', time.hours_ago(1) - 4,
         time.hours_ago(1) + 4, 1, 1, 1, 'DGB', '', 'DOGE', '', None, None),
        (5, 'LTC', 'KMD', '71dcc872-75a2-d4ef-009d-5e9baad162ef', time.hours_ago(1) - 5,
         time.hours_ago(1) + 5, 1, 10, 1, 'LTC', '', 'KMD', '', None, None),
        (6, 'BTC', 'DOGE', '8724d1dc-2762-4633-8add-6ad2e9b1a4e7', time.hours_ago(1) - 6,
         time.hours_ago(1) + 6, 1, 1, 1, 'BTC', '', 'DOGE', '', None, None),
        (7, 'KMD', 'BTC', '24d1dc87-7622-6334-add8-9b1a4e76ad2e', time.hours_ago(1) - 7,
         time.hours_ago(1) + 7, 1000000, 1, 1, 'KMD', '', 'BTC', '', None, None),
        (8, 'LTC', 'KMD', 'c871dc72-73a2-d46d-900d-aad5e9b162ef', time.hours_ago(2) - 8,
         time.hours_ago(2) + 8, 1, 100, 1, 'LTC', '', 'KMD', '', None, None),
        (9, 'KMD-BEP20', 'BTC', '03d3afc2-273f-40a5-bcd4-31efdb6bcc8b', time.days_ago(1) - 9,
         time.days_ago(1) + 9, 2000000, 1, 1, 'KMD', 'BEP20', 'BTC', '', None, None),
        (10, 'BTC', 'LTC', 'acf3e087-ac6f-4649-b420-5eb8e2924bf2', time.days_ago(7) - 10,
         time.days_ago(7) + 10, 5, 1, 1, 'BTC', '', 'LTC', '', None, None),
        (11, 'DGB', 'LTC-segwit', 'f3e0ac87-40a5-4649-b420-5eb8e2924bf2', time.days_ago(7) - 11,
         time.days_ago(7) + 11, 100000, 1, 1, 'DGB', '', 'LTC', 'segwit', None, None),
        (12, 'DGB-segwit', 'LTC', 'cf3e0387-ac6f-a2fb-b360-4bf25fed4292', time.days_ago(30) - 12,
         time.days_ago(30) + 12, 200000, 1, 1, 'DGB', 'segwit', 'LTC', '', None, None),
        (13, 'LTC', 'DOGE', '50d8e2e4-ee4b-494f-a2fb-48467614b613', time.days_ago(60) - 13,
         time.days_ago(60) + 13, 1, 1, 1, 'LTC', '', 'DOGE', '', None, None),
    ]
    sql = 'INSERT INTO stats_swaps VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    DB.sql_cursor.executemany(sql, sample_data)
    yield DB
