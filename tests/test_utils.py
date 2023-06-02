#!/usr/bin/env python3
import sqlite3
from test_db import setup_swaps_test_data, setup_database
import models


def test_get_suffix():
    utils = models.Utils()
    assert utils.get_suffix(1) == "24h"
    assert utils.get_suffix(8) == "8d"

    coins = utils.get_related_coins("LTC")
    assert "LTC" in coins
    assert "LTC-segwit" in coins

    coins = utils.get_related_coins("KMD")
    assert "KMD" in coins
    assert "KMD-BEP20" in coins

    coins = utils.get_related_coins("USDC-BEP20")
    assert "USDC" not in coins
    assert "USDC-BEP20" in coins
    assert "USDC-PLG20" in coins


def test_get_gecko_usd_price(setup_swaps_test_data):
    DB = setup_swaps_test_data
    gecko = models.Gecko()
    assert gecko.get_gecko_usd_price("KMD", DB.gecko_data) == 1
    assert gecko.get_gecko_usd_price("BTC", DB.gecko_data) == 1000000
    price1 = gecko.get_gecko_usd_price("LTC-segwit", DB.gecko_data)
    price2 = gecko.get_gecko_usd_price("LTC", DB.gecko_data)
    assert price1 == price2


def test_get_volumes_and_prices(setup_swaps_test_data):
    # DB = setup_swaps_test_data
    # pair = ("DGB", "LTC")
    # swaps_for_pair = DB.get_swaps_for_pair(pair)
    # TODO: Needs a fixture
    pass


def test_find_lowest_ask():
    # TODO: Needs a fixture
    pass


def test_find_highest_bid():
    # TODO: Needs a fixture
    pass


def test_get_and_parse_orderbook():
    # TODO: Needs a fixture
    pass


def test_summary_for_pair():
    # TODO: Needs a fixture
    pass


def test_ticker_for_pair():
    # TODO: Needs a fixture
    pass


def test_orderbook_for_pair():
    # TODO: Needs a fixture
    pass


def test_trades_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.conn.row_factory = sqlite3.Row
    DB.sql_cursor = DB.conn.cursor()

    pair = models.Pair("DGB_KMD")
    r = pair.trades(DB=DB)
    assert len(r) == 1
    assert r[0]["type"] == "buy"
    assert r[0]["price"] == "{:.10f}".format(0.001)

    pair = models.Pair("KMD_DGB")
    r = pair.trades(DB=DB)
    assert len(r) == 1
    assert r[0]["type"] == "sell"
    assert r[0]["price"] == "{:.10f}".format(1000)

    pair = models.Pair("notAticker")
    r = pair.trades(DB=DB)
    assert r == []

    pair = models.Pair("X_Y")
    r = pair.trades(DB=DB)
    assert r == []
    # TODO: Add extra tests once linked to fixtures for test db


def test_get_chunks():
    gecko = models.Gecko()
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunks = list(gecko.get_chunks(data, 3))
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[3]) == 1


def test_get_coins_config():
    # TODO: Needs a fixture
    pass


def test_get_data_from_gecko():
    # Not TODO: This queries external api, so not sure how to test yet.
    # Might break it down a it into smaller functions
    pass


def test_atomicdex_info(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.sql_cursor = DB.conn.cursor()
    logic = models.CacheLogic()
    r = logic.atomicdex_info(1, DB)
    assert r["swaps_all_time"] == 9
    assert r["swaps_24h"] == 4
    assert r["swaps_30d"] == 7
    # TODO: This value references the orderbook. Will need to add fixture
    # assert r["current_liquidity"] == 504180.75


def test_get_liquidity():
    # TODO: Needs a fixture
    pass


def test_get_value():
    # TODO: Needs a fixture
    pass


def test_atomicdex_timespan_info():
    # TODO: Needs a fixture
    pass


def test_get_top_pairs():
    # TODO: Needs a fixture
    pass
