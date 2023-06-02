#!/usr/bin/env python3
import sqlite3
from test_sqlitedb import setup_swaps_test_data, setup_database
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


def test_get_chunks():
    utils = models.Utils()
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunks = list(utils.get_chunks(data, 3))
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
