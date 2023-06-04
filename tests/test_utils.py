#!/usr/bin/env python3
from fixtures import (
    setup_swaps_test_data,
    setup_database,
    setup_utils,
    setup_orderbook_data,
    setup_time,
    setup_cache,
    logger,
)


def test_find_lowest_ask(setup_utils, setup_orderbook_data):
    utils = setup_utils
    orderbook = setup_orderbook_data
    r = utils.find_lowest_ask(orderbook)
    assert float(r) > 0


def test_find_highest_bid(setup_utils, setup_orderbook_data):
    utils = setup_utils
    orderbook = setup_orderbook_data
    r = utils.find_highest_bid(orderbook)
    assert float(r) > 0


def test_get_suffix(setup_utils):
    utils = setup_utils
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


def test_get_chunks(setup_utils):
    utils = setup_utils
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunks = list(utils.get_chunks(data, 3))
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[3]) == 1


def test_download_json(setup_utils):
    utils = setup_utils
    data = utils.download_json("https://api.coingecko.com/api/v3/coins/list")
    assert len(data) > 0
    data = utils.download_json("foo")
    assert data is None

    # TODO: Needs a fixture
    pass


def test_get_gecko_data():
    # Not TODO: This queries external api, so not sure how to test yet.
    # Might break it down a it into smaller functions
    pass


def test_get_liquidity():
    # TODO: Needs a fixture
    pass


def test_get_value():
    # TODO: Needs a fixture
    pass


def test_atomicdex_fortnight():
    # TODO: Needs a fixture
    pass


def test_get_top_pairs():
    # TODO: Needs a fixture
    pass


def test_get_gecko_usd_price(setup_utils, setup_cache):
    utils = setup_utils
    cache = setup_cache
    assert cache.files.gecko_data == "tests/fixtures/gecko_cache.json"
    assert utils.get_gecko_usd_price("KMD", cache.gecko_data) == 1
    assert utils.get_gecko_usd_price("BTC", cache.gecko_data) == 1000000
    price1 = utils.get_gecko_usd_price("LTC-segwit", cache.gecko_data)
    price2 = utils.get_gecko_usd_price("LTC", cache.gecko_data)
    assert price1 == price2
