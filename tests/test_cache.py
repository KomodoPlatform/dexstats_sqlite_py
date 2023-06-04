#!/usr/bin/env python3
import sys
import pytest
sys.path.append("../dexstats_sqlite_py")
from logger import logger
import models
from test_sqlitedb import setup_swaps_test_data, setup_database


@pytest.fixture
def setup_cache(setup_swaps_test_data):
    yield models.Cache(testing=True, DB=setup_swaps_test_data)


# /////////////////////// #
# Cache.load class tests  #
# /////////////////////// #
def test_get_atomicdexio(setup_cache):
    get = setup_cache.load
    r = get.atomicdexio()
    for i in r:
        assert r[i] > 0
        assert r[i] is not None
        assert isinstance(r[i], (float, int))


def test_get_atomicdex_fortnight(setup_cache):
    get = setup_cache.load
    r = get.atomicdex_fortnight()
    for i in r:
        if i != "top_pairs":
            assert r[i] > 0
            assert r[i] is not None
            assert isinstance(r[i], (float, int))
        else:
            for j in r[i]:
                assert j in [
                    "by_value_traded_usd",
                    "by_current_liquidity_usd",
                    "by_swaps_count"
                ]
                for k in r[i][j]:
                    logger.info(i)
                    logger.info(j)
                    logger.info(k)
                    assert "_" in k
                    assert r[i][j][k] > 0
                    assert isinstance(r[i][j][k], (float, int))


def test_get_coins_config(setup_cache):
    get = setup_cache.load
    data = get.coins_config()
    assert "KMD" in data
    assert "KMD-BEP20" in data
    assert "LTC-segwit" in data
    assert "LTC" in data
    for i in data:
        assert i == data[i]["coin"]
        assert "coingecko_id" in data[i]


def test_get_coins(setup_cache):
    get = setup_cache.load
    assert len(get.coins()) > 0


def test_get_gecko(setup_cache):
    get = setup_cache.load
    gecko = get.gecko_data()
    assert "KMD" in gecko
    assert gecko["KMD"]["usd_market_cap"] == gecko["KMD-BEP20"]["usd_market_cap"]
    assert gecko["KMD"]["usd_price"] == gecko["KMD-BEP20"]["usd_price"]
    assert gecko["KMD"]["coingecko_id"] == gecko["KMD-BEP20"]["coingecko_id"]
    for i in gecko["KMD"]:
        assert i in ["usd_market_cap", "usd_price", "coingecko_id"]
    for i in gecko:
        assert gecko[i]["coingecko_id"] != ""


def test_get_summary(setup_cache):
    get = setup_cache.load
    data = get.summary()
    for i in data:
        if i["trading_pair"] == "MORTY_KMD":
            assert i["base_currency"] == "MORTY"
            assert i["quote_currency"] == "KMD"
            assert i["pair_swaps_count"] == 1
            assert i["base_price_usd"] == 0
            assert i["rel_price_usd"] == 1
            assert i["price_change_percent_24h"] == '0.0000000000'
            assert i["base_liquidity_usd"] == 0
            assert i["base_trade_value_usd"] == 0
            assert i["rel_liquidity_usd"] == 0
            assert i["rel_trade_value_usd"] == 1


def test_get_summary2(setup_cache):
    get = setup_cache.load
    data = get.summary()
    for i in data:
        if i["trading_pair"] == "KMD_LTC":
            assert i["base_currency"] == "KMD"
            assert i["quote_currency"] == "LTC"
            assert i["base_price_usd"] > 0
            assert i["rel_price_usd"] > 0
            if i["pair_swaps_count"] > 1:
                assert float(i["price_change_percent_24h"]) == 0.0009
                assert float(i["highest_price_24h"]) > 0
                assert float(i["lowest_price_24h"]) > 0
                assert float(i["highest_price_24h"]) != float(i["lowest_price_24h"])
                assert float(i["last_price"]) == float(i["highest_price_24h"])
                assert float(i["highest_price_24h"]) > float(i["lowest_price_24h"])
            assert i["base_liquidity_usd"] > 0
            assert float(i["base_liquidity_coins"]) > 0
            assert i["base_trade_value_usd"] > 0
            assert i["rel_liquidity_usd"] > 0
            assert float(i["rel_liquidity_coins"]) > 0
            assert i["rel_trade_value_usd"] > 0
            assert i["base_volume_coins"] > 0
            assert i["rel_volume_coins"] > 0
            assert float(i["base_volume"]) > 0
            assert float(i["quote_volume"]) > 0


def test_get_ticker(setup_cache):
    get = setup_cache.load
    ticker = get.ticker()
    for i in ticker:
        for j in i:
            assert "_" in j
            assert float(i[j]["last_price"]) > 0
            assert float(i[j]["isFrozen"]) == 0
            if j == "MORTY_RICK":
                assert float(i[j]["quote_volume"]) > 0
                assert float(i[j]["base_volume"]) > 0


# /////////////////////// #
# Cache.calc class tests  #
# /////////////////////// #
def test_atomicdex_info(setup_swaps_test_data, setup_cache):
    calc = setup_cache.calc
    r = calc.atomicdexio(1)
    assert r["swaps_all_time"] == 12
    assert r["swaps_24h"] == 7
    assert r["swaps_30d"] == 10
    assert r["current_liquidity"] > 0
    # TODO: This value references the orderbook. Will need to add fixture
    # assert r["current_liquidity"] == 504180.75


def test_ticker(setup_cache):
    calc = setup_cache.calc
    r = calc.ticker()
    assert len(r) > 0


def test_atomicdex_fortnight(setup_cache) -> dict:
    calc = setup_cache.calc
    r = calc.atomicdexio(14, True)
    assert r["days"] == 14
    for i in r:
        if i != "top_pairs":
            assert r[i] > 0
            assert r[i] is not None
            assert isinstance(r[i], (int, float))
        else:
            for j in r[i]:
                assert j in [
                    "by_value_traded_usd",
                    "by_current_liquidity_usd",
                    "by_swaps_count"
                ]
                for k in r[i][j]:
                    assert "_" in k
                    assert r[i][j][k] > 0
                    assert isinstance(r[i][j][k], (int, float))
    r = calc.atomicdexio(7, True)
    assert r["days"] == 7


# /////////////////////// #
# Cache.save class tests  #
# /////////////////////// #
def test_update_gecko(setup_cache):
    updates = setup_cache.save
    assert "result" in updates.gecko_data()


def test_update_summary(setup_cache):
    updates = setup_cache.save
    r = updates.summary()
    assert "result" in r


def test_update_ticker(setup_cache):
    updates = setup_cache.save
    assert "result" in updates.ticker()


def test_update_atomicdexio(setup_cache):
    updates = setup_cache.save
    assert "result" in updates.atomicdexio()


def test_update_atomicdex_fortnight(setup_cache):
    updates = setup_cache.save
    assert "result" in updates.atomicdex_fortnight()


def test_update_coins(setup_cache):
    updates = setup_cache.save
    assert "result" in updates.coins()


def test_update_coins_config(setup_cache):
    updates = setup_cache.save
    assert "result" in updates.coins_config()
