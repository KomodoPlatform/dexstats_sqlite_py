#!/usr/bin/env python3
import pytest
from fixtures import setup_cache, setup_utils, setup_orderbook_data, \
    setup_swaps_test_data, setup_database, setup_time, logger


# /////////////////////// #
# Cache.calc class tests  #
# /////////////////////// #
def test_calc_atomicdex_info(setup_swaps_test_data, setup_cache):
    calc = setup_cache.calc
    r = calc.atomicdexio(1)
    assert r["swaps_all_time"] == 12
    assert r["swaps_24h"] == 7
    assert r["swaps_30d"] == 10
    # TODO: make this a nicer number
    assert r["current_liquidity"] > 0
    r = calc.atomicdexio("foo")
    assert r is None


def test_calc_gecko_data(setup_cache):
    calc = setup_cache.calc
    r = calc.gecko_data()
    assert len(r) > 0


def test_calc_pair_summaries(setup_cache):
    calc = setup_cache.calc
    r = calc.pair_summaries()
    assert len(r) > 0
    r = calc.pair_summaries("foo")
    assert r is None


def test_calc_ticker(setup_cache):
    calc = setup_cache.calc
    r = calc.ticker()
    assert len(r) > 0
    r = calc.ticker("foo")
    assert r is None


def test_calc_atomicdex_fortnight(setup_cache) -> dict:
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
def test_save_gecko(setup_cache):
    save = setup_cache.save
    path = "tests/fixtures/test_save.json"

    with pytest.raises(TypeError):
        data = "foo bar"
        r = save.save(path, data)
        assert r is None

    with pytest.raises(TypeError):
        r = save.save(path, None)
        assert r is None

    with pytest.raises(TypeError):
        r = save.save(None, None)
        assert r is None

    data = {"foo": "bar"}
    with pytest.raises(Exception):
        r = save.save(path, data)
        assert r is None

    data = {"foo": "bar"}
    r = save.save(path, data)
    assert "result" in r
    assert r["result"].startswith("Updated")

    data = {"foo bar": "foo bar"}
    r = save.save(setup_cache.files.coins_config, data)
    assert r["result"].startswith("Validated")

    data = {"foo bar": "foo bar"}
    r = save.save(setup_cache.files.gecko_data, data)
    assert r["result"].startswith("Validated")


def test_save_summary(setup_cache):
    save = setup_cache.save
    r = save.summary()
    assert "result" in r
    r = save.summary("foo")
    assert r is None


def test_save_ticker(setup_cache):
    save = setup_cache.save
    assert "result" in save.ticker()
    r = save.ticker("foo")
    assert r is None


def test_save_atomicdexio(setup_cache):
    save = setup_cache.save
    assert "result" in save.atomicdexio()
    r = save.atomicdexio("foo")
    assert r is None


def test_save_atomicdex_fortnight(setup_cache):
    save = setup_cache.save
    assert "result" in save.atomicdex_fortnight()
    r = save.atomicdex_fortnight("foo")
    assert r is None


def test_save_coins(setup_cache):
    save = setup_cache.save
    assert "result" in save.coins()
    r = save.coins("foo")
    assert r is None


def test_save_coins_config(setup_cache):
    save = setup_cache.save
    assert "result" in save.coins_config()
    r = save.coins_config("foo")
    assert r is None


# /////////////////////// #
# Cache.load class tests  #
# /////////////////////// #
def test_load_atomicdexio(setup_cache):
    load = setup_cache.load
    r = load.atomicdexio()
    for i in r:
        assert r[i] > 0
        assert r[i] is not None
        assert isinstance(r[i], (float, int))

    with pytest.raises(Exception):
        r = load.atomicdexio()
        assert r is None


def test_load_atomicdex_fortnight(setup_cache):
    load = setup_cache.load
    r = load.atomicdex_fortnight()
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


def test_load_coins_config(setup_cache):
    load = setup_cache.load
    data = load.coins_config()
    assert "KMD" in data
    assert "KMD-BEP20" in data
    assert "LTC-segwit" in data
    assert "LTC" in data
    for i in data:
        assert i == data[i]["coin"]
        assert "coingecko_id" in data[i]


def test_load_coins(setup_cache):
    load = setup_cache.load
    assert len(load.coins()) > 0


def test_load_gecko(setup_cache):
    load = setup_cache.load
    gecko = load.gecko_data()
    assert "KMD" in gecko
    assert gecko["KMD"]["usd_market_cap"] == gecko["KMD-BEP20"]["usd_market_cap"]
    assert gecko["KMD"]["usd_price"] == gecko["KMD-BEP20"]["usd_price"]
    assert gecko["KMD"]["coingecko_id"] == gecko["KMD-BEP20"]["coingecko_id"]
    for i in gecko["KMD"]:
        assert i in ["usd_market_cap", "usd_price", "coingecko_id"]
    for i in gecko:
        assert gecko[i]["coingecko_id"] != ""


def test_load_summary(setup_cache):
    load = setup_cache.load
    data = load.summary()
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


def test_load_summary2(setup_cache):
    load = setup_cache.load
    data = load.summary()
    for i in data:
        if i["trading_pair"] == "KMD_LTC":
            assert i["base_currency"] == "KMD"
            assert i["quote_currency"] == "LTC"
            assert i["base_price_usd"] > 0
            assert i["rel_price_usd"] > 0
            assert i["pair_swaps_count"] > 1
            assert float(i["price_change_percent_24h"]) == 0.0009
            assert float(i["highest_price_24h"]) > 0
            assert float(i["lowest_price_24h"]) > 0
            assert float(i["highest_price_24h"]) != float(i["lowest_price_24h"])
            assert float(i["last_price"]) == float(i["highest_price_24h"])
            assert float(i["highest_price_24h"]) > float(i["lowest_price_24h"])
            assert isinstance(i["base_liquidity_coins"], str)
            assert isinstance(i["rel_liquidity_coins"], str)
            assert isinstance(i["base_volume"], str)
            assert isinstance(i["quote_volume"], str)
            assert i["base_liquidity_usd"] > 0
            assert i["base_trade_value_usd"] > 0
            assert i["rel_liquidity_usd"] > 0
            assert i["rel_trade_value_usd"] > 0


def test_load_ticker(setup_cache):
    load = setup_cache.load
    ticker = load.ticker()
    for i in ticker:
        for j in i:
            assert "_" in j
            assert float(i[j]["last_price"]) > 0
            assert float(i[j]["isFrozen"]) == 0
            if j == "DGB_KMD":
                assert float(i[j]["quote_volume"]) > 0
                assert float(i[j]["base_volume"]) > 0
