#!/usr/bin/env python3
import sys
import pytest
sys.path.append("../dexstats_sqlite_py")
import models


@pytest.fixture
def setup_cache_get():
    yield models.CacheGet()


def test_get_adex(setup_cache_get):
    get = setup_cache_get
    r = get.adex()
    for i in r:
        assert r[i] > 0
        assert r[i] is not None
        assert isinstance(r[i], (float, int))


def test_get_adex_fortnight(setup_cache_get):
    get = setup_cache_get
    r = get.adex_fortnight()
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
                    assert "_" in list(k.keys())[0]
                    assert list(k.values())[0] > 0
                    assert isinstance(list(k.values())[0], (float, int))


def test_get_coins_config(setup_cache_get):
    get = setup_cache_get
    coins_config = get.coins_config()
    assert "KMD" in coins_config
    assert "KMD-BEP20" in coins_config
    assert "LTC-segwit" in coins_config
    assert "LTC" in coins_config
    for i in coins_config:
        assert i == coins_config[i]["coin"]
        assert "coingecko_id" in coins_config[i]


def test_get_coins(setup_cache_get):
    get = setup_cache_get
    assert len(get.coins()) > 0


def test_get_gecko(setup_cache_get):
    get = setup_cache_get
    gecko = get.gecko()
    assert "KMD" in gecko
    assert gecko["KMD"]["usd_market_cap"] == gecko["KMD-BEP20"]["usd_market_cap"]
    assert gecko["KMD"]["usd_price"] == gecko["KMD-BEP20"]["usd_price"]
    assert gecko["KMD"]["coingecko_id"] == gecko["KMD-BEP20"]["coingecko_id"]
    for i in gecko["KMD"]:
        assert i in ["usd_market_cap", "usd_price", "coingecko_id"]
    for i in gecko:
        assert gecko[i]["coingecko_id"] != ""


def test_get_summary(setup_cache_get):
    get = setup_cache_get
    summary = get.summary()
    for i in summary:
        if i["trading_pair"] == "RICK_MORTY":
            assert i["base_currency"] == "RICK"
            assert i["quote_currency"] == "MORTY"
            assert i["pair_swaps_count"] > 0
            assert i["base_price_usd"] == 0
            assert i["rel_price_usd"] == 0
            assert i["price_change_percent_24h"] == 0
            assert i["base_liquidity_usd"] == 0
            assert i["base_trade_value_usd"] == 0
            assert i["rel_liquidity_usd"] == 0
            assert i["rel_trade_value_usd"] == 0
        if i["trading_pair"] == "KMD_LTC":
            assert i["base_currency"] == "KMD"
            assert i["quote_currency"] == "LTC"
            assert i["base_price_usd"] > 0
            assert i["rel_price_usd"] > 0
            if i["pair_swaps_count"] > 1:
                assert float(i["price_change_percent_24h"]) > 0
                assert float(i["highest_price_24h"]) > 0
                assert float(i["lowest_price_24h"]) > 0
                assert float(i["last_price"]) == float(i["highest_price_24h"])
            if i["pair_swaps_count"] > 2:
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


def test_get_ticker(setup_cache_get):
    get = setup_cache_get
    ticker = get.ticker()
    for i in ticker:
        for j in i:
            assert "_" in j
            assert float(i[j]["last_price"]) > 0
            assert float(i[j]["isFrozen"]) == 0
            if j == "MORTY_RICK":
                assert float(i[j]["quote_volume"]) > 0
                assert float(i[j]["base_volume"]) > 0
