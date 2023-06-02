#!/usr/bin/env python3
import sys
import pytest
sys.path.append("../dexstats_sqlite_py")
import models
from test_sqlitedb import setup_swaps_test_data, setup_database


@pytest.fixture
def setup_cache_logic():
    yield models.CacheLogic()


def test_atomicdex_info(setup_swaps_test_data):
    DB = setup_swaps_test_data
    DB.sql_cursor = DB.conn.cursor()
    logic = models.CacheLogic()
    r = logic.atomicdex_info(1, DB)
    assert r["swaps_all_time"] == 9
    assert r["swaps_24h"] == 4
    assert r["swaps_30d"] == 7
    assert r["current_liquidity"] > 0
    # TODO: This value references the orderbook. Will need to add fixture
    # assert r["current_liquidity"] == 504180.75


def test_calc_ticker_cache(setup_cache_logic):
    logic = setup_cache_logic
    r = logic.calc_ticker_cache()
    assert len(r) > 0


def test_atomicdex_timespan_info(setup_cache_logic) -> dict:
    logic = setup_cache_logic
    r = logic.atomicdex_timespan_info()
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
                    assert "_" in k.keys()[0]
                    assert k.values()[0] > 0
                    assert isinstance(k.values()[0], float)
    r = logic.atomicdex_timespan_info(7)
    assert r["days"] == 7
