#!/usr/bin/env python3
import sys
import time
import sqlite3
import pytest
from decimal import Decimal
from fixtures import (
    setup_database,
    setup_swaps_test_data,
    setup_time,
    setup_kmd_dgb_tuple_pair_with_db,
    setup_dgb_kmd_str_pair_with_db,
    setup_not_a_real_pair,
    setup_not_existing_pair,
    setup_kmd_ltc_str_pair_with_db,
    logger,
)


def test_trades_for_pair(
    setup_dgb_kmd_str_pair_with_db,
    setup_kmd_dgb_tuple_pair_with_db,
    setup_not_a_real_pair,
    setup_not_existing_pair,
):
    pair = setup_dgb_kmd_str_pair_with_db
    r = pair.trades()
    logger.info(r)
    assert len(r) == 1
    assert r[0]["type"] == "buy"
    assert r[0]["base_volume"] == 1000
    assert r[0]["quote_volume"] == 1
    assert r[0]["price"] == "{:.10f}".format(0.001)

    pair = setup_kmd_dgb_tuple_pair_with_db
    r = pair.trades()
    assert len(r) == 1
    assert r[0]["type"] == "sell"
    assert r[0]["base_volume"] == 1
    assert r[0]["quote_volume"] == 1000
    assert r[0]["price"] == "{:.10f}".format(1000)

    pair = setup_not_a_real_pair
    r = pair.trades()
    assert r == []

    pair = setup_not_existing_pair
    r = pair.trades()
    assert r == []
    # TODO: Add extra tests once linked to fixtures for test db


def test_ticker_for_pair(setup_dgb_kmd_str_pair_with_db):
    pair = setup_dgb_kmd_str_pair_with_db
    r = pair.ticker()
    assert r["DGB_KMD"]["isFrozen"] == "0"
    assert r["DGB_KMD"]["last_price"] == "{:.10f}".format(0.001)
    assert r["DGB_KMD"]["base_volume"] == "{:.10f}".format(1000)
    assert r["DGB_KMD"]["quote_volume"] == "{:.10f}".format(1)


def test_get_volumes_and_prices(setup_kmd_ltc_str_pair_with_db,
                                setup_not_existing_pair):
    pair = setup_kmd_ltc_str_pair_with_db
    r = pair.get_volumes_and_prices()
    assert r["base_volume"] == 110
    assert float(r["highest_price_24h"]) == 0.1
    assert float(r["last_price"]) == 0.1
    assert float(r["lowest_price_24h"]) == 0.01
    assert float(r["price_change_percent_24h"]) == 0.0009
    assert float(r["price_change_24h"]) == 0.09

    pair = setup_not_existing_pair
    r = pair.get_volumes_and_prices()
    assert float(r["last_price"]) == 0
