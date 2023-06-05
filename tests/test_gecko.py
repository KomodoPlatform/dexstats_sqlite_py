#!/usr/bin/env python3
import sys
import time
import sqlite3
import pytest
from decimal import Decimal
from fixtures import (
    setup_swaps_test_data,
    setup_database,
    setup_gecko,
    setup_gecko_info,
    setup_gecko_coin_ids,
    logger,
)


def test_load_gecko_data(setup_gecko):
    gecko = setup_gecko
    r = gecko.load_gecko_data()
    assert r["KMD"]["usd_price"] == 1
    assert r["KMD"]["usd_market_cap"] == 777
    assert r["KMD"]["coingecko_id"] == "komodo"


def test_get_gecko_coin_ids_list(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_gecko_coin_ids_list()
    assert len(r) > 0
    assert "komodo" in r


def test_get_gecko_info_dict(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_gecko_info_dict()
    assert len(r["KMD"]) > 0


def test_get_gecko_coins_dict(setup_gecko, setup_gecko_info, setup_gecko_coin_ids):
    gecko = setup_gecko
    coin_ids = setup_gecko_coin_ids
    coins_info = setup_gecko_info
    r = gecko.get_gecko_coins_dict(coins_info, coin_ids)
    assert len(r["komodo"]) == 2


def test_get_gecko_data(setup_gecko):
    gecko = setup_gecko
    r = gecko.get_gecko_data()
    assert round(r["USDC"]["usd_price"]) == 1
