#!/usr/bin/env python3
import sys
import time
import sqlite3
import pytest
from decimal import Decimal
sys.path.append("../dexstats_sqlite_py")
from logger import logger
import models
from test_sqlitedb import setup_swaps_test_data, setup_database


def test_trades_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data

    pair = models.Pair("DGB_KMD", DB=DB)
    r = pair.trades()
    logger.info(r)
    assert len(r) == 1
    assert r[0]["type"] == "buy"
    assert r[0]["price"] == "{:.10f}".format(0.001)

    pair = models.Pair("KMD_DGB", DB=DB)
    r = pair.trades()
    assert len(r) == 1
    assert r[0]["type"] == "sell"
    assert r[0]["price"] == "{:.10f}".format(1000)

    pair = models.Pair("notAticker", DB=DB)
    r = pair.trades()
    assert r == []

    pair = models.Pair("X_Y", DB=DB)
    r = pair.trades()
    assert r == []
    # TODO: Add extra tests once linked to fixtures for test db


def test_ticker_for_pair(setup_swaps_test_data):
    DB = setup_swaps_test_data

    pair = models.Pair("DGB_KMD", DB=DB)
    r = pair.ticker()
    assert r["DGB_KMD"]["isFrozen"] == "0"
    assert r["DGB_KMD"]["last_price"] == "{:.10f}".format(0.001)
    assert r["DGB_KMD"]["base_volume"] == "{:.10f}".format(1000)
    assert r["DGB_KMD"]["quote_volume"] == "{:.10f}".format(1)
