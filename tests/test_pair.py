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
