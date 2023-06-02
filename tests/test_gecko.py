#!/usr/bin/env python3
import sys
import time
import sqlite3
import pytest
from decimal import Decimal
sys.path.append("../dexstats_sqlite_py")
from test_sqlitedb import setup_swaps_test_data, setup_database
import models


def test_get_gecko_usd_price(setup_swaps_test_data):
    DB = setup_swaps_test_data
    gecko = models.Gecko()
    assert gecko.get_gecko_usd_price("KMD", DB.gecko_data) == 1
    assert gecko.get_gecko_usd_price("BTC", DB.gecko_data) == 1000000
    price1 = gecko.get_gecko_usd_price("LTC-segwit", DB.gecko_data)
    price2 = gecko.get_gecko_usd_price("LTC", DB.gecko_data)
    assert price1 == price2
