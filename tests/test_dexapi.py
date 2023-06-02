#!/usr/bin/env python3
import sys
sys.path.append("../dexstats_sqlite_py")
import models


def test_orderbook():
    '''
    Test orderbook() method of DexAPI class.
    TODO: Test for more response keys, see if
    any other values can be reliably static
    '''
    api = models.DexAPI()
    pair = models.Pair(("RICK", "MORTY"))
    r = api.orderbook(pair.as_tuple)
    assert "bids" in r
    assert "asks" in r

    pair = models.Pair("RICK_MORTY")
    r = api.orderbook(pair.as_tuple)
    assert "bids" in r
    assert "asks" in r
