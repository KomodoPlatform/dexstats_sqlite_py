#!/usr/bin/env python3
import sys
from fixtures import setup_dexapi, setup_rick_morty_tuple_pair, setup_rick_morty_str_pair, logger


def test_orderbook(setup_dexapi, setup_rick_morty_tuple_pair, setup_rick_morty_str_pair):
    '''
    Test orderbook() method of DexAPI class.
    TODO: Test for more response keys, see if
    any other values can be reliably static
    '''
    api = setup_dexapi
    pair = setup_rick_morty_tuple_pair
    r = api.orderbook(pair.as_tuple)
    assert "bids" in r
    assert "asks" in r

    pair = setup_rick_morty_str_pair
    r = api.orderbook(pair.as_tuple)
    assert "bids" in r
    assert "asks" in r
