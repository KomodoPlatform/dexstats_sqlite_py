#!/usr/bin/env python3
import sys
from decimal import Decimal
from fixtures import (
    setup_swaps_test_data,
    setup_database,
    setup_time,
    setup_orderbook,
    setup_kmd_ltc_str_pair_with_db,
    logger,
)


def test_get_and_parse(setup_orderbook):
    orderbook = setup_orderbook
    r = orderbook.get_and_parse()
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert "base_max_volume" in r["asks"][0]
    assert "base_max_volume" in r["bids"][0]
    r = orderbook.get_and_parse(endpoint=True)
    assert "asks" in r
    assert "bids" in r
    assert len(r["asks"]) > 0
    assert len(r["bids"]) > 0
    assert len(r["asks"][0]) == 2
    assert len(r["bids"][0]) == 2
    assert isinstance(r["asks"][0], list)
    assert isinstance(r["bids"][0], list)
