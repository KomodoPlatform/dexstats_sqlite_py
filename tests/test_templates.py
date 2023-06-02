#!/usr/bin/env python3
import sys
sys.path.append("../dexstats_sqlite_py")
import models


def test_pair_summary():
    templates = models.Templates()
    pair_summary = templates.pair_summary("BTC", "LTC")
    for i in pair_summary:
        if i == "trading_pair":
            assert pair_summary[i] == "BTC_LTC"
        elif i == "base_currency":
            assert pair_summary[i] == "BTC"
        elif i == "quote_currency":
            assert pair_summary[i] == "LTC"
        else:
            assert pair_summary[i] == 0


def test_volumes_and_prices():
    templates = models.Templates()
    volumes_and_prices = templates.volumes_and_prices("24h")
    for i in volumes_and_prices:
        assert volumes_and_prices[i] == 0
    keys = volumes_and_prices.keys()
    assert "highest_price_24h" in keys
    assert "lowest_price_24h" in keys
    assert "price_change_percent_24h" in keys
