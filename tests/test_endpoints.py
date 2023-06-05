#!/usr/bin/env python3
from fastapi.testclient import TestClient
from decimal import Decimal
import pytest
from main import app

client = TestClient(app)


def test_atomicdexio_endpoint():
    r = client.get("/api/v1/atomicdexio")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert len(data) == 4
    assert data["swaps_all_time"] > 0
    assert data["swaps_24h"] > 0
    assert data["swaps_30d"] > 0
    assert data["current_liquidity"] > 0
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_atomicdex_fortnight_endpoint():
    r = client.get("/api/v1/atomicdex_fortnight")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert data["days"] == 14
    assert data["swaps_count"] > 0
    assert data["swaps_value"] > 0
    assert data["current_liquidity"] > 0
    assert len(data["top_pairs"]) == 3
    for i in data["top_pairs"]:
        assert len(data["top_pairs"][i]) == 5
        assert i in ["by_value_traded_usd", "by_current_liquidity_usd", "by_swaps_count"]
        for j in data["top_pairs"][i]:
            assert isinstance(data["top_pairs"][i][j], (float, int))
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_summary_endpoint():
    r = client.get("/api/v1/summary")
    assert r.status_code == 200
    data = r.json()
    for i in data:
        if i["trading_pair"] == "KMD_LTC":
            for j in i:
                if j.endswith("_usd") or j.endswith("_count"):
                    assert i[j] > 0
                    assert isinstance(i[j], (float, int))
                elif j in ["trading_pair", "base_currency", "quote_currency"]:
                    assert isinstance(i[j], (str))
                    assert i["base_currency"] == "KMD"
                    assert i["quote_currency"] == "LTC"
                else:
                    assert isinstance(i[j], (str))
                    assert Decimal(i[j]) != 0
            dp = Decimal("0.00000001")
            base_val = Decimal(i["base_liquidity_coins"]) * Decimal(i["base_price_usd"])
            assert base_val.quantize(dp) == Decimal(i["base_liquidity_usd"]).quantize(dp)
            rel_val = Decimal(i["rel_liquidity_coins"]) * Decimal(i["rel_price_usd"])
            assert rel_val.quantize(dp) == Decimal(i["rel_liquidity_usd"]).quantize(dp)
            val = rel_val + base_val
            assert val.quantize(dp) == Decimal(i["pair_liquidity_usd"]).quantize(dp)
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_ticker_endpoint():
    r = client.get("/api/v1/ticker")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert isinstance(data[0], dict)
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_ticker_v2_endpoint():
    r = client.get("/api/v2/ticker")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, dict)
    assert "KMD_LTC" in data
    assert "isFrozen" in data["KMD_LTC"]
    assert data["KMD_LTC"]["isFrozen"] == "0"
    assert data["KMD_LTC"]["last_price"] != "0"
    assert data["KMD_LTC"]["quote_volume"] != "0"
    assert data["KMD_LTC"]["base_volume"] != "0"
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_orderbook_endpoint():
    r = client.get("/api/v1/orderbook/KMD_LTC")
    assert r.status_code == 200
    data = r.json()
    assert data != {}
    assert "asks" in data
    assert "bids" in data
    assert len(data["asks"]) > 0
    assert len(data["bids"]) > 0
    assert len(data["asks"][0]) == 2
    assert len(data["bids"][0]) == 2
    assert isinstance(data["asks"][0], list)
    assert isinstance(data["bids"][0], list)
    assert isinstance(data["asks"][0][0], str)
    assert isinstance(data["bids"][0][0], str)
    assert isinstance(data["asks"][0][1], str)
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_trades_endpoint():
    r = client.get("/api/v1/trades/KMD_LTC")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert isinstance(data[0], dict)
    assert data[0]["type"] in ["buy", "sell"]
    assert isinstance(data[0]["price"], str)
    assert isinstance(data[0]["trade_id"], str)
    assert isinstance(data[0]["timestamp"], int)
    assert isinstance(data[0]["base_volume"], (float, int))
    assert isinstance(data[0]["quote_volume"], (float, int))
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data


def test_last_price_endpoint():
    r = client.get("/api/v1/last_price/KMD_LTC")
    assert r.status_code == 200
    assert Decimal(r.text) > 0
    with pytest.raises(Exception):
        data = r.json()
        assert "error" in data
