import os
import platform
import requests
from fastapi.testclient import TestClient
from requests.auth import HTTPBasicAuth
from main import app

client = TestClient(app)

def test_summary():
    response = client.get("/api/v1/summary")
    assert response.status_code == 200


def test_usd_volume_24h():
    response = client.get("/api/v1/ticker")
    assert response.status_code == 200


def test_summary_for_ticker():
    response = client.get("/api/v1/atomicdexio")
    assert response.status_code == 200


def test_ticker():
    response = client.get("/api/v1/orderbook/KMD_LTC")
    assert response.status_code == 200


def test_ticker_for_ticker():
    response = client.get("/api/v1/trades/KMD_LTC")
    assert response.status_code == 200


def test_swaps24():
    response = client.get("/api/v1/last_price/KMD_LTC")
    assert response.status_code == 200

