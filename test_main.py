from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_summary():
    response = client.get("/api/v1/summary")
    assert response.status_code == 200


def test_usd_volume_24h():
    response = client.get("/api/v1/usd_volume_24h")
    assert response.status_code == 200


def test_summary_for_ticker():
    response = client.get("/api/v1/summary_for_ticker/KMD")
    assert response.status_code == 200


def test_ticker():
    response = client.get("/api/v1/ticker")
    assert response.status_code == 200


def test_ticker_for_ticker():
    response = client.get("/api/v1/ticker_for_ticker/KMD")
    assert response.status_code == 200


def test_swaps24():
    response = client.get("/api/v1/swaps24/KMD")
    assert response.status_code == 200


def test_orderbook():
    response = client.get("/api/v1/orderbook/KMD_BTC")
    assert response.status_code == 200


def test_trades():
    response = client.get("/api/v1/trades/KMD_BTC/1")
    assert response.status_code == 200


def test_atomicdexio():
    response = client.get("/api/v1/atomicdexio")
    assert response.status_code == 200


def test_fiat_rates():
    response = client.get("/api/v1/fiat_rates")
    assert response.status_code == 200


def test_volumes_ticker():
    response = client.get("/api/v1/volumes_ticker/KMD/1")
    assert response.status_code == 200


def test_tickers_summary():
    response = client.get("/api/v1/tickers_summary")
    assert response.status_code == 200
