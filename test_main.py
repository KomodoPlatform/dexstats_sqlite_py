import os
from fastapi.testclient import TestClient
from requests.auth import HTTPBasicAuth

from main import app

from dotenv import load_dotenv
load_dotenv()

API_USER = os.getenv("API_USER")
API_PASS = os.getenv("API_PASS")

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


############################################
## Private Endpoints Below, requires auth ##
############################################

def test_24hr_pubkey_stats():
    response = client.get("/api/v1/private/24hr_pubkey_stats")
    assert response.status_code == 401


def test_24hr_coins_stats():
    response = client.get("/api/v1/private/24hr_coins_stats")
    assert response.status_code == 401


def test_24hr_version_stats():
    response = client.get("/api/v1/private/24hr_version_stats")
    assert response.status_code == 401


def test_24hr_gui_stats():
    response = client.get("/api/v1/private/24hr_gui_stats")
    assert response.status_code == 401


def test_24hr_failed_pubkey_stats():
    response = client.get("/api/v1/private/24hr_failed_pubkey_stats")
    assert response.status_code == 401


def test_24hr_failed_coins_stats():
    response = client.get("/api/v1/private/24hr_failed_coins_stats")
    assert response.status_code == 401


def test_24hr_version_stats():
    response = client.get("/api/v1/private/24hr_version_stats")
    assert response.status_code == 401


def test_24hr_failed_gui_stats():
    response = client.get("/api/v1/private/24hr_failed_gui_stats")
    assert response.status_code == 401


def test_authenticated_24hr_pubkey_stats():
    client.auth = HTTPBasicAuth(API_USER, API_PASS)
    response = client.get("/api/v1/private/24hr_pubkey_stats")
    assert response.status_code == 200


def test_authenticated_24hr_coins_stats():
    client.auth = HTTPBasicAuth(API_USER, API_PASS)
    response = client.get("/api/v1/private/24hr_coins_stats")
    assert response.status_code == 200


def test_authenticated_24hr_version_stats():
    client.auth = HTTPBasicAuth(API_USER, API_PASS)
    response = client.get("/api/v1/private/24hr_version_stats")
    assert response.status_code == 200


def test_authenticated_24hr_gui_stats():
    client.auth = HTTPBasicAuth(API_USER, API_PASS)
    response = client.get("/api/v1/private/24hr_gui_stats")
    assert response.status_code == 200


def test_authenticated_24hr_failed_pubkey_stats():
    client.auth = HTTPBasicAuth(API_USER, API_PASS)
    response = client.get("/api/v1/private/24hr_failed_pubkey_stats")
    assert response.status_code == 200


def test_authenticated_24hr_failed_coins_stats():
    client.auth = HTTPBasicAuth(API_USER, API_PASS)
    response = client.get("/api/v1/private/24hr_failed_coins_stats")
    assert response.status_code == 200


def test_authenticated_24hr_failed_version_stats():
    client.auth = HTTPBasicAuth(API_USER, API_PASS)
    response = client.get("/api/v1/private/24hr_failed_version_stats")
    assert response.status_code == 200


def test_authenticated_24hr_failed_gui_stats():
    client.auth = HTTPBasicAuth(API_USER, API_PASS)
    response = client.get("/api/v1/private/24hr_failed_gui_stats")
    assert response.status_code == 200
