#!/usr/bin/env python3
import os
import json
import time
import secrets
import uvicorn
import platform
import requests
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from stats_utils import get_availiable_pairs, summary_for_pair, ticker_for_pair, orderbook_for_pair, trades_for_pair,\
    atomicdex_info, reverse_string_number, get_data_from_gecko, get_summary_for_ticker, get_ticker_for_ticker, volume_for_ticker,\
    swaps24h_for_ticker, get_tickers_summary, get_24hr_swaps_data, get_24hr_swaps_data_by_pair
from lib_logger import logger
from update_db import mirror_mysql_swaps_db, mirror_mysql_failed_swaps_db, update_json

from dotenv import load_dotenv
load_dotenv()

API_USER = os.getenv("API_USER")
API_PASS = os.getenv("API_PASS")

mm2_db = 'MM2.db'
seednode_swaps_db = 'seednode_swaps.db'
seednode_failed_swaps_db = 'seednode_failed_swaps.db'
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()


def authenticate_user(credentials: HTTPBasicCredentials = Depends(security)):
    current_username_bytes = credentials.username.encode("utf8")
    correct_username_bytes = API_USER.encode("utf8")
    is_correct_username = secrets.compare_digest(
        current_username_bytes, correct_username_bytes
    )
    current_password_bytes = credentials.password.encode("utf8")
    correct_password_bytes = API_PASS.encode("utf8")
    is_correct_password = secrets.compare_digest(
        current_password_bytes, correct_password_bytes
    )
    if not (is_correct_username and is_correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


@app.on_event("startup")
@repeat_every(seconds=86400)  # caching data every day
def update_coins_config():
    data = requests.get("https://raw.githubusercontent.com/KomodoPlatform/coins/master/utils/coins_config.json").json()
    with open('coins_config.json', 'w+') as json_file:
        json.dump(data, json_file)
    logger.info("Updated coins_config.json!")


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every 5 min
def update_db_data():
    time.sleep(10) # To offset from other db queries
    try:
        mirror_mysql_swaps_db(1)
        mirror_mysql_failed_swaps_db(1)
        mirror_mysql_failed_swaps_db(1)
        update_json()
    except Exception as e:
        logger.warning(e)


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_swap_counts():
    data = atomicdex_info(seednode_swaps_db)
    with open('swap_counts.json', 'w+') as json_file:
        json.dump(data, json_file)
    logger.info("Updated swap_counts.json!")


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_gecko_data():
    gecko_data = get_data_from_gecko()
    with open('gecko_cache.json', 'w+') as json_file:
        json.dump(gecko_data, json_file)
    logger.info("Updated gecko_cache.json!")


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_swaps_data():
    swaps_data_24hrs = get_24hr_swaps_data(seednode_swaps_db)
    with open('24hr_swaps_cache.json', 'w+') as json_file:
        json.dump(swaps_data_24hrs, json_file)
    logger.info("Updated 24hr_swaps_cache.json!")


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_swaps_data_by_pair():
    swaps_data_24hrs_by_pair = get_24hr_swaps_data_by_pair(seednode_swaps_db)
    with open('24hr_swaps_cache_by_pair.json', 'w+') as json_file:
        json.dump(swaps_data_24hrs_by_pair, json_file)
    logger.info("Updated 24hr_swaps_cache_by_pair.json!")


@app.on_event("startup")
@repeat_every(seconds=30)  # caching data every 30 seconds
def cache_summary_data():
    available_pairs_summary = get_availiable_pairs(seednode_swaps_db)

    summary_data = []
    with open('gecko_cache.json', 'r') as json_file:
        gecko_cached_data = json.load(json_file)
    total_usd_volume = 0
    for pair in available_pairs_summary:

        pair_summary = summary_for_pair(pair)
        summary_data.append(pair_summary)
        try:
            base_currency_usd_vol = float(gecko_cached_data[pair_summary["base_currency"]]["usd_price"]) \
                                    * float(pair_summary["base_volume"])
        except KeyError:
            base_currency_usd_vol = 0
        try:
            quote_currency_usd_vol = float(gecko_cached_data[pair_summary["quote_currency"]]["usd_price"]) \
                                     * float(pair_summary["quote_volume"])
        except KeyError:
            quote_currency_usd_vol = 0
        # a bit hacky way but its semantically not correct to add both, so we adding most valuable one volume
        if base_currency_usd_vol > quote_currency_usd_vol:
            total_usd_volume += base_currency_usd_vol
        else:
            total_usd_volume += quote_currency_usd_vol
    with open('summary_cache.json', 'w+') as json_file:
        json.dump(summary_data, json_file)
        logger.info("Updated summary_cache.json!")

    usd_vol = {"usd_volume_24h": total_usd_volume}
    with open('usd_volume_cache.json', 'w+') as json_file:
        json.dump(usd_vol, json_file)
        logger.info("Updated usd_volume_cache.json!")


@app.get('/api/v1/summary')
def summary():
    with open('summary_cache.json', 'r') as json_file:
        summary_cache_data = json.load(json_file)
        return summary_cache_data


@app.get('/api/v1/usd_volume_24h')
def usd_volume_24h():
    with open('usd_volume_cache.json', 'r') as json_file:
        usd_volume_cache = json.load(json_file)
        return usd_volume_cache


@app.get('/api/v1/summary_for_ticker/{ticker_summary}')
def summary_for_ticker(ticker_summary="KMD"):
    return get_summary_for_ticker(ticker_summary, seednode_swaps_db)


@app.get('/api/v1/ticker')
def ticker():
    available_pairs_ticker = get_availiable_pairs(seednode_swaps_db)
    ticker_data = []
    for pair in available_pairs_ticker:
        ticker_data.append(ticker_for_pair(pair, seednode_swaps_db, 1))
    return ticker_data


@app.get('/api/v1/ticker_for_ticker/{ticker_ticker}')
def ticker_for_ticker(ticker_ticker="KMD"):
    return get_ticker_for_ticker(ticker_ticker, seednode_swaps_db, 1)


@app.get('/api/v1/swaps24/{ticker}')
def swaps24(ticker="KMD"):
    return swaps24h_for_ticker(ticker, seednode_swaps_db, 1)


@app.get('/api/v1/orderbook/{market_pair}')
def orderbook(market_pair="KMD_BTC"):
    orderbook_data = orderbook_for_pair(market_pair)
    return orderbook_data


@app.get('/api/v1/trades/{market_pair}/{days_in_past}')
def trades(market_pair="KMD_BTC", days_in_past=1):
    trades_data = trades_for_pair(market_pair, seednode_swaps_db, int(days_in_past))
    return trades_data


@app.get('/api/v1/atomicdexio')
def atomicdex_info_api():
    with open('swap_counts.json', 'r') as json_file:
        swap_counts = json.load(json_file)
        return swap_counts


@app.get('/api/v1/fiat_rates')
def fiat_rates():
    with open('gecko_cache.json', 'r') as json_file:
        gecko_cached_data = json.load(json_file)
    return gecko_cached_data


# TODO: get volumes for x days for ticker
@app.get("/api/v1/volumes_ticker/{ticker_vol}/{days_in_past}")
def volumes_history_ticker(ticker_vol="KMD", days_in_past=1):
    return volume_for_ticker(ticker_vol, seednode_swaps_db, int(days_in_past))


@app.get('/api/v1/tickers_summary')
def tickers_summary():
    return get_tickers_summary(seednode_swaps_db)


@app.get('/api/v1/private/24hr_pubkey_stats')
def get_pubkey_stats_24h(username: str = Depends(authenticate_user)):
    with open('24hr_pubkey_stats.json', 'r') as json_file:
        pubkey_stats_24h = json.load(json_file)
    return pubkey_stats_24h


@app.get('/api/v1/private/24hr_coins_stats')
def get_coin_stats_24h(username: str = Depends(authenticate_user)):
    with open('24hr_coins_stats.json', 'r') as json_file:
        data = json.load(json_file)
    return data


@app.get('/api/v1/private/24hr_version_stats')
def get_version_stats_24h(username: str = Depends(authenticate_user)):
    with open('24hr_version_stats.json', 'r') as json_file:
        data = json.load(json_file)
    return data


@app.get('/api/v1/private/24hr_gui_stats')
def get_gui_stats_24h(username: str = Depends(authenticate_user)):
    with open('24hr_gui_stats.json', 'r') as json_file:
        data = json.load(json_file)
    return data


@app.get('/api/v1/private/24hr_failed_pubkey_stats')
def get_24hr_failed_pubkey_stats(username: str = Depends(authenticate_user)):
    with open('24hr_failed_pubkey_stats.json', 'r') as json_file:
        data = json.load(json_file)
    return data


@app.get('/api/v1/private/24hr_failed_coins_stats')
def get_24hr_failed_coins_stats(username: str = Depends(authenticate_user)):
    with open('24hr_failed_coins_stats.json', 'r') as json_file:
        data = json.load(json_file)
    return data


@app.get('/api/v1/private/24hr_failed_version_stats')
def get_24hr_failed_version_stats(username: str = Depends(authenticate_user)):
    with open('24hr_failed_version_stats.json', 'r') as json_file:
        data = json.load(json_file)
    return data


@app.get('/api/v1/private/24hr_failed_gui_stats')
def get_24hr_failed_gui_stats(username: str = Depends(authenticate_user)):
    with open('24hr_failed_gui_stats.json', 'r') as json_file:
        data = json.load(json_file)
    return data



if __name__ == '__main__':
    if platform.node() == "markets-atomicdex-test-api":
        uvicorn.run("main:app", host="0.0.0.0", port=8080, ssl_keyfile="/etc/letsencrypt/live/stats.testchain.xyz/privkey.pem", ssl_certfile="/etc/letsencrypt/live/stats.testchain.xyz/fullchain.pem")
    else:
        uvicorn.run("main:app", host="0.0.0.0", port=8080)
