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
import stats_utils
import lib_mm2
from stats_utils import get_availiable_pairs, summary_for_pair, \
    get_data_from_gecko, get_summary_for_ticker, volume_for_ticker_since,\
    swaps24h_for_ticker, get_tickers_summary
from lib_logger import logger
from update_db import mirror_mysql_swaps_db, mirror_mysql_failed_swaps_db, update_json
import lib_json

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
    try:
        data = requests.get("https://raw.githubusercontent.com/KomodoPlatform/coins/master/utils/coins_config.json").json()
        lib_json.write_jsonfile_data('coins_config.json', data)
    except Exception as e:
        logger.info(f"Error in [update_coins_config]: {e}")
        return {"Error": str(e)}


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every 5 min
def update_db_data():
    try:
        time.sleep(10) # To offset from other db queries
        mirror_mysql_swaps_db(1)
        mirror_mysql_failed_swaps_db(1)
        mirror_mysql_failed_swaps_db(1)
        update_json()
    except Exception as e:
        logger.info(f"Error in [update_db_data]: {e}")
        return {"Error": str(e)}


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_swap_counts():
    lib_json.write_jsonfile_data('swap_counts.json', stats_utils.get_atomicdex_info())


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_gecko_data():
    try:
        data = get_data_from_gecko()
        lib_json.write_jsonfile_data('gecko_cache.json', data)
    except Exception as e:
        logger.info(f"Error in [cache_gecko_data]: {e}")
        return {"Error: ": str(e)}


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_swaps_data():
    try:
        data = stats_utils.get_24hr_swaps_data()
        lib_json.write_jsonfile_data('24hr_swaps_cache.json', data)
    except Exception as e:
        logger.info(f"Error in [cache_swaps_data]: {e}")
        return {"Error: ": str(e)}


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_swaps_data_by_pair():
    try:
        data = stats_utils.get_24hr_swaps_data_by_pair()
        lib_json.write_jsonfile_data('24hr_swaps_cache_by_pair.json', data)
    except Exception as e:
        logger.info(f"Error in [cache_swaps_data_by_pair]: {e}")
        return {"Error: ": str(e)}


@app.on_event("startup")
@repeat_every(seconds=30)  # caching data every 30 seconds
def cache_summary_data():
    try:
        gecko_cached_data = lib_json.get_jsonfile_data('gecko_cache.json')
        swaps_cache_24hr_by_pair = lib_json.get_jsonfile_data('24hr_swaps_cache_by_pair.json')
        summary_data = []
        total_usd_volume = 0
        for pair in swaps_cache_24hr_by_pair:
            pair_summary = stats_utils.summary_for_pair(pair)
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
        usd_vol = {"usd_volume_24h": total_usd_volume}
        lib_json.write_jsonfile_data('summary_cache.json', summary_data)
        lib_json.write_jsonfile_data('usd_volume_cache.json', usd_vol)
    except Exception as e:
        logger.info(f"Error in [cache_summary_data]: {e}")
        return {"Error": str(e)}


@app.get('/api/v1/summary')
def summary():
    return lib_json.get_jsonfile_data('summary_cache.json')


@app.get('/api/v1/usd_volume_24h')
def usd_volume_24h():
    return lib_json.get_jsonfile_data('usd_volume_cache.json')


@app.get('/api/v1/summary_for_ticker/{ticker}')
def summary_for_ticker(ticker="KMD"):
    try:
        return stats_utils.get_summary_for_ticker(ticker, seednode_swaps_db)
    except Exception as e:
        return {"Error": str(e)}


@app.get('/api/v1/ticker')
def ticker():
    try:
        tickers = stats_utils.get_availiable_pairs()
        ticker_data = []
        for pair in tickers:
            ticker_data.append(stats_utils.ticker_for_pair(pair, seednode_swaps_db))
        return ticker_data
    except Exception as e:
        logger.info(f"Error in [ticker]: {e}")
        return {"error": str(e)}


@app.get('/api/v1/ticker_for_ticker/{ticker}')
def ticker_for_ticker(ticker="KMD"):
    return stats_utils.get_ticker_for_ticker(ticker, seednode_swaps_db)


@app.get('/api/v1/swaps24/{ticker}')
def swaps24(ticker="KMD"):
    return swaps24h_for_ticker(ticker, seednode_swaps_db, 1)


@app.get('/api/v1/orderbook/{market_pair}')
def orderbook(market_pair="KMD_BTC"):
    orderbook_data = lib_mm2.orderbook_for_pair(market_pair)
    return orderbook_data


@app.get('/api/v1/trades/{pair}/{days_in_past}')
def trades(pair="KMD_BTC", days_in_past=1):
    try:
        trades_data = stats_utils.trades_for_pair(pair, days_in_past)
        return trades_data
    except Exception as e:
        logger.info(f"Error in [trades]: {e}")
        return {"Error": str(e)}


@app.get('/api/v1/atomicdexio')
def atomicdex_info_api():
    return lib_json.get_jsonfile_data('swap_counts.json')


@app.get('/api/v1/fiat_rates')
def fiat_rates():
    return lib_json.get_jsonfile_data('gecko_cache.json')


# TODO: get volumes for x days for ticker
@app.get("/api/v1/volumes_ticker/{ticker_vol}/{days_in_past}")
def volumes_history_ticker(ticker_vol="KMD", days_in_past=1):
    return volume_for_ticker_since(ticker_vol, int(days_in_past))


@app.get('/api/v1/tickers_summary')
def tickers_summary():
    return get_tickers_summary()


@app.get('/api/v1/private/24hr_pubkey_stats')
def get_pubkey_stats_24h(username: str = Depends(authenticate_user)):
    return lib_json.get_jsonfile_data('24hr_pubkey_stats.json')


@app.get('/api/v1/private/24hr_coins_stats')
def get_coin_stats_24h(username: str = Depends(authenticate_user)):
    return lib_json.get_jsonfile_data('24hr_coins_stats.json')


@app.get('/api/v1/private/24hr_version_stats')
def get_version_stats_24h(username: str = Depends(authenticate_user)):
    return lib_json.get_jsonfile_data('24hr_version_stats.json')


@app.get('/api/v1/private/24hr_gui_stats')
def get_gui_stats_24h(username: str = Depends(authenticate_user)):
    return lib_json.get_jsonfile_data('24hr_gui_stats.json')


@app.get('/api/v1/private/24hr_failed_pubkey_stats')
def get_24hr_failed_pubkey_stats(username: str = Depends(authenticate_user)):
    return lib_json.get_jsonfile_data('24hr_failed_pubkey_stats.json')


@app.get('/api/v1/private/24hr_failed_coins_stats')
def get_24hr_failed_coins_stats(username: str = Depends(authenticate_user)):
    return lib_json.get_jsonfile_data('24hr_failed_coins_stats.json')


@app.get('/api/v1/private/24hr_failed_version_stats')
def get_24hr_failed_version_stats(username: str = Depends(authenticate_user)):
    return lib_json.get_jsonfile_data('24hr_failed_version_stats.json')


@app.get('/api/v1/private/24hr_failed_gui_stats')
def get_24hr_failed_gui_stats(username: str = Depends(authenticate_user)):
    return lib_json.get_jsonfile_data('24hr_failed_gui_stats.json')



if __name__ == '__main__':
    if platform.node() == "markets-atomicdex-test-api":
        uvicorn.run("main:app", host="0.0.0.0", port=8080, ssl_keyfile="/etc/letsencrypt/live/stats.testchain.xyz/privkey.pem", ssl_certfile="/etc/letsencrypt/live/stats.testchain.xyz/fullchain.pem")
    else:
        uvicorn.run("main:app", host="0.0.0.0", port=8080)
