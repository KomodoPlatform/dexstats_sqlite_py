#!/usr/bin/env python3
import uvicorn
import json
import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
import stats_utils
from logger import logger
from cache_loops import CacheLoops
import sqlite_db

loops = CacheLoops()

load_dotenv()
API_HOST = os.getenv('API_HOST')
API_PORT = int(os.getenv('API_PORT'))
MM2_DB_PATH = os.getenv('MM2_DB_PATH')
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_gecko_data():
    try:
        loops.refresh_gecko_cache()
    except Exception as e:
        logger.warning(f"Error in [cache_gecko_data]: {e}")



@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_summary_data():
    try:
        loops.refresh_summary_cache()
    except Exception as e:
        logger.warning(f"Error in [cache_summary_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_ticker_data():
    try:
        loops.refresh_ticker_cache()
    except Exception as e:
        logger.warning(f"Error in [cache_ticker_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def cache_atomicdex_io():
    try:
        data = stats_utils.atomicdex_info(MM2_DB_PATH)
        with open('adex_cache.json', 'w+') as cache_file:
            json.dump(data, cache_file)
            logger.info("Updated adex_cache.json")
    except Exception as e:
        logger.warning(f"Error in [cache_atomicdex_io]: {e}")


@app.get('/api/v1/summary')
def summary():
    '''Trade summary for the last 24 hours for all pairs traded in the last 7 days.'''
    try:
        with open('summary_cache.json', 'r') as json_file:
            summary_cached_data = json.load(json_file)
            return summary_cached_data
    except:
        return {}


@app.get('/api/v1/ticker')
def ticker():
    '''Orderbook summary for the last 24 hours for all pairs traded in the last 7 days.'''
    try:
        with open('ticker_cache.json', 'r') as json_file:
            ticker_cached_data = json.load(json_file)
            return ticker_cached_data
    except:
        return {}


@app.get('/api/v1/atomicdexio')
def atomicdex_info_api():
    '''Simple Summary Statistics'''
    with open('adex_cache.json', 'r') as json_file:
        adex_cached_data = json.load(json_file)
        return adex_cached_data


@app.get('/api/v1/orderbook/{market_pair}')
def orderbook(market_pair="KMD_LTC"):
    '''Live Orderbook for this pair'''
    if len(market_pair) > 32:
        raise HTTPException(status_code=400, detail="Pair cant be longer than 32 symbols")
    orderbook_data = stats_utils.orderbook_for_pair(market_pair, endpoint=True)
    return orderbook_data


@app.get('/api/v1/trades/{market_pair}')
def trades(market_pair="KMD_LTC"):
    '''Swaps for this pair in the last 24 hours'''
    if len(market_pair) > 32:
        raise HTTPException(status_code=400, detail="Pair cant be longer than 32 symbols")
    trades_data = stats_utils.trades_for_pair(market_pair, MM2_DB_PATH)
    return trades_data


@app.get('/api/v1/last_price/{pair}')
def last_price_for_pair(pair="KMD_LTC"):
    '''Last trade price for a given pair.'''
    DB = sqlite_db.sqliteDB(MM2_DB_PATH, dict_format=True)
    last_price = DB.get_last_price_for_pair(pair)
    DB.close()
    return last_price


if __name__ == '__main__':
    uvicorn.run("main:app", host=API_HOST, port=API_PORT)
