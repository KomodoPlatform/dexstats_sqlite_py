#!/usr/bin/env python3
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from logger import logger
import models
import const

cache = models.Cache()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
@repeat_every(seconds=60)
def cache_gecko_data():
    try:
        cache.save.gecko_data()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=60)
def cache_summary():
    try:
        cache.save.summary()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_summary_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=60)
def cache_ticker():
    try:
        cache.save.ticker()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_ticker_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def cache_atomicdexio():
    try:
        cache.save.atomicdexio()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_atomicdex_io]: {e}")


@app.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def cache_atomicdex_fortnight():
    try:
        cache.save.atomicdex_fortnight()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_atomicdex_io_fortnight]: {e}")


@app.on_event("startup")
@repeat_every(seconds=86400)
def update_coins_config():
    try:
        cache.save.coins_config()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_coins_config]: {e}")


@app.on_event("startup")
@repeat_every(seconds=86400)
def update_coins():
    try:
        cache.save.coins()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_coins]: {e}")


# //////////////////////////// #
# Routes retrieved from cache  #
# //////////////////////////// #
@app.get('/api/v1/atomicdexio')
def atomicdexio():
    '''Simple Summary Statistics for last 24 hours'''
    try:
        return cache.load.atomicdexio()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [/api/v1/atomicdexio]: {e}")
        return {}


@app.get('/api/v1/atomicdex_fortnight')
def atomicdex_fortnight():
    '''Extra Summary Statistics over last 2 weeks'''
    try:
        return cache.load.atomicdex_fortnight()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [/api/v1/atomicdex_fortnight]: {e}")
        return {}


@app.get('/api/v1/summary')
def summary():
    '''
    Trade summary for the last 24 hours for all
    pairs traded in the last 7 days.
    '''
    try:
        return cache.load.summary()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [/api/v1/summary]: {e}")
        return {}


@app.get('/api/v1/ticker')
def ticker():
    '''
    Orderbook summary for the last 24 hours
    for all pairs traded in the last 7 days.
    '''
    try:
        return cache.load.ticker()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [/api/v1/ticker]: {e}")
        return {}


# ////////////////////////// #
# Routes retrieved from mm2  #
# ////////////////////////// #
@app.get('/api/v1/orderbook/{pair}')
def orderbook(pair="KMD_LTC"):
    '''Live Orderbook for this pair'''
    try:
        if len(pair) > 32:
            raise HTTPException(
                status_code=400,
                detail="Pair cant be longer than 32 symbols"
            )
        mm2 = models.DexAPI()
        return mm2.orderbook(pair)
    except Exception as e:
        logger.warning(f"{type(e)} Error in [/api/v1/orderbook/{pair}]: {e}")
        return {}


@app.get('/api/v1/trades/{pair}')
def trades(pair="KMD_LTC"):
    '''Swaps for this pair in the last 24 hours'''
    try:
        if len(pair) > 32:
            raise HTTPException(
                status_code=400,
                detail="Pair cant be longer than 32 symbols"
            )
        DB = models.SqliteDB(const.MM2_DB_PATH, dict_format=True)
        pair = models.Pair(pair)
        trades_data = pair.trades(days=1)
        DB.close()
        return trades_data
    except Exception as e:
        logger.warning(f"{type(e)} Error in [/api/v1/trades/{pair}]: {e}")
        return {}


@app.get('/api/v1/last_price/{pair}')
def last_price_for_pair(pair="KMD_LTC"):
    '''Last trade price for a given pair.'''
    try:
        DB = models.SqliteDB(const.MM2_DB_PATH, dict_format=True)
        last_price = DB.get_last_price_for_pair(pair)
        DB.close()
        return last_price
    except Exception as e:
        logger.warning(f"{type(e)} Error in [/api/v1/last_price/{pair}]: {e}")
        return 0


if __name__ == '__main__':
    uvicorn.run("main:app", host=const.API_HOST, port=const.API_PORT)
