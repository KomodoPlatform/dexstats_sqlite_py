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
def cache_gecko_data():  # pragma: no cover
    try:
        cache.save.gecko_data()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_gecko_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=60)
def cache_summary():  # pragma: no cover
    try:
        cache.save.summary()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_summary_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=60)
def cache_ticker():  # pragma: no cover
    try:
        cache.save.ticker()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_ticker_data]: {e}")


@app.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def cache_atomicdexio():  # pragma: no cover
    try:
        cache.save.atomicdexio()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_atomicdex_io]: {e}")


@app.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def cache_atomicdex_fortnight():  # pragma: no cover
    try:
        cache.save.atomicdex_fortnight()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [cache_atomicdex_io_fortnight]: {e}")


@app.on_event("startup")
@repeat_every(seconds=86400)
def update_coins_config():  # pragma: no cover
    try:
        cache.save.coins_config()
    except Exception as e:
        logger.warning(f"{type(e)} Error in [update_coins_config]: {e}")


@app.on_event("startup")
@repeat_every(seconds=86400)
def update_coins():  # pragma: no cover
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
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/atomicdexio]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/atomicdex_fortnight')
def atomicdex_fortnight():
    '''Extra Summary Statistics over last 2 weeks'''
    try:
        return cache.load.atomicdex_fortnight()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/atomicdex_fortnight]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdex_fortnight]: {e}"}


@app.get('/api/v1/summary')
def summary():
    '''
    Trade summary for the last 24 hours for all
    pairs traded in the last 7 days.
    '''
    try:
        return cache.load.summary()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/summary]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/ticker')
def ticker():
    '''
    Orderbook summary for the last 24 hours
    for all pairs traded in the last 7 days.
    '''
    try:
        return cache.load.ticker()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v2/ticker')
def ticker_v2():
    '''
    Orderbook summary for the last 24 hours
    for all pairs traded in the last 7 days.
    '''
    try:
        data = cache.load.ticker()
        cleaned = {}
        [cleaned.update(i) for i in data]
        return cleaned
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/ticker]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


# ////////////////////////// #
# Routes retrieved from mm2  #
# ////////////////////////// #
@app.get('/api/v1/orderbook/{pair}')
def orderbook(pair: str = "KMD_LTC") -> dict:
    '''
    Live Orderbook for this pair
    Parameters:
        pair: str (e.g. KMD_LTC)
    '''
    try:
        if len(pair) > 32:
            raise HTTPException(
                status_code=400,
                detail="Pair cant be longer than 32 symbols"
            )  # pragma: no cover
        elif "_" not in pair:
            raise HTTPException(
                status_code=400,
                detail="Pair should be in format TICKER1_TICKER2"
            )  # pragma: no cover
        elif pair == "":
            raise HTTPException(
                status_code=400,
                detail="Pair can not be empty. Use the format TICKER1_TICKER2"
            )  # pragma: no cover
        pair = models.Pair(pair)
        orderbook = models.Orderbook(pair)
        return orderbook.for_pair(True)
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/orderbook/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/trades/{pair}')
def trades(pair="KMD_LTC"):
    '''
    Swaps for this pair in the last 24 hours.
    Parameters:
        pair: str (e.g. KMD_LTC)
    '''
    try:
        if len(pair) > 32:
            raise HTTPException(
                status_code=400,
                detail="Pair cant be longer than 32 symbols"
            )  # pragma: no cover
        elif "_" not in pair:
            raise HTTPException(
                status_code=400,
                detail="Pair should be in format TICKER1_TICKER2"
            )  # pragma: no cover
        elif pair == "":
            raise HTTPException(
                status_code=400,
                detail="Pair can not be empty. Use the format TICKER1_TICKER2"
            )  # pragma: no cover
        DB = models.SqliteDB(const.MM2_DB_PATH, dict_format=True)
        pair = models.Pair(pair)
        trades_data = pair.trades(days=1)
        DB.close()
        return trades_data
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/trades/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/last_price/{pair}')
def last_price_for_pair(pair="KMD_LTC"):
    '''Last trade price for a given pair.'''
    try:
        pair = models.Pair(pair)
        DB = models.SqliteDB(const.MM2_DB_PATH, dict_format=True)
        last_price = DB.get_last_price_for_pair(pair.base, pair.quote)
        DB.close()
        return last_price
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/last_price/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


if __name__ == '__main__':  # pragma: no cover
    uvicorn.run("main:app", host=const.API_HOST, port=const.API_PORT)
