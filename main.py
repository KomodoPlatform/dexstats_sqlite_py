#!/usr/bin/env python3
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from logger import logger
import requests
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

DEFI_STATS_URL = "https://test.defi-stats.komodo.earth/api/v3/stats-api"

# /////////////////////////////////////////////////// #
# Routes retrieved from test.defi-stats/komodo.earth  #
# /////////////////////////////////////////////////// #
@app.get('/api/v1/atomicdexio')
def atomicdexio():
    '''
    Simple Summary Statistics used on atomicdex.io website.
    Updates every 10 minutes.
    '''
    try:
        return requests.get(f"{DEFI_STATS_URL}/atomicdexio").json()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/atomicdexio]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/atomicdex_fortnight')
def atomicdex_fortnight():
    '''
    Verbose Summary Statistics for the last 14 days.
    Updates every 10 minutes.
    '''
    try:
        return requests.get(f"{DEFI_STATS_URL}/atomicdex_fortnight").json()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/atomicdex_fortnight]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdex_fortnight]: {e}"}


@app.get('/api/v1/summary')
def summary():
    '''
    Pair summary for the last 24 hours for all
    pairs traded in the last 7 days.
    '''
    try:
        return requests.get(f"{DEFI_STATS_URL}/summary").json()
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
        return requests.get(f"{DEFI_STATS_URL}/ticker").json()
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
        return requests.get(f"{DEFI_STATS_URL}/ticker").json()
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
        return requests.get(f"{DEFI_STATS_URL}/orderbook/{pair}").json()
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
        return requests.get(f"{DEFI_STATS_URL}/trades/{pair}").json()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/trades/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


@app.get('/api/v1/last_price/{pair}')
def last_price_for_pair(pair="KMD_LTC"):
    '''Last trade price for a given pair.'''
    try:
        return requests.get(f"{DEFI_STATS_URL}/last_price/{pair}").json()
    except Exception as e:  # pragma: no cover
        logger.warning(f"{type(e)} Error in [/api/v1/last_price/{pair}]: {e}")
        return {"error": f"{type(e)} Error in [/api/v1/atomicdexio]: {e}"}


if __name__ == '__main__':  # pragma: no cover
    uvicorn.run("main:app", host=const.API_HOST, port=const.API_PORT)
