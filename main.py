import uvicorn
import json
import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
import stats_utils
from lib_logger import logger

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
    logger.info(f"Getting gecko_data")
    try:
        gecko_data = stats_utils.get_data_from_gecko()
    except Exception as e:
        #logger.info(f"Error in [cache_gecko_data]: {e}")
        pass
    if "error" not in gecko_data:
        with open('gecko_cache.json', 'w+') as f:
            json.dump(gecko_data, f)
        logger.info("Updated gecko_cache.json")


@app.on_event("startup")
@repeat_every(seconds=30)  # caching data every minute
def cache_summary_data():
    try:
        logger.info(f"Getting summary_data")
        available_pairs = stats_utils.get_availiable_pairs(MM2_DB_PATH)
        summary_data = []
        for pair in available_pairs:
            summary_data.append(stats_utils.summary_for_pair(pair, MM2_DB_PATH))
        with open('summary_cache.json', 'w+') as f:
            json.dump(summary_data, f)
    except Exception as e:
        logger.info(f"Error in [cache_summary_data]: {e}")
    logger.info("Updated summary_cache.json")


@app.get('/api/v1/summary')
def summary():
    with open('summary_cache.json', 'r') as json_file:
        summary_cache_data = json.load(json_file)
        return summary_cache_data


@app.get('/api/v1/ticker')
def ticker():
    available_pairs = stats_utils.get_availiable_pairs(MM2_DB_PATH)
    ticker_data = []
    for pair in available_pairs:
        ticker_data.append(stats_utils.ticker_for_pair(pair, MM2_DB_PATH))
    return ticker_data


@app.get('/api/v1/orderbook/{market_pair}')
def orderbook(market_pair="KMD_BTC"):
    if len(market_pair) > 32:
        raise HTTPException(status_code=400, detail="Pair cant be longer than 32 symbols")
    orderbook_data = stats_utils.orderbook_for_pair(market_pair)
    return orderbook_data


@app.get('/api/v1/trades/{market_pair}')
def trades(market_pair="KMD_BTC"):
    if len(market_pair) > 32:
        raise HTTPException(status_code=400, detail="Pair cant be longer than 32 symbols")
    trades_data = stats_utils.trades_for_pair(market_pair, MM2_DB_PATH)
    return trades_data


@app.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def cache_atomicdex_io():
    data = stats_utils.atomicdex_info(MM2_DB_PATH)
    with open('adex_cache.json', 'w+') as cache_file:
        json.dump(data, cache_file)


@app.get('/api/v1/atomicdexio')
def atomicdex_info_api():
    with open('adex_cache.json', 'r') as json_file:
        adex_cached_data = json.load(json_file)
        return adex_cached_data


if __name__ == '__main__':
    uvicorn.run("main:app", host=API_HOST, port=API_PORT)
