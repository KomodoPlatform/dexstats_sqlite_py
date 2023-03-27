import uvicorn
import json
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from stats_utils import get_swaps_since_timestamp_by_pair

path_to_db = '/DB/43ec929fe30ee72be42c9162c56dde910a05e50d/MM2.db'
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

available_pairs = stats_utils.get_availiable_pairs(path_to_db)

@app.on_event("startup")
@repeat_every(seconds=60)  # caching data every minute
def cache_gecko_data():
    gecko_data = stats_utils.get_data_from_gecko()
    try:
        with open('gecko_cache.json', 'w') as json_file:
            json_file.write(json.dumps(gecko_data))
    except Exception as e:
        print(e)
    print("saved gecko data to file")


@app.get('/api/v1/summary')
def summary():
    available_pairs = stats_utils.get_availiable_pairs(path_to_db)
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    swaps_24hr_by_pair =  stats_utils.get_swaps_since_timestamp_by_pair(path_to_db, timestamp_24h_ago)
    summary_data = []
    for pair in available_pairs:
        summary_data.append(stats_utils.summary_for_pair(pair, swaps_24hr_by_pair))
    return summary_data


@app.get('/api/v1/ticker')
def ticker():
    available_pairs = stats_utils.get_availiable_pairs(path_to_db)
    ticker_data = []
    for pair in available_pairs:
        ticker_data.append(stats_utils.ticker_for_pair(pair, path_to_db))
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
    trades_data = stats_utils.trades_for_pair(market_pair, path_to_db)
    return trades_data


@app.on_event("startup")
@repeat_every(seconds=600)  # caching data every 10 minutes
def cache_atomicdex_io():
    data = stats_utils.atomicdex_info(path_to_db)
    with open('adex_cache.json', 'w+') as cache_file:
        json.dump(data, cache_file)



@app.get('/api/v1/atomicdexio')
def atomicdex_info_api():
    with open('adex_cache.json', 'r') as json_file:
        adex_cached_data = json.load(json_file)
        return adex_cached_data


if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
