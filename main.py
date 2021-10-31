import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from stats_utils import get_availiable_pairs, summary_for_pair, ticker_for_pair, orderbook_for_pair, trades_for_pair, atomicdex_info

path_to_db = 'MM2.db'
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/api/v1/info')
def info():
    exchange_info = {
        "name": "AtomicDEX",
        "description": "The decentralized exchange on AtomicDEX uses P2P order books powered by atomic swaps that enable the transfer of cryptocurrency from one party to another, without the use of a third-party intermediary.",
        "location": "DEX",
        "logo": "",
        "website": "https://atomicdex.io/",
        "twitter": "https://twitter.com/atomicdex",
        "version": "2.0",
        "capability": {
            "markets": True,
            "trades": True,
            "ordersSnapshot": True,
            "candles": True,
            "ticker": True
            }
    }
    return exchange_info

# available_pairs = get_availiable_pairs(path_to_db)
#
# @app.get('/api/v1/summary')
# def summary():
#     available_pairs = get_availiable_pairs(path_to_db)
#     summary_data = []
#     for pair in available_pairs:
#         summary_data.append(summary_for_pair(pair, path_to_db))
#     return summary_data
#
#
# @app.get('/api/v1/ticker')
# def ticker():
#     available_pairs = get_availiable_pairs(path_to_db)
#     ticker_data = []
#     for pair in available_pairs:
#         ticker_data.append(ticker_for_pair(pair, path_to_db))
#     return ticker_data
#
#
# @app.get('/api/v1/orderbook/{market_pair}')
# def orderbook(market_pair="KMD_BTC"):
#     orderbook_data = orderbook_for_pair(market_pair)
#     return orderbook_data
#
#
# @app.get('/api/v1/trades/{market_pair}')
# def trades(market_pair="KMD_BTC"):
#     trades_data = trades_for_pair(market_pair, path_to_db)
#     return trades_data
#
#
# @app.get('/api/v1/atomicdexio')
# def atomicdex_info_api():
#     data = atomicdex_info(path_to_db)
#     return data

if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
