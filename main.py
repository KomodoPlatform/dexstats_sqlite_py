import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from stats_utils import get_availiable_pairs, market_summary, trades_for_pair, orderbook_for_pair

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
def get_exchange_info():
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


@app.get('/api/v1/markets')
def get_markets():
     available_pairs = get_availiable_pairs(path_to_db)
     summary_data = []
     for pair in available_pairs:
         summary_data.append(market_summary(pair))
     return summary_data


# TODO: add slicing by trade id
@app.get('/api/v1/trades')
def get_trades(market: str = "KMD_BTC"):
    trades_data = trades_for_pair(market, path_to_db)
    return trades_data


@app.get('/api/v1/orders/snapshot')
def orderbook_snapshot(market: str = "KMD_BTC"):
    orderbook_data = orderbook_for_pair(market)
    return orderbook_data


if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0", port=8080)
