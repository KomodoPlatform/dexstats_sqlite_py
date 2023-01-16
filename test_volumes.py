import uvicorn
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
from stats_utils import get_availiable_pairs, summary_for_pair, ticker_for_pair, orderbook_for_pair, trades_for_pair,\
    atomicdex_info, reverse_string_number, get_data_from_gecko, summary_for_ticker, ticker_for_ticker, volume_for_ticker,\
    swaps24h_for_ticker, summary_ticker
from decimal import Decimal

path_to_db = 'MM2.db'

available_pairs_summary = get_availiable_pairs(path_to_db)
summary_data = []
with open('gecko_cache.json', 'r') as json_file:
    gecko_cached_data = json.load(json_file)

total_usd_volume = 0
for pair in available_pairs_summary:
    pair_summary = summary_for_pair(pair, path_to_db)
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
    usd_volume = {"usd_volume_24h": total_usd_volume}
with open('usd_volume_cache.json', 'w+') as json_file:
    json.dump(usd_volume, json_file)
