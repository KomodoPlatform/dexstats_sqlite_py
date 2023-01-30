import sqlite3
import requests
import json
import os
import platform
from decimal import Decimal
from datetime import datetime, timedelta
from collections import OrderedDict
from lib_logger import logger
import lib_ts_db
import lib_mm2
import lib_json
import lib_sqlite_db

from dotenv import load_dotenv
load_dotenv()

#LOCALHOST = True
LOCALHOST = False


# Data for atomicdex.io website
def get_atomicdex_info():
    return {
        "swaps_24h" : len(lib_sqlite_db.get_swaps_failed_since(1)),
        "swaps_30d" : len(lib_sqlite_db.get_swaps_failed_since(30)),
        "swaps_all_time" : len(lib_sqlite_db.get_all_swaps())
    }


# getting list of pairs with amount of swaps > 0 from db (list of tuples)
# string -> list (of base, rel tuples)
def get_availiable_pairs():
    conn, cursor = lib_ts_db.get_timescaledb(LOCALHOST)
    pairs = lib_ts_db.get_pairs(cursor, 1)
    pairs.sort()
    return pairs

# list (with swaps statuses) -> dict
# iterating over the list of swaps and counting data for CMC summary call
# last_price, base_volume, quote_volume, highest_price_24h, lowest_price_24h, price_change_percent_24h
def count_volumes_and_prices(swap_statuses):
    pair_volumes_and_prices = {}
    base_volume = 0
    quote_volume = 0
    swap_prices = {}
    for swap_status in swap_statuses:
        if swap_status["trade_type"] == "buy":
            base_volume += swap_status["maker_amount"]
            quote_volume += swap_status["taker_amount"]
            swap_price = Decimal(swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"])
            swap_prices.update({swap_status["epoch"]: swap_price})
        if swap_status["trade_type"] == "sell":
            base_volume += swap_status["taker_amount"]
            quote_volume += swap_status["maker_amount"]
            swap_price = Decimal(swap_status["maker_amount"]) / Decimal(swap_status["taker_amount"])
            swap_prices.update({swap_status["epoch"]: swap_price})


    pair_volumes_and_prices["base_volume"] = base_volume
    pair_volumes_and_prices["quote_volume"] = quote_volume

    try:
        pair_volumes_and_prices["highest_price_24h"] = max(swap_prices.values())
    except ValueError:
        pair_volumes_and_prices["highest_price_24h"] = 0
    try:
        pair_volumes_and_prices["lowest_price_24h"] = min(swap_prices.values())
    except ValueError:
        pair_volumes_and_prices["lowest_price_24h"] = 0
    try:
        pair_volumes_and_prices["last_price"] = swap_prices[max(swap_prices.keys())]
    except ValueError:
        pair_volumes_and_prices["last_price"] = 0
    try:
        pair_volumes_and_prices["price_change_percent_24h"] = ( swap_prices[max(swap_prices.keys())] - swap_prices[min(swap_prices.keys())] ) / Decimal(100)
    except ValueError:
        pair_volumes_and_prices["price_change_percent_24h"] = 0
    logger.info(f"{pair}: {pair_volumes_and_prices}")
    return pair_volumes_and_prices


# SUMMARY Endpoint
# tuple, string -> dictionary
# Receiving tuple with base and rel as an argument and producing CMC summary endpoint data, requires mm2 rpc password and sql db connection
def summary_for_pair(pair):
    pair_summary = OrderedDict()
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    swaps_for_pair_24h = get_swaps_for_pair_24h(pair)
    pair_24h_volumes_and_prices = count_volumes_and_prices(swaps_for_pair_24h)
    pair_summary["trading_pair"] = pair[0] + "_" + pair[1]
    pair_summary["last_price"] = "{:.10f}".format(pair_24h_volumes_and_prices["last_price"])
    try:
        orderbook = lib_mm2.get_mm2_orderbook_for_pair(pair)
        pair_summary["lowest_ask"] = "{:.10f}".format(Decimal(lib_mm2.find_lowest_ask(orderbook)))
        pair_summary["highest_bid"] = "{:.10f}".format(Decimal(lib_mm2.find_highest_bid(orderbook)))
    except Exception as e:
        # This should throw an alert via discord/mattermost/telegram
        pair_summary["lowest_ask"] = "{:.10f}".format(Decimal(0))
        pair_summary["highest_bid"] = "{:.10f}".format(Decimal(0))
        logger.warning(f"Couldn't get orderbook! Is mm2 running? {e}")

    pair_summary["base_currency"] = pair[0]
    pair_summary["base_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["base_volume"])
    pair_summary["quote_currency"] = pair[1]
    pair_summary["quote_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["quote_volume"])
    pair_summary["price_change_percent_24h"] = "{:.10f}".format(pair_24h_volumes_and_prices["price_change_percent_24h"])
    pair_summary["highest_price_24h"] = "{:.10f}".format(pair_24h_volumes_and_prices["highest_price_24h"])
    pair_summary["lowest_price_24h"] = "{:.10f}".format(pair_24h_volumes_and_prices["lowest_price_24h"])
    pair_summary["trades_24h"] = len(swaps_for_pair_24h)
    last_swap_timestamp = 0
    for swap in swaps_for_pair_24h:
        if swap["epoch"] > last_swap_timestamp:
            last_swap_timestamp = swap["epoch"]
    pair_summary["last_swap_timestamp"] = last_swap_timestamp
    return pair_summary


# TICKER Endpoint
def ticker_for_pair(pair, path_to_db, days_in_past=1):
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_cursor = conn.cursor()
    pair_ticker = OrderedDict()
    timestamp_24h_ago = int((datetime.now() - timedelta(days_in_past)).strftime("%s"))
    swaps_for_pair_24h = get_swaps_for_pair_24h(pair)
    pair_24h_volumes_and_prices = count_volumes_and_prices(swaps_for_pair_24h)
    pair_ticker[pair[0] + "_" + pair[1]] = OrderedDict()
    pair_ticker[pair[0] + "_" + pair[1]]["last_price"] = "{:.10f}".format(pair_24h_volumes_and_prices["last_price"])
    pair_ticker[pair[0] + "_" + pair[1]]["quote_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["quote_volume"])
    pair_ticker[pair[0] + "_" + pair[1]]["base_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["base_volume"])
    pair_ticker[pair[0] + "_" + pair[1]]["isFrozen"] = "0"
    conn.close()
    return pair_ticker


# Trades Endpoint
def trades_for_pair(pair, path_to_db, days_in_past):
    pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return {"error": "not valid pair"}
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_cursor = conn.cursor()
    timestamp_since = int((datetime.now() - timedelta(days_in_past)).strftime("%s"))
    swaps_for_pair_since_timestamp = lib_sqlite.get_swaps_since_timestamp_for_pair(sql_cursor, pair, timestamp_since)
    trades_info = []
    for swap_status in swaps_for_pair_since_timestamp:
        trade_info = OrderedDict()
        trade_info["trade_id"] = swap_status["uuid"]
        trade_info["price"] = "{:.10f}".format(Decimal(swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"]))
        trade_info["base_volume"] = swap_status["maker_amount"]
        trade_info["quote_volume"] = swap_status["taker_amount"]
        trade_info["timestamp"] = swap_status["epoch"]
        trade_info["type"] = swap_status["trade_type"]
        trades_info.append(trade_info)
    conn.close()
    return trades_info




def reverse_string_number(string_number):
    if Decimal(string_number) != 0:
        return "{:.10f}".format(1 / Decimal(string_number))
    else:
        return string_number


def get_data_from_gecko():
    coin_ids = []
    gecko_prices = {}
    with open("coins_config.json", "r") as f:
        coins_config = json.load(f)

    for coin in coins_config:
        gecko_prices.update({coin:{"usd_price": 0}})
        coin_id = coins_config[coin]["coingecko_id"]
        if coin_id not in ["na", "test-coin", ""]:
            coin_ids.append(coin_id)
    coin_ids = ','.join(list(set(coin_ids)))
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_ids}&vs_currencies=usd"
        r = requests.get(url)
    except Exception as e:
        logger.error(url)
        logger.error(e)
        return {"error": "https://api.coingecko.com/api/v3/simple/price?ids= is not available"}
    gecko_data = r.json()

    try:
        for coin in coins_config:
            coin_id = coins_config[coin]["coingecko_id"]
            if coin_id in gecko_data:
                if "usd" in gecko_data[coin_id]:
                    gecko_prices[coin]["usd_price"] = gecko_data[coin_id]["usd"]
                else:
                    logger.warning(f"{coin} coingecko id ({coin_id}) returns no price. Is it valid?")
    except Exception as e:
        logger.error(e)
        pass
    return gecko_prices


def get_summary_for_ticker(ticker_summary, path_to_db):
    available_pairs_summary_ticker = get_availiable_pairs()
    summary_data = []
    for pair in available_pairs_summary_ticker:
        if ticker_summary in pair:
            summary_data.append(summary_for_pair(pair))
    summary_data_modified = []
    for summary_sample in summary_data:
        # filtering empty data
        if Decimal(summary_sample["base_volume"]) != 0 and Decimal(summary_sample["quote_volume"]) != 0:
            if summary_sample["base_currency"] == ticker_summary:
                summary_data_modified.append(summary_sample)
            else:
                summary_sample_modified = {
                    "trading_pair": summary_sample["quote_currency"] + "_" + summary_sample["base_currency"],
                    "last_price": reverse_string_number(summary_sample["last_price"]),
                    "lowest_ask": reverse_string_number(summary_sample["lowest_ask"]),
                    "highest_bid": reverse_string_number(summary_sample["highest_bid"]),
                    "base_currency": summary_sample["quote_currency"],
                    "base_volume": summary_sample["quote_volume"],
                    "quote_currency": summary_sample["base_currency"],
                    "quote_volume": summary_sample["base_volume"],
                    "price_change_percent_24h": summary_sample["price_change_percent_24h"],
                    "highest_price_24h": reverse_string_number(summary_sample["lowest_price_24h"]),
                    "lowest_price_24h": reverse_string_number(summary_sample["highest_price_24h"])
                }
                summary_data_modified.append(summary_sample_modified)
    return summary_data_modified


def get_ticker_for_ticker(ticker_ticker, path_to_db, days_in_past=1):
    available_pairs_ticker = get_availiable_pairs()
    ticker_data = []
    for pair in available_pairs_ticker:
        if ticker_ticker in pair:
            ticker_data.append(ticker_for_pair(pair, path_to_db, days_in_past))
    ticker_data_unified = []
    for data_sample in ticker_data:
        # not adding zero volumes data
        first_key = list(data_sample.keys())[0]
        if Decimal(data_sample[first_key]["last_price"]) != 0:
            base_ticker = first_key.split("_")[0]
            rel_ticker = first_key.split("_")[1]
            data_sample_unified = {}
            if base_ticker != ticker_ticker:
                last_price_reversed = reverse_string_number(data_sample[first_key]["last_price"])
                data_sample_unified[ticker_ticker + "_" + base_ticker] = {
                    "last_price": last_price_reversed,
                    "quote_volume": data_sample[first_key]["base_volume"],
                    "base_volume": data_sample[first_key]["quote_volume"],
                    "isFrozen": "0"
                }
                ticker_data_unified.append(data_sample_unified)
            else:
                ticker_data_unified.append(data_sample)
    return ticker_data_unified


def swaps24h_for_ticker(ticker, path_to_db, days_in_past=1):
    available_pairs_ticker = get_availiable_pairs()
    ticker_data = []
    for pair in available_pairs_ticker:
        if ticker in pair:
            ticker_data.append(ticker_for_pair(pair, path_to_db, days_in_past))
    return {"swaps_amount_24h": len(ticker_data)}


def volume_for_ticker(ticker, path_to_db, days_in_past):
    volumes_dict = {}
    previous_volume = 0
    for i in range(0, days_in_past):
        overall_volume = 0
        ticker_data = get_ticker_for_ticker(ticker, path_to_db, i+1)
        d = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')
        volumes_dict[d] = 0
        for pair in ticker_data:
            overall_volume += Decimal(pair[list(pair.keys())[0]]["base_volume"])
        volumes_dict[d] = overall_volume - previous_volume
        previous_volume = overall_volume
    return volumes_dict

def get_swaps_for_pair_24h(pair):
    data_a_b = []
    data_b_a = []
    pair = pair.split("/")
    try:
        swaps_cache_24hr_by_pair = lib_json.get_swaps_cache_24hr_by_pair()
        if f"{pair[0]}/{pair[1]}" in swaps_cache_24hr_by_pair:
            data_a_b = swaps_cache_24hr_by_pair[f"{pair[0]}/{pair[1]}"]
            for swap in data_a_b:
                swap["trade_type"] = "buy"
        if f"{pair[1]}/{pair[0]}" in swaps_cache_24hr_by_pair:
            data_b_a = swaps_cache_24hr_by_pair[f"{pair[1]}/{pair[0]}"]
            for swap in data_b_a:
                swap["trade_type"] = "sell"
    except Exception as e:
        logger.warning(f"Error in [get_swaps_for_pair_24h]: {e}")

    return data_b_a + data_a_b

def get_tickers_summary():

    available_pairs = get_availiable_pairs()

    tickers_summary = {}
    for pair in available_pairs:
        for ticker in pair:
            tickers_summary[ticker] = {"volume_24h": 0, "trades_24h": 0}

    for pair in available_pairs:

        swaps_for_pair_24h = get_swaps_for_pair_24h(pair)
        for swap in swaps_for_pair_24h:
            if swap["trade_type"] == "buy":
                tickers_summary[swap["maker_coin"]]["volume_24h"] += swap["maker_amount"]
                tickers_summary[swap["maker_coin"]]["trades_24h"] += 1
                tickers_summary[swap["taker_coin"]]["volume_24h"] += swap["taker_amount"]
                tickers_summary[swap["taker_coin"]]["trades_24h"] += 1
            if swap["trade_type"] == "sell":
                tickers_summary[swap["maker_coin"]]["volume_24h"] += swap["maker_amount"]
                tickers_summary[swap["maker_coin"]]["trades_24h"] += 1
                tickers_summary[swap["taker_coin"]]["volume_24h"] += swap["taker_amount"]
                tickers_summary[swap["taker_coin"]]["trades_24h"] += 1

    for summary in list(tickers_summary):
        if tickers_summary[summary] == {"volume_24h": 0, "trades_24h": 0}:
            tickers_summary.pop(summary)
    return tickers_summary

def get_24hr_swaps_data():
    conn, cursor = lib_ts_db.get_timescaledb(LOCALHOST)
    return lib_ts_db.get_swaps_data(conn, cursor, 1)

# Returns last 24hrs swap data by maker/taker pair
def get_24hr_swaps_data_by_pair():
    pair_data = {}
    try:
        data = get_24hr_swaps_data()

        for i in data:
            maker = i["maker_coin"]
            taker = i["taker_coin"]
            pair = f"{maker}/{taker}"
            if pair not in pair_data:
                pair_data.update({pair:[]})
            pair_data[pair].append(i)
    except Exception as e:
        logger.warning(f"Error in [get_24hr_swaps_data_by_pair] {e}")
    return pair_data


