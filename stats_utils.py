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
import lib_helper
import lib_ts_db
import lib_sqlite_db

from dotenv import load_dotenv
load_dotenv()

#LOCALHOST = True
LOCALHOST = False


# Data for atomicdex.io website
def get_atomicdex_info():
    try:
        conn, cursor = lib_ts_db.get_timescaledb(LOCALHOST)
        return {
            "swaps_24h" : len(lib_ts_db.get_swaps_since(cursor, lib_helper.days_ago(1))),
            "swaps_30d" : len(lib_ts_db.get_swaps_since(cursor, lib_helper.days_ago(30))),
            "swaps_all_time" : len(lib_ts_db.get_all_swaps(cursor))
        }
    except Exception as e:
        return {"Error": str(e)}


# getting list of pairs with amount of swaps > 0 from db (list of tuples)
# string -> list (of base, rel tuples)
def get_availiable_pairs(days=1):
    conn, cursor = lib_ts_db.get_timescaledb(LOCALHOST)
    pairs = lib_ts_db.get_pairs_since(cursor, lib_helper.days_ago(days))
    pairs.sort()
    return pairs


# list (with swaps statuses) -> dict
# iterating over the list of swaps and counting data for CMC summary call
# last_price, base_volume, quote_volume, highest_price_24h, lowest_price_24h, price_change_percent_24h
def count_volumes_and_prices(swap_statuses):
    try:
        pair_volumes_and_prices = {}
        base_volume = 0
        quote_volume = 0
        swap_prices = {}
        for swap_status in swap_statuses:
            if swap_status["trade_type"] == "buy":
                logger.info(f'[buy] adding {swap_status["taker_amount"]} {swap_status["taker_coin"]} to quote vol for {swap_status["epoch"]}')
                base_volume += swap_status["maker_amount"]
                quote_volume += swap_status["taker_amount"]

                swap_price = Decimal(swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"])

            if swap_status["trade_type"] == "sell":
                logger.info(f'[sell] adding {swap_status["maker_amount"]} {swap_status["maker_coin"]} to quote vol for {swap_status["epoch"]}')
                base_volume += swap_status["taker_amount"]
                quote_volume += swap_status["maker_amount"]

                swap_price = Decimal(swap_status["maker_amount"]) / Decimal(swap_status["taker_amount"])

            swap_prices.update({swap_status["epoch"]: swap_price})

        pair_volumes_and_prices["base_volume"] = base_volume
        pair_volumes_and_prices["quote_volume"] = quote_volume
    except Exception as e:
        logger.warning(f"Error: {e}")
        return {}

    pair_volumes_and_prices = get_pair_volumes_and_prices_stats(pair_volumes_and_prices, swap_prices)
    return pair_volumes_and_prices


def get_pair_volumes_and_prices_stats(pair_volumes_and_prices, swap_prices):
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
    return pair_volumes_and_prices


# SUMMARY Endpoint
# tuple, string -> dictionary
# Receiving tuple with base and rel as an argument and producing CMC summary endpoint data, requires mm2 rpc password and sql db connection
def summary_for_pair(pair):
    pair_summary = OrderedDict()
    pair_tickers = lib_helper.get_pair_tickers(pair)
    conn, cursor = lib_ts_db.get_timescaledb(LOCALHOST)
    cache = lib_json.get_24hr_swaps_by_pair_cache()
    swaps_for_pair_24h = set_pair_trade_type(pair, cache)
    pair_24h_volumes_and_prices = count_volumes_and_prices(swaps_for_pair_24h)
    pair_summary["trading_pair"] = pair_tickers[0] + "_" + pair_tickers[1]
    pair_summary["last_price"] = "{:.10f}".format(pair_24h_volumes_and_prices["last_price"])
    try:
        pair_orderbook = lib_mm2.get_mm2_orderbook_for_pair(pair_tickers)
        pair_summary["lowest_ask"] = "{:.10f}".format(Decimal(lib_mm2.find_lowest_ask_for_pair(pair_orderbook)))
        pair_summary["highest_bid"] = "{:.10f}".format(Decimal(lib_mm2.find_highest_bid_for_pair(pair_orderbook)))
    except Exception as e:
        # This should throw an alert via discord/mattermost/telegram
        pair_summary["lowest_ask"] = "{:.10f}".format(Decimal(0))
        pair_summary["highest_bid"] = "{:.10f}".format(Decimal(0))
        logger.warning(f"Couldn't get {pair} orderbook! Is mm2 running? {e}")

    pair_summary["base_currency"] = pair_tickers[0]
    pair_summary["base_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["base_volume"])
    pair_summary["quote_currency"] = pair_tickers[1]
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
def ticker_for_pair(pair, path_to_db):
    try:
        data = OrderedDict()
        conn = sqlite3.connect(path_to_db)
        conn.row_factory = sqlite3.Row
        with conn:
            sql_cursor = conn.cursor()
            pair_tickers = pair.split("/")
            cache = lib_json.get_24hr_swaps_by_pair_cache()
            swaps_for_pair_24h = set_pair_trade_type(pair, cache)
            pair_24h_volumes_and_prices = count_volumes_and_prices(swaps_for_pair_24h)
            market_pair = pair_tickers[0] + "_" + pair_tickers[1]
            data[market_pair] = OrderedDict()
            data[market_pair]["last_price"] = "{:.10f}".format(pair_24h_volumes_and_prices["last_price"])
            data[market_pair]["quote_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["quote_volume"])
            data[market_pair]["base_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["base_volume"])
            data[market_pair]["isFrozen"] = "0"
        return data
    except Exception as e:
        logger.warning(f"Error in [ticker_for_pair]: {e}")
        return {"Error": str(e)}


# Trades Endpoint
def trades_for_pair(pair, days_in_past):
    try:
        conn, cursor = lib_ts_db.get_timescaledb(LOCALHOST)
        if not lib_helper.validate_pair(pair):
            return {"error": "not valid pair"}
        pair_tickers = lib_helper.get_pair_tickers(pair)
        cache = lib_json.get_24hr_swaps_by_pair_cache()
        swaps_for_pair = set_pair_trade_type(pair, cache)
        trades_info = []
        for swap_status in swaps_for_pair:
            trade_info = OrderedDict()
            trade_info.update({
                "trade_id": swap_status["uuid"],
                "price": "{:.10f}".format(Decimal(swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"])),
                "base_volume": swap_status["maker_amount"],
                "quote_volume": swap_status["taker_amount"],
                "timestamp": swap_status["epoch"],
                "type": swap_status["trade_type"]
            })
            trades_info.append(trade_info)
        return trades_info
    except Exception as e:
        logger.warning(f"Error in [trades_for_pair]: {e}")
        return {"Error": str(e)}


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


def get_summary_for_ticker(ticker, path_to_db):
    try:
        pairs = get_availiable_pairs()
        summary_data = []
        for pair in pairs:
            pair_tickers = pair.split("/")
            if ticker in pair_tickers:
                summary_data.append(summary_for_pair(pair))
        for i in summary_data:
            # filtering empty data
            if Decimal(i["base_volume"]) != 0 and Decimal(i["quote_volume"]) != 0:
                if i["base_currency"] != ticker:
                    i.update({
                        "trading_pair": i["quote_currency"] + "_" + i["base_currency"],
                        "last_price": lib_helper.reverse_string_number(i["last_price"]),
                        "lowest_ask": lib_helper.reverse_string_number(i["lowest_ask"]),
                        "highest_bid": lib_helper.reverse_string_number(i["highest_bid"]),
                        "base_currency": i["quote_currency"],
                        "base_volume": i["quote_volume"],
                        "quote_currency": i["base_currency"],
                        "quote_volume": i["base_volume"],
                        "price_change_percent_24h": i["price_change_percent_24h"],
                        "highest_price_24h": lib_helper.reverse_string_number(i["lowest_price_24h"]),
                        "lowest_price_24h": lib_helper.reverse_string_number(i["highest_price_24h"])
                    })
        return summary_data
    except Exception as e:
        logger.info(e)
        return {"err": str(e)}


def get_ticker_for_ticker(ticker, path_to_db):
    try:
        tickers = get_availiable_pairs()
        ticker_data = []
        for pair in tickers:
            pair_tickers = pair.split("/")
            if ticker in pair_tickers:
                ticker_data.append(ticker_for_pair(pair, path_to_db))
        ticker_data_unified = []
        for data_sample in ticker_data:
            # not adding zero volumes data
            first_key = list(data_sample.keys())[0]
            if Decimal(data_sample[first_key]["last_price"]) != 0:
                base_ticker = first_key.split("_")[0]
                rel_ticker = first_key.split("_")[1]
                data_sample_unified = {}
                if base_ticker != ticker:
                    last_price_reversed = lib_helper.reverse_string_number(data_sample[first_key]["last_price"])
                    data_sample_unified[ticker + "_" + base_ticker] = {
                        "last_price": last_price_reversed,
                        "quote_volume": data_sample[first_key]["base_volume"],
                        "base_volume": data_sample[first_key]["quote_volume"],
                        "isFrozen": "0"
                    }
                    ticker_data_unified.append(data_sample_unified)
                else:
                    ticker_data_unified.append(data_sample)
        return ticker_data_unified
    except Exception as e:
        logger.warning(f"Error: {e}")
        return {"Error": str(e)}


def swaps24h_for_ticker(ticker, path_to_db, days_in_past=1):
    available_pairs_ticker = get_availiable_pairs()
    ticker_data = []
    for pair in available_pairs_ticker:
        if ticker in pair:
            ticker_data.append(ticker_for_pair(pair, path_to_db))
    return {"swaps_amount_24h": len(ticker_data)}


def volume_for_ticker_since(ticker, days_in_past):
    conn, cursor = lib_ts_db.get_timescaledb(LOCALHOST)
    volumes_dict = {}
    previous_volume = 0
    for i in range(0, days_in_past):
        overall_volume = 0
        ticker_data = get_ticker_for_ticker_since(ticker, i+1)
        d = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')
        volumes_dict[d] = 0
        for pair in ticker_data:
            logger.info(pair)
            overall_volume += Decimal(pair[list(pair.keys())[0]]["base_volume"])
        volumes_dict[d] = overall_volume - previous_volume
        previous_volume = overall_volume
    return volumes_dict


def get_ticker_for_ticker_since(ticker, days_in_past):
    try:
        tickers = get_availiable_pairs()
        ticker_data = []
        checked_pairs = []
        for pair in tickers:
            pair_tickers = pair.split("/")
            if ticker in pair_tickers and pair_tickers not in checked_pairs:
                checked_pairs.append(pair_tickers)
                checked_pairs.append(pair_tickers[::-1])
                ticker_data.append(ticker_for_pair_since(pair, days_in_past))
        # logger.info(ticker_data)
        ticker_data_unified = []
        for data_sample in ticker_data:
            # not adding zero volumes data
            first_key = list(data_sample.keys())[0]
            if Decimal(data_sample[first_key]["last_price"]) != 0:
                base_ticker = first_key.split("_")[0]
                rel_ticker = first_key.split("_")[1]
                data_sample_unified = {}
                if base_ticker != ticker:
                    last_price_reversed = lib_helper.reverse_string_number(data_sample[first_key]["last_price"])
                    data_sample_unified[ticker + "_" + base_ticker] = {
                        "last_price": last_price_reversed,
                        "quote_volume": data_sample[first_key]["base_volume"],
                        "base_volume": data_sample[first_key]["quote_volume"],
                        "isFrozen": "0"
                    }
                    ticker_data_unified.append(data_sample_unified)
                else:
                    ticker_data_unified.append(data_sample)
        return ticker_data_unified
    except Exception as e:
        logger.warning(f"Error in [get_ticker_for_ticker_since]: {e}")
        return {"Error": str(e)}

# TICKER Endpoint
def ticker_for_pair_since(pair, days):
    try:
        conn, cursor = lib_ts_db.get_timescaledb(LOCALHOST)
        data = OrderedDict()
        pair_tickers = lib_helper.get_pair_tickers(pair)
        epoch = lib_helper.days_ago(days)

        swaps_data = lib_ts_db.get_bidirectional_swaps_json_by_pair_since(cursor, epoch, pair)
        swaps_by_pair = get_swaps_by_pair(swaps_data)
        swaps_by_pair = set_pair_trade_type(pair, swaps_by_pair)
        pair_volumes_and_prices = count_volumes_and_prices(swaps_by_pair)
        logger.info(f"pair_volumes_and_prices: {pair_volumes_and_prices}")
        market_pair = pair_tickers[0] + "_" + pair_tickers[1]
        data[market_pair] = OrderedDict()
        data[market_pair]["last_price"] = "{:.10f}".format(pair_volumes_and_prices["last_price"])
        data[market_pair]["quote_volume"] = "{:.10f}".format(pair_volumes_and_prices["quote_volume"])
        data[market_pair]["base_volume"] = "{:.10f}".format(pair_volumes_and_prices["base_volume"])
        data[market_pair]["isFrozen"] = "0"
        logger.info(data)
        return data
    except Exception as e:
        logger.warning(f"Error in [ticker_for_pair_since]: {e}")
        return {"Error": str(e)}

def get_swaps_by_pair(swaps):
    data = {}
    for i in swaps:
        pair = f"{i['maker_coin']}/{i['taker_coin']}"
        if pair not in data:
            data.update({pair: [i]})
        else:
            data[pair].append(i)
    return data

def set_pair_trade_type(pair, data):
    data_a_b = []
    data_b_a = []
    maker_coin, taker_coin = lib_helper.get_pair_tickers(pair)
    try:
        if f"{pair}" in data:
            data_a_b = data[f"{pair}"]
            for swap in data_a_b:
                swap["trade_type"] = "buy"
        if f"{taker_coin}/{maker_coin}" in data:
            data_b_a = data[f"{taker_coin}/{maker_coin}"]
            for swap in data_b_a:
                swap["trade_type"] = "sell"
    except Exception as e:
        logger.warning(f"Error in [set_pair_trade_type]: {e}")

    return data_b_a + data_a_b

def get_tickers_summary():
    available_pairs = get_availiable_pairs()
    cache = lib_json.get_24hr_swaps_by_pair_cache()

    tickers_summary = {}
    for pair in available_pairs:
        for ticker in pair:
            tickers_summary[ticker] = {"volume_24h": 0, "trades_24h": 0}

    for pair in available_pairs:

        swaps_for_pair = set_pair_trade_type(pair, cache)
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
    return lib_ts_db.get_swaps_json_since(cursor, lib_helper.days_ago(1))

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


