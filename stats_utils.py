#!/usr/bin/env python3
import os
import sqlite3
import requests
import json
import logging
from decimal import Decimal
from datetime import datetime, timedelta
from collections import OrderedDict
from dotenv import load_dotenv
from logger import logger
import sqlite_db

load_dotenv()
MM2_DB_PATH = os.getenv('MM2_DB_PATH')


def get_suffix(days: int) -> str:
    if days == 1:
        return "24h"
    else:
        return f"{days}d"


# list (with swaps statuses) -> dict
# iterating over the list of swaps and counting data for CMC summary call
# last_price, base_volume, quote_volume, highest_price_24h, lowest_price_24h, price_change_percent_24h
def count_volumes_and_prices(swap_statuses, pair, path_to_db, days=1):
    try:
        suffix = get_suffix(days)
        pair_volumes_and_prices = {}
        base_volume = 0
        quote_volume = 0
        swap_prices = {}
        for swap_status in swap_statuses:
            base_volume += swap_status["maker_amount"]
            quote_volume += swap_status["taker_amount"]
            swap_price = Decimal(
                swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"])
            swap_prices[swap_status["started_at"]] = swap_price

        pair_volumes_and_prices["base_volume"] = base_volume
        pair_volumes_and_prices["quote_volume"] = quote_volume
    except Exception as e:
        logger.info(f"Error in [count_volumes_and_prices]: {e}")
    try:
        pair_volumes_and_prices[f"highest_price_{suffix}"] = max(
            swap_prices.values())
    except ValueError:
        pair_volumes_and_prices[f"highest_price_{suffix}"] = 0
    try:
        pair_volumes_and_prices[f"lowest_price_{suffix}"] = min(swap_prices.values())
    except ValueError:
        pair_volumes_and_prices[f"lowest_price_{suffix}"] = 0
    try:
        pair_volumes_and_prices["last_price"] = swap_prices[max(
            swap_prices.keys())]
    except ValueError:
        DB = sqlite_db.sqliteDB(MM2_DB_PATH, dict_format=True)
        pair_volumes_and_prices["last_price"] = DB.get_last_price_for_pair(
            pair)
        DB.close()

    try:
        pair_volumes_and_prices[f"price_change_percent_{suffix}"] = (swap_prices[max(
            swap_prices.keys())] - swap_prices[min(swap_prices.keys())]) / Decimal(100)
    except ValueError:
        pair_volumes_and_prices[f"price_change_percent_{suffix}"] = 0

    return pair_volumes_and_prices


# tuple, string, string -> list
# returning orderbook for given trading pair
def get_mm2_orderbook_for_pair(pair):
    try:
        mm2_host = "http://127.0.0.1:7783"
        params = {
            'method': 'orderbook',
            'base': pair[0],
            'rel': pair[1]
        }
        r = requests.post(mm2_host, json=params)
        return json.loads(r.text)
    except Exception as e:
        logger.info(f"Error in [get_mm2_orderbook_for_pair]: {e}")


# list -> string
# returning lowest ask from provided orderbook

def find_lowest_ask(orderbook):
    lowest_ask = {"price": "0"}
    try:
        for ask in orderbook["asks"]:
            if lowest_ask["price"] == "0":
                lowest_ask = ask
            elif Decimal(ask["price"]) < Decimal(lowest_ask["price"]):
                lowest_ask = ask
    except KeyError:
        return 0
    return lowest_ask["price"]


# list -> string
# returning highest bid from provided orderbook
def find_highest_bid(orderbook):
    highest_bid = {"price": "0"}
    try:
        for bid in orderbook["bids"]:
            if Decimal(bid["price"]) > Decimal(highest_bid["price"]):
                highest_bid = bid
    except KeyError:
        return 0
    return highest_bid["price"]


def get_related_coins(coin):
    script_path = os.path.realpath(os.path.dirname(__file__))
    coin = coin.split("-")[0]
    with open(f"{script_path}/mm2/coins", "r") as f:
        coins = json.load(f)
    return [i["coin"] for i in coins if i["coin"] == coin or i["coin"].startswith(f"{coin}-")]


def get_and_parse_orderbook(pair, endpoint=False):
    try:
        coin_a = pair[0]
        coin_b = pair[1]
        coins_a = get_related_coins(coin_a)
        coins_b = get_related_coins(coin_b)
        orderbooks_list = []
        for coin_a in coins_a:
            for coin_b in coins_b:
                if coin_a != coin_b:
                    alt_pair = (coin_a, coin_b)
                    orderbook = get_mm2_orderbook_for_pair(alt_pair)
                    orderbooks_list.append(orderbook)
        orderbook = {"pair": f"{pair[0]}_{pair[1]}", "bids": [], "asks": [
        ], "total_asks_base_vol": 0, "total_bids_rel_vol": 0}
        for i in orderbooks_list:
            # case when there is no such ticker in coins file
            if next(iter(i)) == "error":
                i = {"bids": [], "asks": [],
                     "total_asks_base_vol": 0, "total_bids_rel_vol": 0}
            orderbook["bids"] += i["bids"]
            orderbook["asks"] += i["asks"]
            orderbook["total_asks_base_vol"] += Decimal(i["total_asks_base_vol"])
            orderbook["total_bids_rel_vol"] += Decimal(i["total_bids_rel_vol"])
        orderbook["total_asks_base_vol"] = str(orderbook["total_asks_base_vol"])
        orderbook["total_bids_rel_vol"] = str(orderbook["total_bids_rel_vol"])
        bids_converted_list = []
        asks_converted_list = []
    except Exception as e:
        logger.warning(f"Error in [get_and_parse_orderbook]: {e}")
        return {"bids": [], "asks": [], "total_asks_base_vol": 0, "total_bids_rel_vol": 0}

    try:
        for bid in orderbook["bids"]:
            if endpoint:
                converted_bid = []
                converted_bid.append(bid["price"])
                converted_bid.append(bid["base_max_volume"])
                bids_converted_list.append(converted_bid)
            else:
                converted_bid = {
                    "price": bid["price"],
                    "base_max_volume": bid["base_max_volume"]
                }
            bids_converted_list.append(converted_bid)
    except KeyError as e:
        logger.warning(f"Error in [get_and_parse_orderbook]: {e}")
        pass

    try:
        for ask in orderbook["asks"]:
            if endpoint:
                converted_ask = []
                converted_ask.append(ask["price"])
                converted_ask.append(ask["base_max_volume"])
                asks_converted_list.append(converted_ask)
            else:
                converted_ask = {
                    "price": ask["price"],
                    "base_max_volume": ask["base_max_volume"]
                }
            asks_converted_list.append(converted_ask)
    except KeyError:
        logger.warning(f"Error in [get_and_parse_orderbook]: {e}")
        pass
    orderbook["bids"] = bids_converted_list
    orderbook["asks"] = asks_converted_list
    return orderbook


# SUMMARY Endpoint
# tuple, string -> dictionary
# Receiving tuple with base and rel as an argument and producing CMC summary endpoint data, requires mm2 rpc password and sql db connection
def summary_for_pair(pair, path_to_db, days=1):
    try:
        suffix = get_suffix(days)
        DB = sqlite_db.sqliteDB(path_to_db, dict_format=True)
        pair_summary = OrderedDict()
        swaps_for_pair = DB.get_swaps_for_pair(pair, days)
        gecko_cached_data = DB.gecko_data
        DB.close()
        pair_summary["pair_swaps_count"] = len(swaps_for_pair)
        pair_volumes_and_prices = count_volumes_and_prices(swaps_for_pair, pair, path_to_db, days)
        pair_summary["trading_pair"] = pair[0] + "_" + pair[1]
        pair_summary["last_price"] = "{:.10f}".format(pair_volumes_and_prices["last_price"])
        orderbook = orderbook_for_pair(pair)
        pair_summary["lowest_ask"] = "{:.10f}".format(Decimal(find_lowest_ask(orderbook)))
        pair_summary["highest_bid"] = "{:.10f}".format(Decimal(find_highest_bid(orderbook)))
        pair_summary["base_currency"] = pair[0]
        pair_summary["base_volume"] = "{:.10f}".format(pair_volumes_and_prices["base_volume"])
        pair_summary["quote_currency"] = pair[1]
        pair_summary["quote_volume"] = "{:.10f}".format(pair_volumes_and_prices["quote_volume"])
        pair_summary[f"price_change_percent_{suffix}"] = "{:.10f}".format(pair_volumes_and_prices[f"price_change_percent_{suffix}"])
        pair_summary[f"highest_price_{suffix}"] = "{:.10f}".format(pair_volumes_and_prices[f"highest_price_{suffix}"])
        pair_summary[f"lowest_price_{suffix}"] = "{:.10f}".format(pair_volumes_and_prices[f"lowest_price_{suffix}"])
    except Exception as e:
        logger.error(f"Error while getting summary for pair {pair}: {e}")
    # liqudity in USD
    try:
        base_liqudity_in_coins = orderbook["total_asks_base_vol"]
        rel_liqudity_in_coins = orderbook["total_bids_rel_vol"]
        try:
            base_liqudity_in_usd = float(gecko_cached_data[pair_summary["base_currency"]]["usd_price"]) \
                * float(base_liqudity_in_coins)
        except KeyError:
            base_liqudity_in_usd = 0
        try:
            rel_liqudity_in_usd = float(gecko_cached_data[pair_summary["quote_currency"]]["usd_price"]) \
                * float(rel_liqudity_in_coins)
        except KeyError:
            rel_liqudity_in_usd = 0

        pair_summary["base_liqudity_coins"] = base_liqudity_in_coins
        pair_summary["rel_liqudity_coins"] = rel_liqudity_in_coins
        pair_summary["base_liqudity_usd"] = base_liqudity_in_usd
        pair_summary["rel_liqudity_usd"] = rel_liqudity_in_usd
        pair_summary["pair_liqudity_usd"] = base_liqudity_in_usd + rel_liqudity_in_usd
    except KeyError:
        pair_summary["base_liqudity_coins"] = 0
        pair_summary["rel_liqudity_coins"] = 0
        pair_summary["base_liqudity_usd"] = 0
        pair_summary["rel_liqudity_usd"] = 0
        pair_summary["pair_liqudity_usd"] = 0
    
    # Value traded in USD
    try:
        base_volume = pair_volumes_and_prices["base_volume"]
        rel_volume = pair_volumes_and_prices["quote_volume"]
        try:
            base_value_in_usd = float(gecko_cached_data[pair_summary["base_currency"]]["usd_price"]) \
                * float(base_volume)
        except KeyError:
            base_value_in_usd = 0
        try:
            rel_value_in_usd = float(gecko_cached_data[pair_summary["quote_currency"]]["usd_price"]) \
                * float(rel_volume)
        except KeyError:
            rel_value_in_usd = 0

        pair_summary["base_volume_coins"] = base_volume
        pair_summary["rel_volume_coins"] = rel_volume
        pair_summary["base_value_usd"] = base_value_in_usd
        pair_summary["rel_value_usd"] = rel_value_in_usd
        pair_summary["pair_value_usd"] = base_value_in_usd + rel_value_in_usd
    except KeyError:
        pair_summary["base_volume_coins"] = 0
        pair_summary["rel_volume_coins"] = 0
        pair_summary["base_value_usd"] = 0
        pair_summary["rel_value_usd"] = 0
        pair_summary["pair_value_usd"] = 0
    
    return pair_summary


def ticker_for_pair(pair, path_to_db):
    pair_ticker = OrderedDict()
    DB = sqlite_db.sqliteDB(path_to_db, dict_format=True)
    swaps_for_pair_24h = DB.get_swaps_for_pair(pair)
    DB.close()
    pair_24h_volumes_and_prices = count_volumes_and_prices(swaps_for_pair_24h, pair, path_to_db)
    pair_ticker[pair[0] + "_" + pair[1]] = OrderedDict()
    pair_ticker[pair[0] + "_" + pair[1]]["last_price"] = "{:.10f}".format(pair_24h_volumes_and_prices["last_price"])
    pair_ticker[pair[0] + "_" + pair[1]]["quote_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["quote_volume"])
    pair_ticker[pair[0] + "_" + pair[1]]["base_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["base_volume"])
    pair_ticker[pair[0] + "_" + pair[1]]["isFrozen"] = "0"
    return pair_ticker


# Orderbook Endpoint
def orderbook_for_pair(pair, endpoint=False):
    if "_" in pair:
        pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return {"error": "not valid pair"}
    orderbook_data = OrderedDict()
    orderbook_data["timestamp"] = "{}".format(int(datetime.now().strftime("%s")))
    # TODO: maybe it'll be asked on API side? quite tricky to convert strings and sort the
    data = get_and_parse_orderbook(pair, endpoint)
    orderbook_data["bids"] = data["bids"]
    orderbook_data["asks"] = data["asks"]
    orderbook_data["asks"] = data["asks"]
    orderbook_data["total_asks_base_vol"] = data["total_asks_base_vol"]
    orderbook_data["total_bids_rel_vol"] = data["total_bids_rel_vol"]
    return orderbook_data


# Trades Endpoint

def trades_for_pair(pair, path_to_db):
    logger.info(pair)
    pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return {"error": "not valid pair"}

    DB = sqlite_db.sqliteDB(path_to_db, dict_format=True)
    swaps_for_pair_24h = DB.get_swaps_for_pair(pair)
    DB.close()

    trades_info = []
    for swap_status in swaps_for_pair_24h:
        trade_info = OrderedDict()
        trade_info["trade_id"] = swap_status["uuid"]
        trade_info["price"] = "{:.10f}".format(Decimal(swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"]))
        trade_info["base_volume"] = swap_status["maker_amount"]
        trade_info["quote_volume"] = swap_status["taker_amount"]
        trade_info["timestamp"] = swap_status["started_at"]
        trade_info["type"] = swap_status["trade_type"]
        trades_info.append(trade_info)

    return trades_info


def get_chunks(data, chunk_length):
    for i in range(0, len(data), chunk_length):
        yield data[i:i + chunk_length]


def get_coins_config():
    with open("coins_config.json", "r") as coins_json:
        return json.load(coins_json)


def get_data_from_gecko():
    coin_ids = []
    coins_info = {}
    gecko_coins = {}
    coins_config = get_coins_config()
    for coin in coins_config:
        coins_info.update({coin: {}})
        coins_info[coin].update({
            "usd_market_cap": 0,
            "usd_price": 0,
            "coingecko_id": ""
        })
        # filter for coins with coingecko_id
        if "coingecko_id" in coins_config[coin]:
            coin_id = coins_config[coin]["coingecko_id"]
            # ignore test coins
            if coin_id not in ["na", "test-coin", ""]:
                coins_info[coin].update({"coingecko_id": coin_id})
                if coin_id not in coin_ids: coin_ids.append(coin_id)
                if coin_id not in gecko_coins: gecko_coins[coin_id] = []
                gecko_coins[coin_id].append(coin)
                # Special case for tokens like USDT which have no base variant
                if coin.split("-")[0] not in gecko_coins[coin_id]:
                    gecko_coins[coin_id].append(coin.split("-")[0])
                if coin.split("-")[0] not in coins_info:
                    coins_info.update({
                        coin.split("-")[0]: {
                            "usd_market_cap": 0,
                            "usd_price": 0,
                            "coingecko_id": coin_id
                        }
                    })

    coin_ids.sort()
    param_limit = 200
    logger.debug(f"{len(coin_ids)} coins_ids to get data from gecko")
    coin_id_chunks = list(get_chunks(coin_ids, param_limit))
    logger.debug(f"{len(coin_id_chunks)} chunks")
    for chunk in coin_id_chunks:
        chunk_ids = ",".join(chunk)
        r = ""
        try:
            url = f'https://api.coingecko.com/api/v3/simple/price?ids={chunk_ids}&vs_currencies=usd&include_market_cap=true'
            gecko_data = requests.get(url).json()
        except Exception as e:
            return {"error": "https://api.coingecko.com/api/v3/simple/price?ids= is not available"}
        try:
            for coin_id in gecko_data:
                # This gracefully skips cases where id returned from api is not the same as in the url
                try:
                    coins = gecko_coins[coin_id]
                    for coin in coins:
                        if "usd" in gecko_data[coin_id]:
                            coins_info[coin].update({"usd_price": gecko_data[coin_id]["usd"]})
                        else:
                            coins_info[coin].update({"usd_price": 0})
                        if "usd_market_cap" in gecko_data[coin_id]:
                            coins_info[coin].update({"usd_market_cap": gecko_data[coin_id]["usd_market_cap"]})
                        else:
                            coins_info[coin].update({"usd_market_cap": 0})
                except:
                    logger.warning(f"CoinGecko ID in response does not match ID in request [{coin_id}]")
                    pass

        except Exception as e:
            logger.info(f'Error in [get_data_from_gecko]: {e}')
    return coins_info


# Data for atomicdex.io website
def atomicdex_info(path_to_db):
    try:
        DB = sqlite_db.sqliteDB(path_to_db)
        summary = DB.get_adex_summary()
        pairs = DB.get_pairs()
        summary_data = [summary_for_pair(pair, path_to_db, days=1) for pair in pairs]
        current_liqudity = 0
        current_liqudity += sum([pair_summary["pair_liqudity_usd"] for pair_summary in summary_data])
        summary.update({"current_liqudity": current_liqudity})
    except Exception as e:
        logger.error(f"Error in [atomicdex_info]: {e}")
        return None
    return summary


def get_liquidity(summary_data: dict) -> float:
    '''Returns the total liquidity of all pairs in the summary data'''
    liqudity = 0
    liqudity += sum([pair_summary["pair_liqudity_usd"] for pair_summary in summary_data])
    return liqudity


def get_value(summary_data: dict) -> float:
    '''Returns the total value of all pairs in the summary data'''
    value = 0
    value += sum([pair_summary["pair_value_usd"] for pair_summary in summary_data])
    return value


def atomicdex_timespan_info(path_to_db: str, days: int=14) -> dict:
    '''Summary data for a timespan (e.g. 14 days for marketing tweets etc.)'''
    try:
        DB = sqlite_db.sqliteDB(path_to_db)
        days = 14
        summary = {}
        pairs = DB.get_pairs(days)
        swaps = DB.get_timespan_swaps(days)
        # Total Value traded
        summary_data = [summary_for_pair(pair, path_to_db, days) for pair in pairs]
        summary.update({
            "days": days,
            "swaps_count": len(swaps),
            "swaps_value": get_value(summary_data),
            "current_liqudity": get_liquidity(summary_data),
            "top_pairs": get_top_pairs(path_to_db, pairs, swaps, days)
        })

    except Exception as e:
        logger.error(f"Error in [atomicdex_info]: {e}")
        return None
    return summary


def get_top_pairs(path_to_db, pairs, swaps, days=14):
    try:
        pairs_data = [summary_for_pair(pair, path_to_db, days) for pair in pairs]
        print(pairs_data)
        pairs_data.sort(key=lambda x: x["pair_value_usd"], reverse=True)
        value_data = pairs_data[:3]
        top_pairs_by_value = [{i['trading_pair']:i['pair_value_usd']} for i in value_data]
        pairs_data.sort(key=lambda x: x["pair_liqudity_usd"], reverse=True)
        liqudity_data = pairs_data[:3]
        top_pairs_by_liquidity = [{i['trading_pair']:i['pair_liqudity_usd']} for i in liqudity_data]
        pairs_data.sort(key=lambda x: x["pair_swaps_count"], reverse=True)
        swaps_data = pairs_data[:3]
        top_pairs_by_swaps = [{i['trading_pair']:i['pair_swaps_count']} for i in swaps_data]
        return {
            "by_value_usd": top_pairs_by_value,
            "by_liqudity_usd": top_pairs_by_liquidity,
            "by_swaps_count": top_pairs_by_swaps
        }
    except Exception as e:
        logger.error(f"Error in [get_top_pairs]: {e}")
        return {
            "by_volume": [],
            "by_liqudity": [],
            "by_swaps": []
        }
