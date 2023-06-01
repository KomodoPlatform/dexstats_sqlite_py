#!/usr/bin/env python3
import os
import requests
import json
from decimal import Decimal
from datetime import datetime, timedelta
from collections import OrderedDict
from logger import logger
import sqlite_db
import const


def get_suffix(days: int) -> str:
    if days == 1:
        return "24h"
    else:
        return f"{days}d"


def get_volumes_and_prices_template(suffix) -> dict:
    return {
        "base_volume": 0,
        "quote_volume": 0,
        f"highest_price_{suffix}": 0,
        f"lowest_price_{suffix}": 0,
        "last_price": 0,
        f"price_change_percent_{suffix}": 0
    }


def get_swap_prices(swaps_for_pair: list) -> dict:
    swap_prices = {}
    for swap in swaps_for_pair:
        swap_price = Decimal(
            swap["taker_amount"]) / Decimal(swap["maker_amount"])
        swap_prices[swap["started_at"]] = swap_price
    return swap_prices


def get_swaps_volumes(swaps_for_pair: list) -> dict:
    try:
        base_volume = 0
        quote_volume = 0
        for swap in swaps_for_pair:
            base_volume += swap["maker_amount"]
            quote_volume += swap["taker_amount"]
        return [base_volume, quote_volume]
    except Exception as e:
        logger.info(f"Error in [get_swaps_volumes]: {e}")
        return [0, 0]


def get_volumes_and_prices(
        swaps_for_pair: list,
        pair: str,
        days: int = 1,
        DB: sqlite_db.sqliteDB = None
        ) -> dict:
    '''
    Iterates over list of swaps to get data for CMC summary endpoint
    '''
    try:
        if not DB:
            DB = sqlite_db.sqliteDB(const.MM2_DB_PATH, dict_format=True)
        suffix = get_suffix(days)
        volumes_and_prices = get_volumes_and_prices_template(suffix)
        swap_prices = get_swap_prices(swaps_for_pair)
        swaps_volumes = get_swaps_volumes(swaps_for_pair)
        volumes_and_prices["base_volume"] = swaps_volumes[0]
        volumes_and_prices["quote_volume"] = swaps_volumes[1]
    except Exception as e:
        logger.info(f"Error in [get_volumes_and_prices]: {e}")
    if len(swap_prices) > 0:
        highest_price = max(swap_prices.values())
        lowest_price = min(swap_prices.values())
        last_price = swap_prices[max(swap_prices.keys())]
        oldest_price = swap_prices[min(swap_prices.keys())]
        pct_change = Decimal(last_price - oldest_price) / Decimal(100)
        volumes_and_prices[f"highest_price_{suffix}"] = highest_price
        volumes_and_prices[f"lowest_price_{suffix}"] = lowest_price
        volumes_and_prices["last_price"] = last_price
        volumes_and_prices[f"price_change_percent_{suffix}"] = pct_change
    else:
        volumes_and_prices["last_price"] = DB.get_last_price_for_pair(pair)

    return volumes_and_prices


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


def find_lowest_ask(orderbook: list) -> str:
    '''Returns lowest ask from provided orderbook'''
    lowest_ask = {"price": "0"}
    try:
        for ask in orderbook["asks"]:
            if lowest_ask["price"] == "0":
                lowest_ask = ask
            elif Decimal(ask["price"]) < Decimal(lowest_ask["price"]):
                lowest_ask = ask
    except KeyError:
        pass
    return "{:.10f}".format(Decimal(lowest_ask["price"]))


def find_highest_bid(orderbook: list) -> str:
    '''Returns highest bid from provided orderbook'''
    highest_bid = {"price": "0"}
    try:
        for bid in orderbook["bids"]:
            if Decimal(bid["price"]) > Decimal(highest_bid["price"]):
                highest_bid = bid
    except KeyError:
        pass
    return "{:.10f}".format(Decimal(highest_bid["price"]))


def get_related_coins(coin):
    script_path = os.path.realpath(os.path.dirname(__file__))
    coin = coin.split("-")[0]
    with open(f"{script_path}/mm2/coins", "r") as f:
        coins = json.load(f)
    return [i["coin"] for i in coins if i["coin"] == coin or i["coin"].startswith(f"{coin}-")]


def get_orderbooks_list(pair):
    # TODO: Move to dex_utils
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
    return orderbooks_list


def get_and_parse_orderbook(pair, endpoint=False, orderbooks_list=None):
    try:
        if not orderbooks_list:
            orderbooks_list = get_orderbooks_list(pair)
        orderbook = {
            "pair": f"{pair[0]}_{pair[1]}",
            "bids": [],
            "asks": [],
            "total_asks_base_vol": 0,
            "total_bids_rel_vol": 0
        }
        for i in orderbooks_list:
            # case when there is no such ticker in coins file
            if next(iter(i)) == "error":
                i = {
                        "bids": [],
                        "asks": [],
                        "total_asks_base_vol": 0,
                        "total_bids_rel_vol": 0
                    }
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
    except KeyError as e:
        logger.warning(f"Error in [get_and_parse_orderbook]: {e}")
        pass
    orderbook["bids"] = bids_converted_list
    orderbook["asks"] = asks_converted_list
    return orderbook


def get_gecko_usd_price(coin: str, gecko_cached_data: dict = None) -> float:
    if not gecko_cached_data:
        gecko_cached_data = sqlite_db.sqliteDB(const.MM2_DB_PATH).gecko_data
    try:
        return gecko_cached_data[coin]["usd_price"]
    except KeyError:
        return 0


def get_pair_summary_template(base: str, quote: str) -> dict:
    pair_summary = OrderedDict()
    pair_summary["trading_pair"] = f"{base}_{quote}"
    pair_summary["base_currency"] = base
    pair_summary["quote_currency"] = quote
    pair_summary["pair_swaps_count"] = 0
    pair_summary["base_price_usd"] = 0
    pair_summary["rel_price_usd"] = 0
    pair_summary["base_volume_coins"] = 0
    pair_summary["rel_volume_coins"] = 0
    pair_summary["base_liquidity_coins"] = 0
    pair_summary["base_liquidity_usd"] = 0
    pair_summary["base_trade_value_usd"] = 0
    pair_summary["rel_liquidity_coins"] = 0
    pair_summary["rel_liquidity_usd"] = 0
    pair_summary["rel_trade_value_usd"] = 0
    pair_summary["pair_liquidity_usd"] = 0
    pair_summary["pair_trade_value_usd"] = 0
    pair_summary["lowest_ask"] = 0
    pair_summary["highest_bid"] = 0
    return pair_summary


def safe_float(value, rounding=8):
    try:
        return round(float(value), rounding)
    except (ValueError, TypeError):
        return 0


def summary_for_pair(
        pair: str,
        days: int,
        DB: sqlite_db.sqliteDB = None,
        orderbook: dict = None) -> dict:
    '''Calculates CMC summary endpoint data for a pair'''
    try:
        if not DB:
            DB = sqlite_db.sqliteDB(const.MM2_DB_PATH)
        base = pair[0]
        quote = pair[1]
        suffix = get_suffix(days)
        pair_summary = get_pair_summary_template(base, quote)
        timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))

        swaps_for_pair = DB.get_swaps_for_pair(pair, timestamp)
        gecko_cached_data = DB.gecko_data
        pair_summary["pair_swaps_count"] = len(swaps_for_pair)

        volumes_and_prices = get_volumes_and_prices(swaps_for_pair, pair, days, DB)
        for i in ["last_price", "base_volume", "quote_volume"]:
            value = "{:.10f}".format(volumes_and_prices[i])
            pair_summary[i] = value

        for i in ["price_change_percent", "highest_price", "lowest_price"]:
            value = "{:.10f}".format(volumes_and_prices[f"{i}_{suffix}"])
            pair_summary[f"{i}_{suffix}"] = value

    except Exception as e:
        logger.error(f"Error while getting summary for pair {pair}: {e}")

    # Liquidity
    base_price = get_gecko_usd_price(base, gecko_cached_data)
    base_volume = volumes_and_prices["base_volume"]
    quote_price = get_gecko_usd_price(quote, gecko_cached_data)
    quote_volume = volumes_and_prices["quote_volume"]

    pair_summary["base_price_usd"] = base_price
    pair_summary["rel_price_usd"] = quote_price
    pair_summary["base_volume_coins"] = base_volume
    pair_summary["rel_volume_coins"] = quote_volume

    try:
        if not orderbook:
            orderbook = orderbook_for_pair(pair)
        pair_summary["lowest_ask"] = find_lowest_ask(orderbook)
        pair_summary["highest_bid"] = find_highest_bid(orderbook)
        pair_summary["base_liquidity_coins"] = orderbook["total_asks_base_vol"]
        pair_summary["rel_liquidity_coins"] = orderbook["total_bids_rel_vol"]
    except Exception as e:
        logger.error(f"Error while getting orderbook info for {pair}: {e}")

    try:
        pair_summary["base_liquidity_usd"] = safe_float(
            Decimal(base_price) * Decimal(pair_summary["base_liquidity_coins"]),
            2
        )
    except KeyError:
        pass

    try:
        pair_summary["rel_liquidity_usd"] = safe_float(
            Decimal(quote_price) * Decimal(pair_summary["rel_liquidity_coins"]),
            2
        )
    except KeyError:
        pass
    pair_summary["pair_liquidity_usd"] = pair_summary["rel_liquidity_usd"] \
        + pair_summary["base_liquidity_usd"]

    # Value traded in USD
    try:
        pair_summary["base_trade_value_usd"] = safe_float(
            Decimal(base_price) * Decimal(base_volume),
            2
        )
    except KeyError:
        pass
    try:
        pair_summary["rel_trade_value_usd"] = safe_float(
            Decimal(quote_price) * Decimal(quote_volume),
            2
        )
    except KeyError:
        pass
    pair_summary["pair_trade_value_usd"] = pair_summary["base_trade_value_usd"] \
        + pair_summary["rel_trade_value_usd"]
    return pair_summary


def ticker_for_pair(
        pair,
        DB: sqlite_db.sqliteDB = None,
        days=1) -> dict:
    if not DB:
        DB = sqlite_db.sqliteDB(const.MM2_DB_PATH, dict_format=True)
    pair_ticker = OrderedDict()
    swaps_for_pair = DB.get_swaps_for_pair(pair)
    volumes_and_prices = get_volumes_and_prices(swaps_for_pair, pair, days, DB)
    pair_ticker[pair[0] + "_" + pair[1]] = OrderedDict()
    last_price = "{:.10f}".format(volumes_and_prices["last_price"])
    pair_ticker[pair[0] + "_" + pair[1]]["last_price"] = last_price
    quote_volume = "{:.10f}".format(volumes_and_prices["quote_volume"])
    pair_ticker[pair[0] + "_" + pair[1]]["quote_volume"] = quote_volume
    base_volume = "{:.10f}".format(volumes_and_prices["base_volume"])
    pair_ticker[pair[0] + "_" + pair[1]]["base_volume"] = base_volume
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
    data = get_and_parse_orderbook(pair, endpoint)
    orderbook_data["bids"] = data["bids"]
    orderbook_data["asks"] = data["asks"]
    orderbook_data["asks"] = data["asks"]
    orderbook_data["total_asks_base_vol"] = data["total_asks_base_vol"]
    orderbook_data["total_bids_rel_vol"] = data["total_bids_rel_vol"]
    return orderbook_data


def validate_pair(pair: tuple) -> bool:
    if len(pair) != 2 \
            or not isinstance(pair[0], str) \
            or not isinstance(pair[0], str):
        return False
    return True


def trades_for_pair(pair: str, DB: sqlite_db.sqliteDB = None):

    if not DB:
        DB = sqlite_db.sqliteDB(const.MM2_DB_PATH, dict_format=True)
    pair = tuple(map(str, pair.split('_')))
    if not validate_pair(pair):
        return {"error": "not valid pair"}
    swaps_for_pair = DB.get_swaps_for_pair(pair)

    trades_info = []
    for swap in swaps_for_pair:
        trade_info = OrderedDict()
        trade_info["trade_id"] = swap["uuid"]
        price = Decimal(swap["taker_amount"]) / Decimal(swap["maker_amount"])
        trade_info["price"] = "{:.10f}".format(price)
        trade_info["base_volume"] = swap["maker_amount"]
        trade_info["quote_volume"] = swap["taker_amount"]
        trade_info["timestamp"] = swap["started_at"]
        trade_info["type"] = swap["trade_type"]
        trades_info.append(trade_info)

    return trades_info


def get_chunks(data, chunk_length):
    for i in range(0, len(data), chunk_length):
        yield data[i:i + chunk_length]


def get_coins_config():
    with open("coins_config.json", "r") as coins_json:
        return json.load(coins_json)


def get_gecko_info_template(coin_id=""):
    return {
        "usd_market_cap": 0,
        "usd_price": 0,
        "coingecko_id": coin_id
    }


def get_gecko_coin_ids_list(coins_config: dict) -> list:
    coin_ids = list(set([
        coins_config[i]["coingecko_id"] for i in coins_config
        if coins_config[i]["coingecko_id"] not in ["na", "test-coin", ""]
    ]))
    coin_ids.sort()
    logger.debug(f"{len(coin_ids)} coins_ids to get data from gecko")
    return coin_ids


def get_gecko_info_dict(coins_config: dict) -> dict:
    coins_info = {}
    for coin in coins_config:
        coins_info.update({
            coin: get_gecko_info_template()
        })
    for coin in coins_config:
        coin_id = coins_config[coin]["coingecko_id"]
        native_coin = coin.split("-")[0]
        if native_coin not in coins_info:
            coins_info.update({
                native_coin: get_gecko_info_template(coin_id)
            })
    return coins_info


def get_gecko_coins_dict(coins_config: dict, coin_ids: list) -> dict:
    gecko_coins = {}
    for coin_id in coin_ids:
        gecko_coins.update({
            coin_id: []
        })

    for coin in coins_config:
        coin_id = coins_config[coin]["coingecko_id"]
        if coin_id not in ["na", "test-coin", ""]:
            gecko_coins[coin_id].append(coin)
            native_coin = coin.split("-")[0]
            if native_coin not in gecko_coins[coin_id]:
                gecko_coins[coin_id].append(native_coin)

    return gecko_coins


def get_data_from_gecko():
    param_limit = 200
    coins_config = get_coins_config()
    coin_ids = get_gecko_coin_ids_list(coins_config)
    coins_info = get_gecko_info_dict(coins_config)
    logger.debug(f"{len(coins_info)} coins_info")
    gecko_coins = get_gecko_coins_dict(coins_config, coin_ids)
    coin_id_chunks = list(get_chunks(coin_ids, param_limit))
    logger.debug(f"{len(coin_id_chunks)} chunks")
    for chunk in coin_id_chunks:
        chunk_ids = ",".join(chunk)
        try:
            params = f"ids={chunk_ids}&vs_currencies=usd&include_market_cap=true"
            url = f'https://api.coingecko.com/api/v3/simple/price?{params}'
            gecko_data = requests.get(url).json()
        except Exception as e:
            error = {"error": f"{url} is not available"}
            logger.error(f'Error in [get_data_from_gecko]: {e} ({error})')
            return error
        try:
            for coin_id in gecko_data:
                try:
                    coins = gecko_coins[coin_id]
                    for coin in coins:
                        if "usd" in gecko_data[coin_id]:
                            coins_info[coin].update({
                                "usd_price": gecko_data[coin_id]["usd"]
                            })
                        if "usd_market_cap" in gecko_data[coin_id]:
                            coins_info[coin].update({
                                "usd_market_cap": gecko_data[coin_id]["usd_market_cap"]
                            })
                except Exception as e:
                    error = f"CoinGecko ID request/response mismatch [{coin_id}] [{e}]"
                    logger.warning(error)
                    pass

        except Exception as e:
            logger.info(f'Error in [get_data_from_gecko]: {e}')
    return coins_info


# Data for atomicdex.io website
def atomicdex_info(
        DB: sqlite_db.sqliteDB = None,
        days: int = 1):
    try:
        if not DB:
            DB = sqlite_db.sqliteDB(const.MM2_DB_PATH, dict_format=True)
        summary = DB.get_adex_summary()
        pairs = DB.get_pairs()
        summary_data = [summary_for_pair(pair, days, DB) for pair in pairs]
        current_liquidity = get_liquidity(summary_data)
        summary.update({"current_liquidity": round(current_liquidity, 2)})
    except Exception as e:
        logger.error(f"Error in [atomicdex_info]: {e}")
        return None
    return summary


def get_liquidity(summary_data: dict) -> float:
    '''Returns the total liquidity of all pairs in the summary data'''
    liquidity = 0
    liquidity += sum([i["pair_liquidity_usd"] for i in summary_data])
    return liquidity


def get_value(summary_data: dict) -> float:
    '''Returns the total value of all pairs in the summary data'''
    value = 0
    value += sum([i["pair_trade_value_usd"] for i in summary_data])
    return value


def atomicdex_timespan_info(
        DB: sqlite_db.sqliteDB = None,
        days: int = 14) -> dict:
    '''
    Summary data for a timespan (e.g. 14 days for marketing tweets etc.)
    '''
    try:
        if not DB:
            DB = sqlite_db.sqliteDB(const.MM2_DB_PATH, dict_format=True)
        summary = {}
        pairs = DB.get_pairs(days)
        swaps = DB.get_timespan_swaps(days)
        # Total Value traded
        summary_data = [summary_for_pair(pair, days, DB) for pair in pairs]
        summary.update({
            "days": days,
            "swaps_count": len(swaps),
            "swaps_value": get_value(summary_data),
            "current_liquidity": get_liquidity(summary_data),
            "top_pairs": get_top_pairs(pairs, swaps, days)
        })

    except Exception as e:
        logger.error(f"Error in [atomicdex_info]: {e}")
        return None
    return summary


def get_top_pairs(
        pairs,
        swaps,
        days: int = 14,
        DB: sqlite_db.sqliteDB = None):
    try:
        if not DB:
            DB = sqlite_db.sqliteDB(const.MM2_DB_PATH, dict_format=True)
        pairs_data = [summary_for_pair(pair, days, DB) for pair in pairs]
        pairs_data.sort(key=lambda x: x["pair_trade_value_usd"], reverse=True)
        value_data = pairs_data[:3]
        top_pairs_by_value = [
            {i['trading_pair']:i['pair_trade_value_usd']}
            for i in value_data
        ]
        pairs_data.sort(key=lambda x: x["pair_liquidity_usd"], reverse=True)
        liquidity_data = pairs_data[:3]
        top_pairs_by_liquidity = [
            {i['trading_pair']:i['pair_liquidity_usd']}
            for i in liquidity_data
        ]
        pairs_data.sort(key=lambda x: x["pair_swaps_count"], reverse=True)
        swaps_data = pairs_data[:3]
        top_pairs_by_swaps = [{
            i['trading_pair']:i['pair_swaps_count']}
            for i in swaps_data
        ]
        return {
            "by_value_traded_usd": top_pairs_by_value,
            "by_current_liquidity_usd": top_pairs_by_liquidity,
            "by_swaps_count": top_pairs_by_swaps
        }
    except Exception as e:
        logger.error(f"Error in [get_top_pairs]: {e}")
        return {
            "by_volume": [],
            "by_liquidity": [],
            "by_swaps": []
        }
