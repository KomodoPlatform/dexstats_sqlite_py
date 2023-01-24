import sqlite3
import requests
import json
from decimal import Decimal
from datetime import datetime, timedelta
from collections import OrderedDict
from lib_logger import logger

# getting list of pairs with amount of swaps > 0 from db (list of tuples)
# string -> list (of base, rel tuples)
def get_availiable_pairs(path_to_db):
    conn = sqlite3.connect(path_to_db)
    sql_cursor = conn.cursor()
    sql_cursor.execute("SELECT DISTINCT maker_coin, taker_coin FROM swaps;")
    available_pairs = sql_cursor.fetchall()
    sorted_available_pairs = []
    for pair in available_pairs:
       sorted_available_pairs.append(tuple(sorted(pair)))
    conn.close()
    # removing duplicates
    return list(set(sorted_available_pairs))


# tuple, integer -> list (with swap status dicts)
# select from DB swap statuses for desired pair with timestamps > than provided
def get_swaps_since_timestamp_for_pair(sql_cursor, pair, timestamp):
    t = (timestamp,pair[0],pair[1],)
    sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND maker_coin=? AND taker_coin=?;", t)
    swap_statuses_a_b = [dict(row) for row in sql_cursor.fetchall()]
    for swap in swap_statuses_a_b:
        swap["trade_type"] = "buy"
    sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND taker_coin=? AND maker_coin=?;", t)
    swap_statuses_b_a = [dict(row) for row in sql_cursor.fetchall()]
    # should be enough to change amounts place = change direction
    for swap in swap_statuses_b_a:
        swap["trade_type"] = "sell"
    swap_statuses = swap_statuses_a_b + swap_statuses_b_a
    return swap_statuses

# tuple, integer -> list (with swap status dicts)
# select from json cache swap statuses for desired pair with timestamps > than provided
def get_24hr_swaps_for_pair(swaps_cache_24hr, pair):
    t = (timestamp,pair[0],pair[1],)
    sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND maker_coin=? AND taker_coin=?;", t)
    swap_statuses_a_b = [dict(row) for row in sql_cursor.fetchall()]
    for swap in swap_statuses_a_b:
        swap["trade_type"] = "buy"
    sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND taker_coin=? AND maker_coin=?;", t)
    swap_statuses_b_a = [dict(row) for row in sql_cursor.fetchall()]
    # should be enough to change amounts place = change direction
    for swap in swap_statuses_b_a:
        swap["trade_type"] = "sell"
    swap_statuses = swap_statuses_a_b + swap_statuses_b_a
    return swap_statuses


def get_swaps_between_timestamps_for_pair(sql_cursor, pair, timestamp_a, timestamp_b):
    t = (timestamp_a,timestamp_b,pair[0],pair[1],)
    sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND time_stamp < ? AND maker_coin=? AND taker_coin=?;", t)
    swap_statuses_a_b = [dict(row) for row in sql_cursor.fetchall()]
    for swap in swap_statuses_a_b:
        swap["trade_type"] = "buy"
    sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ? AND time_stamp < ? AND taker_coin=? AND maker_coin=?;", t)
    swap_statuses_b_a = [dict(row) for row in sql_cursor.fetchall()]
    # should be enough to change amounts place = change direction
    for swap in swap_statuses_b_a:
        swap["trade_type"] = "sell"
    swap_statuses = swap_statuses_a_b + swap_statuses_b_a
    return swap_statuses


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
            swap_prices.update({swap_status["time_stamp"]: swap_price})
        if swap_status["trade_type"] == "sell":
            base_volume += swap_status["taker_amount"]
            quote_volume += swap_status["maker_amount"]
            swap_price = Decimal(swap_status["maker_amount"]) / Decimal(swap_status["taker_amount"])
            swap_prices.update({swap_status["time_stamp"]: swap_price})


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
    return pair_volumes_and_prices


# tuple, string, string -> list
# returning orderbook for given trading pair
def get_mm2_orderbook_for_pair(pair):
    mm2_host = "http://127.0.0.1:7783"
    params = {
              'method': 'orderbook',
              'base': pair[0],
              'rel': pair[1]
             }
    r = requests.post(mm2_host, json=params)
    return json.loads(r.text)


# list -> string
# returning lowest ask from provided orderbook

def find_lowest_ask(orderbook):
    lowest_ask = {"price" : "0"}
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
    highest_bid = {"price" : "0"}
    try:
        for bid in orderbook["bids"]:
            if Decimal(bid["price"]) > Decimal(highest_bid["price"]):
                highest_bid = bid
    except KeyError:
        return 0
    return highest_bid["price"]


def get_and_parse_orderbook(pair):
    orderbook = get_mm2_orderbook_for_pair(pair)
    bids_converted_list = []
    asks_converted_list = []
    try:
        for bid in orderbook["bids"]:
            converted_bid = []
            converted_bid.append(bid["price"])
            converted_bid.append(bid["maxvolume"])
            bids_converted_list.append(converted_bid)
    except KeyError:
        pass
    try:
        for ask in orderbook["asks"]:
            converted_ask = []
            converted_ask.append(ask["price"])
            converted_ask.append(ask["maxvolume"])
            asks_converted_list.append(converted_ask)
    except KeyError:
        pass
    return bids_converted_list, asks_converted_list


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
        orderbook = get_mm2_orderbook_for_pair(pair)
        pair_summary["lowest_ask"] = "{:.10f}".format(Decimal(find_lowest_ask(orderbook)))
        pair_summary["highest_bid"] = "{:.10f}".format(Decimal(find_highest_bid(orderbook)))
    except:
        # This should throw an alert via discord/mattermost/telegram
        pair_summary["lowest_ask"] = "{:.10f}".format(Decimal(0))
        pair_summary["highest_bid"] = "{:.10f}".format(Decimal(0))
        logger.warning("Couldn't get orderbook! Is mm2 running?")
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
        if swap["time_stamp"] > last_swap_timestamp:
            last_swap_timestamp = swap["time_stamp"]
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


# Orderbook Endpoint
def orderbook_for_pair(pair):
    pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return {"error": "not valid pair"}
    orderbook_data = OrderedDict()
    orderbook_data["timestamp"] = "{}".format(int(datetime.now().strftime("%s")))
    # TODO: maybe it'll be asked on API side? quite tricky to convert strings and sort the
    orderbook_data["bids"] = get_and_parse_orderbook(pair)[0]
    orderbook_data["asks"] = get_and_parse_orderbook(pair)[1]
    return orderbook_data


# Trades Endpoint
def trades_for_pair(pair, path_to_db, days_in_past):
    pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return {"error": "not valid pair"}
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_cursor = conn.cursor()
    timestamp_since = int((datetime.now() - timedelta(days_in_past)).strftime("%s"))
    swaps_for_pair_since_timestamp = get_swaps_since_timestamp_for_pair(sql_cursor, pair, timestamp_since)
    trades_info = []
    for swap_status in swaps_for_pair_since_timestamp:
        trade_info = OrderedDict()
        trade_info["trade_id"] = swap_status["uuid"]
        trade_info["price"] = "{:.10f}".format(Decimal(swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"]))
        trade_info["base_volume"] = swap_status["maker_amount"]
        trade_info["quote_volume"] = swap_status["taker_amount"]
        trade_info["timestamp"] = swap_status["time_stamp"]
        trade_info["type"] = swap_status["trade_type"]
        trades_info.append(trade_info)
    conn.close()
    return trades_info


# Data for atomicdex.io website
def atomicdex_info(path_to_db):
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    timestamp_30d_ago = int((datetime.now() - timedelta(30)).strftime("%s"))
    conn = sqlite3.connect(path_to_db)
    sql_cursor = conn.cursor()
    sql_cursor.execute("SELECT * FROM swaps;")
    swaps_all_time = len(sql_cursor.fetchall())
    sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ?;", (timestamp_24h_ago,))
    swaps_24h = len(sql_cursor.fetchall())
    sql_cursor.execute("SELECT * FROM swaps WHERE time_stamp > ?;", (timestamp_30d_ago,))
    swaps_30d = len(sql_cursor.fetchall())
    conn.close()

    return {
        "swaps_all_time" : swaps_all_time,
        "swaps_30d" : swaps_30d,
        "swaps_24h" : swaps_24h
    }


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
    available_pairs_summary_ticker = get_availiable_pairs(path_to_db)
    logger.info("got available pairs")
    summary_data = []
    for pair in available_pairs_summary_ticker:
        if ticker_summary in pair:
            summary_data.append(summary_for_pair(pair))
            logger.info("got summary_for_pair")
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
    available_pairs_ticker = get_availiable_pairs(path_to_db)
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
    available_pairs_ticker = get_availiable_pairs(path_to_db)
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
    with open('24hr_swaps_cache_by_pair.json', 'r') as json_file:
        swaps_cache_24hr_by_pair = json.load(json_file)
    data_a_b = []
    data_b_a = []
    if f"{pair[0]}/{pair[1]}" in swaps_cache_24hr_by_pair:
        data_a_b = swaps_cache_24hr_by_pair[f"{pair[0]}/{pair[1]}"]
        for swap in data_a_b:
            swap["trade_type"] = "buy"
    if f"{pair[1]}/{pair[0]}" in swaps_cache_24hr_by_pair:
        data_b_a = swaps_cache_24hr_by_pair[f"{pair[1]}/{pair[0]}"]
        for swap in data_b_a:
            swap["trade_type"] = "sell"

    return data_b_a + data_a_b

def get_tickers_summary(path_to_db):

    available_pairs = get_availiable_pairs(path_to_db)

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


def get_24hr_swaps_data(path_to_db):
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_cursor = conn.cursor()
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    sql = f"SELECT * FROM swaps WHERE time_stamp > {timestamp_24h_ago};"
    sql_cursor.execute(sql)
    return [dict(row) for row in sql_cursor.fetchall()]


# Returns last 24hrs swap data by maker/taker pair
def get_24hr_swaps_data_by_pair(path_to_db):
    data = get_24hr_swaps_data(path_to_db)
    pair_data = {}
    for i in data:
        maker = i["maker_coin"]
        taker = i["taker_coin"]
        pair = f"{maker}/{taker}"
        if pair not in pair_data:
            pair_data.update({pair:[]})
        pair_data[pair].append(i)
    return pair_data


