import sqlite3
import requests
import json
from decimal import Decimal
from datetime import datetime, timedelta
from collections import OrderedDict

# getting list of pairs with amount of swaps > 0 from db (list of tuples)
# string -> list (of base, rel tuples)
def get_availiable_pairs(path_to_db):
    conn = sqlite3.connect(path_to_db)
    sql_coursor = conn.cursor()
    sql_coursor.execute("SELECT DISTINCT maker_coin_ticker, taker_coin_ticker FROM stats_swaps;")
    available_pairs = sql_coursor.fetchall()
    sorted_available_pairs = []
    for pair in available_pairs:
       sorted_available_pairs.append(tuple(sorted(pair)))
    conn.close()
    # removing duplicates
    return list(set(sorted_available_pairs))

# tuple, integer -> list (with swap status dicts)
# select from DB swap statuses for desired pair with timestamps > than provided
def get_swaps_since_timestamp_for_pair(sql_coursor, pair, timestamp):
    t = (timestamp,pair[0],pair[1],)
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND maker_coin_ticker=? AND taker_coin_ticker=? AND is_success=1;", t)
    swap_statuses_a_b = [dict(row) for row in sql_coursor.fetchall()]
    for swap in swap_statuses_a_b:
        swap["trade_type"] = "buy"
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND taker_coin_ticker=? AND maker_coin_ticker=? AND is_success=1;", t)
    swap_statuses_b_a = [dict(row) for row in sql_coursor.fetchall()]
    # should be enough to change amounts place = change direction
    for swap in swap_statuses_b_a:
        temp_maker_amount = swap["maker_amount"]
        swap["maker_amount"] = swap["taker_amount"]
        swap["taker_amount"] = temp_maker_amount
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
        base_volume += swap_status["maker_amount"]
        quote_volume += swap_status["taker_amount"]
        swap_price = Decimal(swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"])
        swap_prices[swap_status["started_at"]] = swap_price

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
    orderbook = json.loads(r.text)
    return orderbook


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
    if "-ERC20" not in pair[0] and "-ERC20" not in pair[1] and "-BEP20" not in pair[0] and "-BEP20" not in pair[1]:
        # TODO use combinatorica lib and tokens types list
        pair_erc20_a = (pair[0] + "-ERC20", pair[1])
        pair_erc20_a_orderbook = get_mm2_orderbook_for_pair(pair_erc20_a)
        pair_erc20_b = (pair[0], pair[1] + "-ERC20")
        pair_erc20_b_orderbook = get_mm2_orderbook_for_pair(pair_erc20_b)
        pair_bep20_a = (pair[0] + "-BEP20", pair[1])
        pair_bep20_a_orderbook = get_mm2_orderbook_for_pair(pair_bep20_a)
        pair_bep20_b = (pair[0], pair[1] + "-BEP20")
        pair_bep20_b_orderbook = get_mm2_orderbook_for_pair(pair_bep20_b)
        pair_erc20_erc20 = (pair[0] + "-ERC20", pair[1] + "-ERC20")
        pair_erc20_erc20_orderbook = get_mm2_orderbook_for_pair(pair_erc20_erc20)
        pair_bep20_bep20 = (pair[0] + "-BEP20", pair[1] + "-BEP20")
        pair_bep20_bep20_orderbook = get_mm2_orderbook_for_pair(pair_bep20_bep20)
        pair_bep20_erc20 =  (pair[0] + "-BEP20", pair[1] + "-ERC20")
        pair_bep20_erc20_orderbook = get_mm2_orderbook_for_pair(pair_bep20_erc20)
        pair_erc20_bep20 = (pair[0] + "-ERC20", pair[1] + "-BEP20")
        pair_erc20_bep20_orderbook = get_mm2_orderbook_for_pair(pair_erc20_bep20)
        usual_orderbook = get_mm2_orderbook_for_pair(pair)
        orderbooks_list = [usual_orderbook, pair_erc20_a_orderbook, pair_erc20_b_orderbook, pair_bep20_a_orderbook, pair_bep20_b_orderbook, pair_erc20_erc20_orderbook, pair_bep20_bep20_orderbook, pair_bep20_erc20_orderbook, pair_erc20_bep20_orderbook]
        orderbook = {"bids" : [], "asks": []}
        for orderbook_permutation in orderbooks_list:
            # case when there is no such ticker in coins file
            if next(iter(orderbook_permutation)) == "error":
                orderbook_permutation = {"bids" : [], "asks": []}
            orderbook["bids"] += orderbook_permutation["bids"]
            orderbook["asks"] += orderbook_permutation["asks"]
    else:
        orderbook = get_mm2_orderbook_for_pair(pair)
        if next(iter(orderbook)) == "error":
            orderbook = {"bids" : [], "asks": []}
    bids_converted_list = []
    asks_converted_list = []
    try:
        for bid in orderbook["bids"]:
            converted_bid = []
            converted_bid.append(bid["price"])
            converted_bid.append(bid["base_max_volume"])
            bids_converted_list.append(converted_bid)
    except KeyError:
        pass
    try:
        for ask in orderbook["asks"]:
            converted_ask = []
            converted_ask.append(ask["price"])
            converted_ask.append(ask["base_max_volume"])
            asks_converted_list.append(converted_ask)
    except KeyError:
        pass
    return bids_converted_list, asks_converted_list

# SUMMARY Endpoint
# tuple, string -> dictionary
# Receiving tuple with base and rel as an argument and producing CMC summary endpoint data, requires mm2 rpc password and sql db connection
def summary_for_pair(pair, path_to_db):
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_coursor = conn.cursor()
    pair_summary = OrderedDict()
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    swaps_for_pair_24h = get_swaps_since_timestamp_for_pair(sql_coursor, pair, timestamp_24h_ago)
    pair_24h_volumes_and_prices = count_volumes_and_prices(swaps_for_pair_24h)

    pair_summary["trading_pair"] = pair[0] + "_" + pair[1]
    pair_summary["last_price"] = "{:.10f}".format(pair_24h_volumes_and_prices["last_price"])
    orderbook = get_mm2_orderbook_for_pair(pair)
    pair_summary["lowest_ask"] = "{:.10f}".format(Decimal(find_lowest_ask(orderbook)))
    pair_summary["highest_bid"] = "{:.10f}".format(Decimal(find_highest_bid(orderbook)))
    pair_summary["base_currency"] = pair[0]
    pair_summary["base_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["base_volume"])
    pair_summary["quote_currency"] = pair[1]
    pair_summary["quote_volume"] = "{:.10f}".format(pair_24h_volumes_and_prices["quote_volume"])
    pair_summary["price_change_percent_24h"] = "{:.10f}".format(pair_24h_volumes_and_prices["price_change_percent_24h"])
    pair_summary["highest_price_24h"] = "{:.10f}".format(pair_24h_volumes_and_prices["highest_price_24h"])
    pair_summary["lowest_price_24h"] = "{:.10f}".format(pair_24h_volumes_and_prices["lowest_price_24h"])

    conn.close()
    return pair_summary

def market_summary(pair):
    market_summary = OrderedDict({
        "id": pair[0] + "_" + pair[1],
        "type": "spot",
        "base": pair[0],
        "quote": pair[1]
    })
    return market_summary

# TICKER Endpoint
def ticker_for_pair(pair, path_to_db):
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_coursor = conn.cursor()
    pair_ticker = OrderedDict()
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    swaps_for_pair_24h = get_swaps_since_timestamp_for_pair(sql_coursor, pair, timestamp_24h_ago)
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
    orderbook_data["timestamp"] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    # TODO: maybe it'll be asked on API side? quite tricky to convert strings and sort the
    orderbook_data["bids"] = get_and_parse_orderbook(pair)[0]
    orderbook_data["asks"] = get_and_parse_orderbook(pair)[1]
    return orderbook_data

# Trades Endpoint
def trades_for_pair(pair, path_to_db, since):
    pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return {"error": "not valid pair"}
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_coursor = conn.cursor()
    # since is uuid of swap from which we should cut the data so we have to find timestamp of this swap
    if since == "":
        cut_timestamp = 0
    else:
        t = (since,)
        sql_coursor.execute("SELECT * FROM stats_swaps WHERE is_success=1 AND uuid=?;", t)
        swap_status_for_uuid = [dict(row) for row in sql_coursor.fetchall()]
        try:
            cut_timestamp = swap_status_for_uuid[0]["started_at"]
        except Exception as e:
            return {"error": "no swaps after provided slice id"}
    swaps_for_pair = get_swaps_since_timestamp_for_pair(sql_coursor, pair, cut_timestamp)
    trades_info = []
    for swap_status in swaps_for_pair:
        trade_info = OrderedDict()
        trade_info["id"] = swap_status["uuid"]
        trade_info["price"] = str("{:.10f}".format(Decimal(swap_status["taker_amount"]) / Decimal(swap_status["maker_amount"])))
        trade_info["amount"] = str(swap_status["maker_amount"])
        trade_info["amount_quote"] = str(swap_status["taker_amount"])
        trade_info["timestamp"] = datetime.utcfromtimestamp(int(swap_status["started_at"])).strftime('%Y-%m-%dT%H:%M:%SZ')
        trade_info["side"] = swap_status["trade_type"]
        trade_info["raw"] = swap_status
        trades_info.append(trade_info)
    conn.close()
    return trades_info

# Data for atomicdex.io website
def atomicdex_info(path_to_db):
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    timestamp_30d_ago = int((datetime.now() - timedelta(30)).strftime("%s"))
    conn = sqlite3.connect(path_to_db)
    sql_coursor = conn.cursor()
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE is_success=1;")
    swaps_all_time = len(sql_coursor.fetchall())
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND is_success=1;", (timestamp_24h_ago,))
    swaps_24h = len(sql_coursor.fetchall())
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND is_success=1;", (timestamp_30d_ago,))
    swaps_30d = len(sql_coursor.fetchall())
    conn.close()
    return {
        "swaps_all_time" : swaps_all_time,
        "swaps_30d" : swaps_30d,
        "swaps_24h" : swaps_24h
    }
