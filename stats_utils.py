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
    sql_cursor = conn.cursor()
    # Without a time limit, this is returning too many pairs to send a response before timeout.
    timestamp_7d_ago = int((datetime.now() - timedelta(7)).strftime("%s"))
    sql_cursor.execute("SELECT DISTINCT maker_coin_ticker, taker_coin_ticker FROM stats_swaps WHERE started_at > ?;", (timestamp_7d_ago,))
    available_pairs = sql_cursor.fetchall()
    print(f"{len(available_pairs)} distinct maker/taker pairs for last 7 days")
    sorted_available_pairs = []
    for pair in available_pairs:
       sorted_available_pairs.append(tuple(sorted(pair)))
    conn.close()
    return list(set(sorted_available_pairs))


# tuple, integer -> list (with swap status dicts)
# select from DB swap statuses for desired pair with timestamps > than provided
def get_swaps_since_timestamp_for_pair(sql_cursor, pair, timestamp):
    t = (timestamp,pair[0],pair[1],)
    sql_cursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND maker_coin_ticker=? AND taker_coin_ticker=? AND is_success=1;", t)
    swap_statuses_a_b = [dict(row) for row in sql_cursor.fetchall()]
    for swap in swap_statuses_a_b:
        swap["trade_type"] = "buy"
    sql_cursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND taker_coin_ticker=? AND maker_coin_ticker=? AND is_success=1;", t)
    swap_statuses_b_a = [dict(row) for row in sql_cursor.fetchall()]
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
    try:
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
    except Exception as e:
        print(f"Error in [count_volumes_and_prices]: {e}")
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
        print(f"Error in [get_mm2_orderbook_for_pair]: {e}")


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
    sql_cursor = conn.cursor()
    pair_summary = OrderedDict()
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    swaps_for_pair_24h = get_swaps_since_timestamp_for_pair(sql_cursor, pair, timestamp_24h_ago)
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
    # liqudity in USD
    try:
        base_liqudity_in_coins = orderbook["total_asks_base_vol"]
        rel_liqudity_in_coins  = orderbook["total_bids_rel_vol"]
        with open('gecko_cache.json', 'r') as json_file:
            gecko_cached_data = json.load(json_file)
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

        pair_summary["pair_liqudity_usd"] = base_liqudity_in_usd + rel_liqudity_in_usd
    except KeyError:
        pair_summary["pair_liqudity_usd"] = 0
    conn.close()
    return pair_summary



# Ticker Endpoint
def ticker_for_pair(pair, path_to_db):
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_cursor = conn.cursor()
    pair_ticker = OrderedDict()
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    swaps_for_pair_24h = get_swaps_since_timestamp_for_pair(sql_cursor, pair, timestamp_24h_ago)
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
def trades_for_pair(pair, path_to_db):
    pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return {"error": "not valid pair"}
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_cursor = conn.cursor()
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    swaps_for_pair_24h = get_swaps_since_timestamp_for_pair(sql_cursor, pair, timestamp_24h_ago)
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
    conn.close()
    return trades_info

# Last Trade Price
def get_last_price_for_pair(pair, path_to_db):
    conn = sqlite3.connect(path_to_db)
    conn.row_factory = sqlite3.Row
    sql_cursor = conn.cursor()

    sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{pair[0]}' and taker_coin_ticker='{pair[1]}' AND  is_success=1 ORDER BY started_at LIMIT 1;"
    sql_cursor.execute(sql)
    resp = sql_cursor.fetchone()
    try:
        swap_price = Decimal(resp["taker_amount"]) / Decimal(resp["maker_amount"])
        swap_time = resp["started_at"]
    except:
        swap_price = None

    sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{pair[1]}' and taker_coin_ticker='{pair[0]}' AND  is_success=1 ORDER BY started_at LIMIT 1;"
    sql_cursor.execute(sql)
    resp2 = sql_cursor.fetchone()
    try: 
        swap_price2 = 1/(Decimal(resp2["taker_amount"]) / Decimal(resp2["maker_amount"]))
        swap_time2 = resp2["started_at"]
    except: 
        swap_price2 = None
    if swap_price and swap_price2:
        if swap_time > swap_time2:
            return swap_price
    elif swap_price: swap_price = swap_price
    elif swap_price2: swap_price = swap_price2
    else: swap_price = 0
    return swap_price

def get_data_from_gecko():
    coin_ids_dict = {}
    with open("coins_config.json", "r") as coins_json:
        json_data = json.load(coins_json)
        for coin in json_data:
            try:
                coin_ids_dict[coin] = {}
                coin_ids_dict[coin]["coingecko_id"] = json_data[coin]["coingecko_id"]
            except KeyError as e:
                 coin_ids_dict[coin]["coingecko_id"] = "na"
    coin_ids = []
    for coin in coin_ids_dict:
        coin_id = coin_ids_dict[coin]["coingecko_id"]
        if coin_id not in ["na", "test-coin", ""]:
            coin_ids.append(coin_id)
    coin_ids = list(set(coin_ids))
    coin_ids.sort()
    coin_ids = ",".join(coin_ids)
    r = ""
    try:
        url = f'https://api.coingecko.com/api/v3/simple/price?ids={coin_ids}&vs_currencies=usd'
        print(url)
        gecko_data = requests.get(url).json()
    except Exception as e:
        return {"error": "https://api.coingecko.com/api/v3/simple/price?ids= is not available"}
    try:
        for coin in coin_ids_dict:
            coin_id = coin_ids_dict[coin]["coingecko_id"]
            if coin_id not in ["na", "test-coin", ""] and coin_id in gecko_data:
                if "usd" in gecko_data[coin_id]:
                    coin_ids_dict[coin]["usd_price"] = gecko_data[coin_id]["usd"]
            else:
                coin_ids_dict[coin]["usd_price"] = 0
    except Exception as e:
        print(f'Error in [get_data_from_gecko]: {e}')
    return coin_ids_dict

# Data for atomicdex.io website
def atomicdex_info(path_to_db):
    timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
    timestamp_30d_ago = int((datetime.now() - timedelta(30)).strftime("%s"))
    conn = sqlite3.connect(path_to_db)
    sql_cursor = conn.cursor()
    sql_cursor.execute("SELECT * FROM stats_swaps WHERE is_success=1;")
    swaps_all_time = len(sql_cursor.fetchall())
    sql_cursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND is_success=1;", (timestamp_24h_ago,))
    swaps_24h = len(sql_cursor.fetchall())
    sql_cursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND is_success=1;", (timestamp_30d_ago,))
    swaps_30d = len(sql_cursor.fetchall())
    available_pairs = get_availiable_pairs(path_to_db)
    summary_data = []
    try:
        for pair in available_pairs:
            summary_data.append(summary_for_pair(pair, path_to_db))
        current_liqudity = 0
        for pair_summary in summary_data:
            current_liqudity += pair_summary["pair_liqudity_usd"]
    except Exception as e:
        print(f"Error in [atomicdex_info]: {e}")
    conn.close()
    return {
        "swaps_all_time" : swaps_all_time,
        "swaps_30d" : swaps_30d,
        "swaps_24h" : swaps_24h,
        "current_liqudity" : current_liqudity
    }
