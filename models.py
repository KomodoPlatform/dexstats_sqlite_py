#!/usr/bin/env python3
import json
import sqlite3
import requests
from collections import OrderedDict
from decimal import Decimal
from datetime import datetime, timedelta
from logger import logger
import const


class CacheGet:
    def __init__(self):
        pass

    def adex(self):
        with open("cache/adex_cache.json", "r") as f:
            return json.load(f)

    def adex_fortnight(self):
        with open("cache/adex_fortnight_cache.json", "r") as f:
            return json.load(f)

    def coins_config(self):
        with open("cache/coins_config.json", "r") as f:
            return json.load(f)

    def coins(self):
        with open("mm2/coins", "r") as f:
            return json.load(f)

    def gecko(self, path="cache/gecko_cache.json"):
        with open(path, "r") as f:
            return json.load(f)

    def summary(self):
        with open("cache/summary_cache.json", "r") as f:
            return json.load(f)

    def ticker(self):
        with open("cache/ticker_cache.json", "r") as f:
            return json.load(f)


class CacheLogic:
    def __init__(self, db_path: str = const.MM2_DB_PATH):
        self.db_path = db_path
        self.templates = Templates()
        self.utils = Utils()
        self.swaps = Swaps()
        self.gecko = Gecko()

    def calc_ticker_cache(self, DB=None):
        try:
            if not DB:
                DB = SqliteDB(self.db_path)
            pairs = DB.get_pairs()
            logger.info(f"Calculating ticker cache for {len(pairs)} pairs")
            return [Pair(i).ticker(self.db_path) for i in pairs]
        except Exception as e:
            logger.error(f"Error in [CacheLogic.calc_ticker_cache]: {e}")
            return []

    # Data for atomicdex.io website
    def atomicdex_info(self, days: int = 1, DB=None):
        try:
            if not DB:
                DB = SqliteDB(self.db_path)
            DB.conn.row_factory = sqlite3.Row
            DB.sql_cursor = DB.conn.cursor()
            summary = DB.get_adex_summary()
            pairs = DB.get_pairs()
            summary_data = [Pair(i).summary(days) for i in pairs]
            current_liquidity = self.utils.get_liquidity(summary_data)
            summary.update({"current_liquidity": round(current_liquidity, 2)})
        except Exception as e:
            logger.error(f"Error in [CacheLogic.atomicdex_info]: {e}")
            return None
        return summary

    def atomicdex_timespan_info(self, days: int = 14, DB=None) -> dict:
        """
        Summary data for a timespan (e.g. 14 days)
        """
        try:
            if not DB:
                DB = SqliteDB(self.db_path)
            DB.conn.row_factory = sqlite3.Row
            DB.sql_cursor = DB.conn.cursor()
            summary = {}
            pairs = DB.get_pairs(days)
            swaps = DB.get_timespan_swaps(days)
            # Total Value traded
            summary_data = [Pair(i).summary(days) for i in pairs]
            logger.info(f"Calculating summary data for {len(pairs)} pairs")

            summary.update(
                {
                    "days": days,
                    "swaps_count": len(swaps),
                    "swaps_value": self.utils.get_value(summary_data),
                    "current_liquidity": self.utils.get_liquidity(summary_data),
                    "top_pairs": self.swaps.get_top_pairs(pairs, days),
                }
            )

        except Exception as e:
            logger.error(f"Error in [CacheLogic.atomicdex_timespan_info]: {e}")
            return None
        return summary


class CacheUpdate:
    def __init__(self, db_path=const.MM2_DB_PATH):
        self.db_path = db_path
        self.logic = CacheLogic(db_path=db_path)
        self.gecko = Gecko()

    def adex(self):
        data = self.logic.atomicdex_info()
        if data:
            with open("cache/adex_cache.json", "w+") as cache_file:
                json.dump(data, cache_file, indent=4)
                logger.info("Updated cache/adex_cache.json")
                return {"result": "Updated cache/adex_cache.json"}
        else:
            return {"error": "Failed to update cache/adex_cache.json"}

    def adex_fortnight(self):
        data = self.logic.atomicdex_timespan_info()
        if data:
            with open("cache/adex_fortnight_cache.json", "w+") as cache_file:
                json.dump(data, cache_file, indent=4)
                logger.info("Updated cache/adex_fortnight_cache.json")
                return {"result": "Updated cache/adex_fortnight_cache.json"}
        else:
            return {"error": "Failed to update cache/adex_fortnight_cache.json"}

    def coins_config(self) -> dict:
        data = requests.get(const.COINS_CONFIG_URL).json()
        if isinstance(data, (list, dict)):
            with open("cache/coins_config.json", "w+") as f:
                json.dump(data, f, indent=4)
                logger.info("Updated cache/coins_config.json")
                return {"result": "Updated cache/coins_config.json"}
        else:
            return {"error": "Failed to update cache/coins_config.json"}

    def coins(self) -> dict:
        data = requests.get(const.COINS_URL).json()
        if isinstance(data, (list, dict)):
            with open("mm2/coins", "w+") as f:
                json.dump(data, f, indent=4)
                logger.info("Updated mm2/coins")
                return {"result": "Updated mm2/coins"}
        else:
            return {"error": "Failed to update mm2/coins"}

    def gecko_data(self) -> dict:
        data = self.gecko.get_data_from_gecko()
        if "KMD" in data:
            with open("cache/gecko_cache.json", "w+") as f:
                json.dump(data, f, indent=4)
                logger.info("Updated cache/gecko_cache.json")
                return {"result": "Updated cache/gecko_cache.json"}
        else:
            return {"error": "Failed to update cache/gecko_cache.json"}

    def summary(self, days=1):
        # Takes around 1:20 minute to run with 300 pairs
        DB = SqliteDB(self.db_path)
        pairs = DB.get_pairs()
        logger.debug(f"Got {len(pairs)} pairs from DB")
        data = [Pair(i).summary(days) for i in pairs]
        logger.debug(f"Calculated summary for {len(data)} pairs")
        with open("cache/summary_cache.json", "w+") as f:
            json.dump(data, f, indent=4)
            logger.info("Updated cache/summary_cache.json")
        DB.close()
        return {"result": "Updated cache/summary_cache.json"}

    def ticker(self):
        data = self.logic.calc_ticker_cache()
        # Takes around 1:20 minute to run with 300 pairs
        with open("cache/ticker_cache.json", "w+") as f:
            json.dump(data, f, indent=4)
            logger.info("Updated cache/ticker_cache.json")
        return {"result": "Updated cache/ticker_cache.json"}


class Gecko:
    def __init__(self):
        self.cache_get = CacheGet()
        self.utils = Utils()
        pass

    def get_gecko_info_template(self, coin_id=""):
        return {"usd_market_cap": 0, "usd_price": 0, "coingecko_id": coin_id}

    def get_gecko_coin_ids_list(self, coins_config: dict) -> list:
        coin_ids = list(
            set(
                [
                    coins_config[i]["coingecko_id"]
                    for i in coins_config
                    if coins_config[i]["coingecko_id"] not in ["na", "test-coin", ""]
                ]
            )
        )
        coin_ids.sort()
        return coin_ids

    def get_gecko_info_dict(self, coins_config: dict) -> dict:
        coins_info = {}
        for coin in coins_config:
            coins_info.update({coin: self.get_gecko_info_template()})
        for coin in coins_config:
            coin_id = coins_config[coin]["coingecko_id"]
            native_coin = coin.split("-")[0]
            if native_coin not in coins_info:
                coins_info.update({native_coin: self.get_gecko_info_template(coin_id)})
        return coins_info

    def get_gecko_coins_dict(self, coins_config: dict, coin_ids: list) -> dict:
        gecko_coins = {}
        for coin_id in coin_ids:
            gecko_coins.update({coin_id: []})

        for coin in coins_config:
            coin_id = coins_config[coin]["coingecko_id"]
            if coin_id not in ["na", "test-coin", ""]:
                gecko_coins[coin_id].append(coin)
                native_coin = coin.split("-")[0]
                if native_coin not in gecko_coins[coin_id]:
                    gecko_coins[coin_id].append(native_coin)

        return gecko_coins

    def get_data_from_gecko(self):
        param_limit = 200
        coins_config = self.cache_get.coins_config()
        coin_ids = self.get_gecko_coin_ids_list(coins_config)
        coins_info = self.get_gecko_info_dict(coins_config)
        gecko_coins = self.get_gecko_coins_dict(coins_config, coin_ids)
        coin_id_chunks = list(self.utils.get_chunks(coin_ids, param_limit))
        for chunk in coin_id_chunks:
            chunk_ids = ",".join(chunk)
            try:
                params = f"ids={chunk_ids}&vs_currencies=usd&include_market_cap=true"
                url = f"https://api.coingecko.com/api/v3/simple/price?{params}"
                gecko_data = requests.get(url).json()
            except Exception as e:
                error = {"error": f"{url} is not available"}
                logger.error(f"Error in [get_data_from_gecko]: {e} ({error})")
                return error
            try:
                for coin_id in gecko_data:
                    try:
                        coins = gecko_coins[coin_id]
                        for coin in coins:
                            if "usd" in gecko_data[coin_id]:
                                coins_info[coin].update(
                                    {"usd_price": gecko_data[coin_id]["usd"]}
                                )
                            if "usd_market_cap" in gecko_data[coin_id]:
                                coins_info[coin].update(
                                    {
                                        "usd_market_cap": gecko_data[coin_id][
                                            "usd_market_cap"
                                        ]
                                    }
                                )
                    except Exception as e:
                        error = (
                            f"CoinGecko ID request/response mismatch [{coin_id}] [{e}]"
                        )
                        logger.warning(error)
                        pass

            except Exception as e:
                logger.error(f"Error in [get_data_from_gecko]: {e}")
        return coins_info

    def get_gecko_usd_price(self, coin: str, gecko_cached_data: dict = None) -> float:
        if not gecko_cached_data:
            gecko_cached_data = SqliteDB(const.MM2_DB_PATH).gecko_data
        try:
            return gecko_cached_data[coin]["usd_price"]
        except KeyError:
            return 0


class Orderbook:
    def __init__(self, pair):
        self.pair = pair
        self.utils = Utils()
        self.dexapi = DexAPI()
        pass

    def for_pair(self, endpoint=False):
        try:
            orderbook_data = OrderedDict()
            orderbook_data["timestamp"] = "{}".format(
                int(datetime.now().strftime("%s"))
            )
            data = self.get_and_parse(endpoint)
            orderbook_data["bids"] = data["bids"]
            orderbook_data["asks"] = data["asks"]
            orderbook_data["asks"] = data["asks"]
            orderbook_data["total_asks_base_vol"] = data["total_asks_base_vol"]
            orderbook_data["total_bids_rel_vol"] = data["total_bids_rel_vol"]
            return orderbook_data
        except Exception as e:
            logger.error(f"Error in [Orderbook.for_pair]: {e}")
            return []

    def related_orderbooks_list(self):
        coin_a = self.pair.as_tuple[0]
        coin_b = self.pair.as_tuple[1]
        coins_a = self.utils.get_related_coins(coin_a)
        coins_b = self.utils.get_related_coins(coin_b)
        orderbooks_list = []
        for coin_a in coins_a:
            for coin_b in coins_b:
                if coin_a != coin_b:
                    alt_pair = (coin_a, coin_b)
                    orderbook = self.dexapi.orderbook(alt_pair)
                    orderbooks_list.append(orderbook)
        return orderbooks_list

    def get_and_parse(self, endpoint=False, orderbooks_list=None):
        try:
            if orderbooks_list is None:
                orderbooks_list = self.related_orderbooks_list()
            orderbook = {
                "pair": self.pair.as_str,
                "bids": [],
                "asks": [],
                "total_asks_base_vol": 0,
                "total_bids_rel_vol": 0,
            }
            for i in orderbooks_list:
                # case when there is no such ticker in coins file
                if next(iter(i)) == "error":
                    i = {
                        "bids": [],
                        "asks": [],
                        "total_asks_base_vol": 0,
                        "total_bids_rel_vol": 0,
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
            return {
                "bids": [],
                "asks": [],
                "total_asks_base_vol": 0,
                "total_bids_rel_vol": 0,
            }

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
                        "base_max_volume": bid["base_max_volume"],
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
                        "base_max_volume": ask["base_max_volume"],
                    }
                asks_converted_list.append(converted_ask)
        except KeyError as e:
            logger.warning(f"Error in [get_and_parse_orderbook]: {e}")
            pass
        orderbook["bids"] = bids_converted_list
        orderbook["asks"] = asks_converted_list
        return orderbook

    def find_lowest_ask(self, orderbook: list) -> str:
        """Returns lowest ask from provided orderbook"""
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

    def find_highest_bid(self, orderbook: list) -> str:
        """Returns highest bid from provided orderbook"""
        highest_bid = {"price": "0"}
        try:
            for bid in orderbook["bids"]:
                if Decimal(bid["price"]) > Decimal(highest_bid["price"]):
                    highest_bid = bid
        except KeyError:
            pass
        return "{:.10f}".format(Decimal(highest_bid["price"]))


class Pair:
    """Allows for referencing pairs as a string or tuple."""

    def __init__(self, pair, db_path=const.MM2_DB_PATH):
        try:
            self.db_path = db_path
            if isinstance(pair, tuple):
                self.as_tuple = pair
            elif isinstance(pair, list):
                self.as_tuple = tuple(pair)
            elif isinstance(pair, str):
                self.as_tuple = tuple(map(str, pair.split("_")))

            if len(self.as_tuple) != 2:
                self.as_str = {"error": "not valid pair"}
                self.as_str = {"error": "not valid pair"}
                self.base = {"error": "not valid pair"}
                self.quote = {"error": "not valid pair"}
            else:
                self.as_str = self.as_tuple[0] + "_" + self.as_tuple[1]
                self.base = self.as_tuple[0]
                self.quote = self.as_tuple[1]

            self.swaps = Swaps(pair=self, db_path=self.db_path)
            self.orderbook = Orderbook(pair=self)
            self.gecko = Gecko()
            self.utils = Utils()
            self.templates = Templates()
        except Exception as e:
            logger.warning(f"Error in [Pair]: {e}")

    def trades(self, DB=None):
        """Returns trades for this pair."""
        try:
            if not DB:
                DB = SqliteDB(self.db_path)
            swaps_for_pair = DB.get_swaps_for_pair(self.as_tuple)
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
        except Exception as e:
            logger.warning(f"Error in [Pair.trades]: {e}")
            return []

    def ticker(self, days=1, DB=None) -> dict:
        try:
            if not DB:
                DB = SqliteDB(self.db_path)
            DB.conn.row_factory = sqlite3.Row
            DB.sql_cursor = DB.conn.cursor()
            pair = self.as_tuple
            pair_ticker = OrderedDict()
            swaps_for_pair = DB.get_swaps_for_pair(pair)
            data = self.swaps.get_volumes_and_prices(swaps_for_pair, days)
            pair_ticker[pair[0] + "_" + pair[1]] = OrderedDict()
            last_price = "{:.10f}".format(data["last_price"])
            pair_ticker[pair[0] + "_" + pair[1]]["last_price"] = last_price
            quote_volume = "{:.10f}".format(data["quote_volume"])
            pair_ticker[pair[0] + "_" + pair[1]]["quote_volume"] = quote_volume
            base_volume = "{:.10f}".format(data["base_volume"])
            pair_ticker[pair[0] + "_" + pair[1]]["base_volume"] = base_volume
            pair_ticker[pair[0] + "_" + pair[1]]["isFrozen"] = "0"
            return pair_ticker
        except Exception as e:
            logger.warning(f"Error in [Pair.ticker]: {e}")
            return {}

    def summary(self, days: int = 1, orderbook: dict = None, DB=None) -> dict:
        """Calculates CMC summary endpoint data for a pair"""
        try:
            if not DB:
                DB = SqliteDB(self.db_path)
            base = self.base
            quote = self.quote
            suffix = self.utils.get_suffix(days)
            data = self.templates.pair_summary(base, quote)
            timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
            swaps_for_pair = DB.get_swaps_for_pair(self.as_tuple, timestamp)
            gecko_cached_data = DB.gecko_data
            data["pair_swaps_count"] = len(swaps_for_pair)
            volumes_and_prices = self.swaps.get_volumes_and_prices(swaps_for_pair, days)
            for i in ["last_price", "base_volume", "quote_volume"]:
                value = "{:.10f}".format(volumes_and_prices[i])
                data[i] = value

            for i in ["price_change_percent", "highest_price", "lowest_price"]:
                value = "{:.10f}".format(volumes_and_prices[f"{i}_{suffix}"])
                data[f"{i}_{suffix}"] = value

        except Exception as e:
            logger.error(f"Error while getting summary for pair {days} {type(days)}")
            logger.error(f"Error while getting summary for pair {self.as_str}: {e}")

        try:
            # Liquidity
            base_price = self.gecko.get_gecko_usd_price(base, gecko_cached_data)
            base_volume = volumes_and_prices["base_volume"]
            quote_price = self.gecko.get_gecko_usd_price(quote, gecko_cached_data)
            quote_volume = volumes_and_prices["quote_volume"]

            data["base_price_usd"] = base_price
            data["rel_price_usd"] = quote_price
            data["base_volume_coins"] = base_volume
            data["rel_volume_coins"] = quote_volume
        except Exception as e:
            logger.error(f"Error in [Pair.summary] for {self.as_str}: {e}")

        try:
            if not orderbook:
                orderbook = self.orderbook.for_pair(endpoint=False)
            data["lowest_ask"] = self.orderbook.find_lowest_ask(orderbook)
            data["highest_bid"] = self.orderbook.find_highest_bid(orderbook)
            data["base_liquidity_coins"] = orderbook["total_asks_base_vol"]
            data["rel_liquidity_coins"] = orderbook["total_bids_rel_vol"]
        except Exception as e:
            logger.error(f"Error in [Pair.summary] for {self.as_str}: {e}")

        try:
            data["base_liquidity_usd"] = self.utils.safe_float(
                Decimal(base_price) * Decimal(data["base_liquidity_coins"]), 2
            )
        except KeyError:
            pass

        try:
            data["rel_liquidity_usd"] = self.utils.safe_float(
                Decimal(quote_price) * Decimal(data["rel_liquidity_coins"]), 2
            )
        except KeyError as e:
            logger.warning(f"Error in [Pair.summary] for {self.as_str}: {e}")
            pass
        data["pair_liquidity_usd"] = (
            data["rel_liquidity_usd"] + data["base_liquidity_usd"]
        )

        # Value traded in USD
        try:
            data["base_trade_value_usd"] = self.utils.safe_float(
                Decimal(base_price) * Decimal(base_volume), 2
            )
        except KeyError:
            pass
        try:
            data["rel_trade_value_usd"] = self.utils.safe_float(
                Decimal(quote_price) * Decimal(quote_volume), 2
            )
        except KeyError as e:
            logger.warning(f"Error in [Pair.summary] for {self.as_str}: {e}")
            pass
        data["pair_trade_value_usd"] = (
            data["base_trade_value_usd"] + data["rel_trade_value_usd"]
        )
        return data


class SqliteDB:
    def __init__(
        self, path_to_db, dict_format=False, cache_path="cache/gecko_cache.json"
    ):
        self.conn = sqlite3.connect(path_to_db)
        if dict_format:
            self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        with open(cache_path, "r") as json_file:
            self.gecko_data = json.load(json_file)

    def close(self):
        self.conn.close()

    def get_pairs(self, days: int = 7) -> list:
        """
        Returns a list of pairs (as a list of tuples) with at least one
        successful swap in the last 'x' days.
        """
        timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
        sql = f"SELECT DISTINCT maker_coin_ticker, taker_coin_ticker FROM stats_swaps \
                WHERE started_at > {timestamp} AND is_success=1;"
        self.sql_cursor.execute(sql)
        pairs = self.sql_cursor.fetchall()
        sorted_pairs = [tuple(sorted(pair)) for pair in pairs]
        pairs = list(set(sorted_pairs))
        logger.debug(f"{len(pairs)} distinct pairs for last {days} days")
        adjusted = []
        for pair in pairs:
            if pair[0] in self.gecko_data:
                if pair[1] in self.gecko_data:
                    if (
                        self.gecko_data[pair[1]]["usd_market_cap"]
                        < self.gecko_data[pair[0]]["usd_market_cap"]
                    ):
                        pair = (pair[1], pair[0])
                else:
                    pair = (pair[1], pair[0])
            adjusted.append(pair)
        return adjusted

    def get_swaps_for_pair(self, pair: tuple, timestamp: int = -1) -> list:
        """
        Returns a list of swaps for a given pair since a timestamp.
        If no timestamp is given, returns all swaps for the pair.
        Includes both buy and sell swaps (e.g. KMD/BTC & BTC/KMD)
        """
        try:
            if timestamp == -1:
                timestamp = int((datetime.now() - timedelta(days=1)).strftime("%s"))
            t = (
                timestamp,
                pair[0],
                pair[1],
            )
            sql = "SELECT * FROM stats_swaps WHERE started_at > ? \
                    AND maker_coin_ticker=? \
                    AND taker_coin_ticker=? \
                    AND is_success=1;"
            self.conn.row_factory = sqlite3.Row
            self.sql_cursor = self.conn.cursor()
            self.sql_cursor.execute(
                sql,
                t,
            )
            data = self.sql_cursor.fetchall()
            swaps_for_pair_a_b = [dict(row) for row in data]

            for swap in swaps_for_pair_a_b:
                swap["trade_type"] = "buy"
            sql = "SELECT * FROM stats_swaps WHERE started_at > ? \
                    AND taker_coin_ticker=? \
                    AND maker_coin_ticker=? \
                    AND is_success=1;"
            self.sql_cursor.execute(
                sql,
                t,
            )
            data = self.sql_cursor.fetchall()
            swaps_for_pair_b_a = [dict(row) for row in data]
            for swap in swaps_for_pair_b_a:
                temp_maker_amount = swap["maker_amount"]
                swap["maker_amount"] = swap["taker_amount"]
                swap["taker_amount"] = temp_maker_amount
                swap["trade_type"] = "sell"
            swaps_for_pair = swaps_for_pair_a_b + swaps_for_pair_b_a
            return swaps_for_pair
        except Exception as e:
            logger.warning(f"Error in [get_swaps_for_pair]: {e}")
            return []

    def get_last_price_for_pair(self, pair: str) -> float:
        """
        Takes a pair in the format `KMD_BTC` and returns the
        last trade price for that pair. Response scans both
        buy and sell swaps (e.g. KMD/BTC and BTC/KMD)
        """
        pair = Pair(pair)
        coin_a = pair.base
        coin_b = pair.quote
        self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()

        try:
            swap_price = None
            swap_time = None
            sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{coin_a}' \
                    AND taker_coin_ticker='{coin_b}' AND is_success=1 \
                    ORDER BY started_at DESC LIMIT 1;"
            self.sql_cursor.execute(sql)
            resp = self.sql_cursor.fetchone()
            if resp is not None:
                swap_price = Decimal(resp["taker_amount"]) / Decimal(resp["maker_amount"])
                swap_time = resp["started_at"]
        except Exception as e:
            logger.warning(f"Error getting swap_price for {pair.as_str}: {e}")

        try:
            swap_price2 = None
            swap_time2 = None
            sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{coin_b}' \
                    AND taker_coin_ticker='{coin_a}' AND is_success=1 \
                    ORDER BY started_at DESC LIMIT 1;"
            self.sql_cursor.execute(sql)
            resp2 = self.sql_cursor.fetchone()
            if resp2 is not None:
                swap_price2 = Decimal(resp2["maker_amount"]) / Decimal(
                    resp2["taker_amount"]
                )
                swap_time2 = resp2["started_at"]
        except Exception as e:
            logger.warning(f"Error getting swap_price2 for {pair.as_str}: {e}")

        if swap_price and swap_price2:
            if swap_time > swap_time2:
                price = swap_price
            else:
                price = swap_price2
        elif swap_price:
            price = swap_price
        elif swap_price2:
            price = swap_price2
        else:
            price = 0
        return price

    def get_adex_summary(self) -> dict:
        """
        Returns a dict of swap counts for the last 24 hours,
        last 30 days and all time
        """
        self.sql_cursor.execute("SELECT * FROM stats_swaps WHERE is_success=1;")
        return {
            "swaps_all_time": len(self.sql_cursor.fetchall()),
            "swaps_24h": len(self.get_timespan_swaps(1)),
            "swaps_30d": len(self.get_timespan_swaps(30)),
        }

    def get_timespan_swaps(self, days: int = 1) -> list:
        """
        Returns a list of swaps for the last 'x' days
        """
        timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
        self.sql_cursor.execute(
            "SELECT * FROM stats_swaps WHERE started_at > ? AND is_success=1;",
            (timestamp,),
        )
        timespan_swaps = self.sql_cursor.fetchall()
        return timespan_swaps


class Swaps:
    def __init__(self, pair=None, db_path=const.MM2_DB_PATH):
        self.pair = pair
        self.db_path = db_path
        self.utils = Utils()
        self.templates = Templates()

    def get_swap_prices(self, swaps_for_pair: list) -> dict:
        swap_prices = {}
        for swap in swaps_for_pair:
            swap_price = Decimal(swap["taker_amount"]) / Decimal(swap["maker_amount"])
            swap_prices[swap["started_at"]] = swap_price
        return swap_prices

    def get_swaps_volumes(self, swaps_for_pair: list) -> dict:
        try:
            base_volume = 0
            quote_volume = 0
            for swap in swaps_for_pair:
                base_volume += swap["maker_amount"]
                quote_volume += swap["taker_amount"]
            return [base_volume, quote_volume]
        except Exception as e:
            logger.error(f"Error in [get_swaps_volumes]: {e}")
            return [0, 0]

    def get_volumes_and_prices(
        self,
        swaps_for_pair: list,
        days: int = 1,
    ) -> dict:
        """
        Iterates over list of swaps to get data for CMC summary endpoint
        """
        try:
            suffix = self.utils.get_suffix(days)
            data = self.templates.volumes_and_prices(suffix)
            swap_prices = self.get_swap_prices(swaps_for_pair)
            swaps_volumes = self.get_swaps_volumes(swaps_for_pair)
            data["base_volume"] = swaps_volumes[0]
            data["quote_volume"] = swaps_volumes[1]
        except Exception as e:
            logger.error(f"Error in [Swaps.get_volumes_and_prices]: {e}")

        try:
            if len(swap_prices) > 0:
                highest_price = max(swap_prices.values())
                lowest_price = min(swap_prices.values())
                last_price = swap_prices[max(swap_prices.keys())]
                oldest_price = swap_prices[min(swap_prices.keys())]
                pct_change = Decimal(last_price - oldest_price) / Decimal(100)
                data[f"highest_price_{suffix}"] = highest_price
                data[f"lowest_price_{suffix}"] = lowest_price
                data["last_price"] = last_price
                data[f"price_change_percent_{suffix}"] = pct_change
            else:
                DB = SqliteDB(self.db_path)
                if self.pair:
                    data["last_price"] = DB.get_last_price_for_pair(self.pair.as_str)
                else:
                    data["last_price"] = 0
            return data
        except Exception as e:
            logger.error(f"Error in [Swaps.get_volumes_and_prices]: {e}")
            return {}

    def get_top_pairs(self, pairs, days: int = 14):
        try:
            for i in pairs:
                i = Pair(i)

            pairs_data = [Pair(i).summary(days) for i in pairs]
            pairs_data.sort(key=lambda x: x["pair_trade_value_usd"], reverse=True)
            value_data = pairs_data[:3]
            top_pairs_by_value = [
                {i["trading_pair"]: i["pair_trade_value_usd"]} for i in value_data
            ]
            pairs_data.sort(key=lambda x: x["pair_liquidity_usd"], reverse=True)
            liquidity_data = pairs_data[:3]
            top_pairs_by_liquidity = [
                {i["trading_pair"]: i["pair_liquidity_usd"]} for i in liquidity_data
            ]
            pairs_data.sort(key=lambda x: x["pair_swaps_count"], reverse=True)
            swaps_data = pairs_data[:3]
            top_pairs_by_swaps = [
                {i["trading_pair"]: i["pair_swaps_count"]} for i in swaps_data
            ]
            return {
                "by_value_traded_usd": top_pairs_by_value,
                "by_current_liquidity_usd": top_pairs_by_liquidity,
                "by_swaps_count": top_pairs_by_swaps,
            }
        except Exception as e:
            logger.error(f"Error in [get_top_pairs]: {e}")
            return {"by_volume": [], "by_liquidity": [], "by_swaps": []}


class Templates:
    def __init__(self):
        pass

    def pair_summary(self, base: str, quote: str) -> dict:
        data = OrderedDict()
        data["trading_pair"] = f"{base}_{quote}"
        data["base_currency"] = base
        data["quote_currency"] = quote
        data["pair_swaps_count"] = 0
        data["base_price_usd"] = 0
        data["rel_price_usd"] = 0
        data["base_volume_coins"] = 0
        data["rel_volume_coins"] = 0
        data["base_liquidity_coins"] = 0
        data["base_liquidity_usd"] = 0
        data["base_trade_value_usd"] = 0
        data["rel_liquidity_coins"] = 0
        data["rel_liquidity_usd"] = 0
        data["rel_trade_value_usd"] = 0
        data["pair_liquidity_usd"] = 0
        data["pair_trade_value_usd"] = 0
        data["lowest_ask"] = 0
        data["highest_bid"] = 0
        return data

    def volumes_and_prices(self, suffix) -> dict:
        return {
            "base_volume": 0,
            "quote_volume": 0,
            f"highest_price_{suffix}": 0,
            f"lowest_price_{suffix}": 0,
            "last_price": 0,
            f"price_change_percent_{suffix}": 0,
        }


class Utils:
    def __init__(self):
        pass

    def safe_float(self, value, rounding=8):
        try:
            return round(float(value), rounding)
        except (ValueError, TypeError):
            return 0

    def get_suffix(self, days: int) -> str:
        if days == 1:
            return "24h"
        else:
            return f"{days}d"

    def get_related_coins(self, coin):
        coin = coin.split("-")[0]
        with open(f"{const.SCRIPT_PATH}/mm2/coins", "r") as f:
            coins = json.load(f)
        return [
            i["coin"]
            for i in coins
            if i["coin"] == coin or i["coin"].startswith(f"{coin}-")
        ]

    def get_liquidity(self, summary_data: dict) -> float:
        """Returns the total liquidity of all pairs in the summary data"""
        try:
            return sum([i["pair_liquidity_usd"] for i in summary_data])
        except Exception as e:
            logger.warning(f"Error getting liquidity for {summary_data}: {e}")
            return 0

    def get_value(self, summary_data: dict) -> float:
        """Returns the total value of all pairs in the summary data"""
        try:
            return sum([i["pair_trade_value_usd"] for i in summary_data])
        except Exception as e:
            logger.warning(f"Error getting total value for {summary_data}: {e}")
            return 0

    def get_chunks(self, data, chunk_length):
        for i in range(0, len(data), chunk_length):
            yield data[i: i + chunk_length]


class DexAPI:
    def __init__(self):
        pass

    # tuple, string, string -> list
    # returning orderbook for given trading pair
    def orderbook(self, pair):
        try:
            mm2_host = "http://127.0.0.1:7783"
            params = {"method": "orderbook", "base": pair[0], "rel": pair[1]}
            r = requests.post(mm2_host, json=params)
            return json.loads(r.text)
        except Exception as e:
            logger.error(f"Error in [DexAPI.orderbook]: {e}")
