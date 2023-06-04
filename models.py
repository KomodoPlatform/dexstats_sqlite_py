#!/usr/bin/env python3
import time
import json
import sqlite3
import requests
from collections import OrderedDict
from decimal import Decimal
from datetime import datetime, timedelta
from logger import logger
import const


class Files:
    def __init__(self, testing: bool = False):
        if testing:
            folder = "tests/fixtures"
        else:
            folder = "cache"
        self.atomicdexio = f"{folder}/atomicdexio_cache.json"
        self.atomicdex_fortnight = f"{folder}/atomicdex_fortnight_cache.json"
        self.coins = "mm2/coins"
        self.coins_config = f"{folder}/coins_config.json"
        self.gecko_data = f"{folder}/gecko_cache.json"
        self.summary = f"{folder}/summary_cache.json"
        self.ticker = f"{folder}/ticker_cache.json"


class Cache:
    def __init__(self, testing: bool = False, db_path=const.MM2_DB_PATH, DB=None):
        self.db_path = db_path
        self.DB = DB
        self.testing = testing
        self.utils = Utils()
        self.files = Files(self.testing)
        self.load = self.Load(
            files=self.files,
            utils=self.utils
        )
        self.calc = self.Calc(
            db_path=self.db_path,
            load=self.load,
            testing=self.testing,
            utils=self.utils,
            DB=self.DB
        )
        self.save = self.Save(
            calc=self.calc,
            testing=self.testing,
            files=self.files
        )
        self.atomicdexio = None
        self.atomicdex_fortnight = None
        self.coins_config = None
        self.gecko_data = None
        self.summary = None
        self.ticker = None
        self.refresh()
        logger.info("Cache initialized...")

    def refresh(self):
        self.coins_config = self.load.coins_config()
        self.gecko_data = self.load.gecko_data()
        self.atomicdexio = self.load.atomicdexio()
        self.atomicdex_fortnight = self.load.atomicdex_fortnight()
        self.summary = self.load.summary()
        self.ticker = self.load.ticker()

    class Load:
        def __init__(self, files, utils):
            self.files = files
            self.utils = utils

        def atomicdexio(self):
            return self.utils.load_jsonfile(self.files.atomicdexio)

        def atomicdex_fortnight(self):
            return self.utils.load_jsonfile(self.files.atomicdex_fortnight)

        def coins(self):
            return self.utils.load_jsonfile(self.files.coins)

        def coins_config(self):
            return self.utils.load_jsonfile(self.files.coins_config)

        def gecko_data(self):
            return self.utils.load_jsonfile(self.files.gecko_data)

        def summary(self):
            return self.utils.load_jsonfile(self.files.summary)

        def ticker(self):
            return self.utils.load_jsonfile(self.files.ticker)

    class Calc:
        def __init__(self, db_path, load, testing, utils, DB=None):
            self.DB = DB
            self.db_path = db_path
            self.load = load
            self.testing = testing
            self.utils = utils

        # Data for atomicdex.io website
        def atomicdexio(self, days: int = 1, verbose: bool = True) -> dict:
            try:
                if self.DB is None:
                    DB = SqliteDB(self.db_path)
                else:
                    DB = self.DB
                pairs = DB.get_pairs(days)
                logger.info(f"{len(pairs)} pairs ({days} days)")
                pair_summaries = [Pair(i, self.db_path, self.testing, DB=DB).summary(days) for i in pairs]
                current_liquidity = self.utils.get_liquidity(pair_summaries)
                if days == 1:
                    data = DB.get_atomicdexio()
                else:
                    swaps = DB.get_timespan_swaps(days)
                    logger.info(f"{len(swaps)} swaps ({days} days)")
                    data = {
                        "days": days,
                        "swaps_count": len(swaps),
                        "swaps_value": self.utils.get_value(pair_summaries),
                        "top_pairs": self.utils.get_top_pairs(pair_summaries),
                    }
                    data = self.utils.clean_decimal_dict(data)
                data.update(
                    {"current_liquidity": round(float(current_liquidity), 8)}
                )
                logger.info(f"data: {data}")
                return data
            except Exception as e:
                logger.error(f"Error in [Cache.calc.atomicdexio]: {e}")
                return None

        def gecko_data(self):
            try:
                return Gecko().get_gecko_data()
            except Exception as e:
                logger.error(f"Error in [Cache.calc.gecko_data]: {e}")
                return None

        def pair_summaries(self, days: int = 1, clean=False):
            try:
                if self.DB is None:
                    DB = SqliteDB(self.db_path)
                else:
                    DB = self.DB
                pairs = DB.get_pairs(days)
                logger.info(f"Calculating pair summaries for {len(pairs)} pairs ({days} days)")
                data = [Pair(i, self.db_path, self.testing, DB=DB).summary(days) for i in pairs]
                if clean:
                    return self.utils.clean_decimal_dict_list(data)
                return data
            except Exception as e:
                logger.error(f"Error in [Cache.calc.pair_summaries]: {e}")
                return None

        def ticker(self, days: int = 1):
            try:
                if self.DB is None:
                    DB = SqliteDB(self.db_path)
                else:
                    DB = self.DB
                pairs = DB.get_pairs(days)
                logger.info(f"Calculating ticker cache for {len(pairs)} pairs ({days} days)")
                return [Pair(i, self.db_path, self.testing, DB=DB).ticker(days) for i in pairs]
            except Exception as e:
                logger.error(f"Error in [Cache.calc.ticker]: {e}")
                return None

    class Save:
        '''
        Updates cache json files.
        '''
        def __init__(self, calc, files, testing=False):
            self.calc = calc
            self.files = files
            self.testing = testing

        def save(self, path, data):
            if self.testing:
                if isinstance(data, dict):
                    if path in [
                        self.files.gecko_data,
                        self.files.coins_config
                    ]:
                        return {"result": f"Validated {path} data"}
            try:
                with open(path, "w+") as f:
                    json.dump(data, f, indent=4)
                    logger.info(f"Updated {path}")
                    return {
                        "result": f"Updated {path}"
                    }
            except Exception as e:
                logger.error(f"Error saving {path}: {e}")
            return None

        def atomicdexio(self):
            data = self.calc.atomicdexio(days=1, verbose=False)
            if data is not None:
                return self.save(self.files.atomicdexio, data)

        def atomicdex_fortnight(self):
            data = self.calc.atomicdexio(days=14, verbose=True)
            if data is not None:
                return self.save(self.files.atomicdex_fortnight, data)

        def coins_config(self) -> dict:
            data = requests.get(const.COINS_CONFIG_URL).json()
            if data is not None:
                return self.save(self.files.coins_config, data)

        def coins(self) -> dict:
            data = requests.get(const.COINS_URL).json()
            if data is not None:
                return self.save(self.files.coins, data)

        def gecko_data(self) -> dict:
            data = self.calc.gecko_data()
            if data is not None:
                return self.save(self.files.gecko_data, data)

        def summary(self, days=1):
            data = self.calc.pair_summaries(days, clean=True)
            if data is not None:
                return self.save(self.files.summary, data)

        def ticker(self):
            data = self.calc.ticker()
            if data is not None:
                return self.save(self.files.ticker, data)


class Gecko:
    def __init__(self, testing=False):
        self.testing = testing
        self.utils = Utils()
        self.files = Files(self.testing)
        self.coins_config = self.utils.load_jsonfile(self.files.coins_config)

    def load_gecko_data(self):
        return self.utils.load_jsonfile(self.files.gecko_data)

    def get_gecko_info_template(self, coin_id):
        return {"usd_market_cap": 0, "usd_price": 0, "coingecko_id": coin_id}

    def get_gecko_coin_ids_list(self) -> list:
        coin_ids = list(
            set(
                [
                    self.coins_config[i]["coingecko_id"]
                    for i in self.coins_config
                    if self.coins_config[i]["coingecko_id"]
                    not in ["na", "test-coin", ""]
                ]
            )
        )
        coin_ids.sort()
        return coin_ids

    def get_gecko_info_dict(self) -> dict:
        coins_info = {}
        for coin in self.coins_config:
            native_coin = coin.split("-")[0]
            coin_id = self.coins_config[coin]["coingecko_id"]
            if coin_id not in ["na", "test-coin", ""]:
                coins_info.update({coin: self.get_gecko_info_template(coin_id)})
                if native_coin not in coins_info:
                    coins_info.update(
                        {native_coin: self.get_gecko_info_template(coin_id)}
                    )
        return coins_info

    def get_gecko_coins_dict(self, coins_info: dict, coin_ids: list) -> dict:
        gecko_coins = {}
        for coin_id in coin_ids:
            gecko_coins.update({coin_id: []})
        for coin in coins_info:
            coin_id = coins_info[coin]["coingecko_id"]
            gecko_coins[coin_id].append(coin)
        return gecko_coins

    def get_gecko_data(self):
        param_limit = 200
        coin_ids = self.get_gecko_coin_ids_list()
        coins_info = self.get_gecko_info_dict()
        gecko_coins = self.get_gecko_coins_dict(coins_info, coin_ids)
        coin_id_chunks = list(self.utils.get_chunks(coin_ids, param_limit))
        for chunk in coin_id_chunks:
            chunk_ids = ",".join(chunk)
            try:
                params = f"ids={chunk_ids}&vs_currencies=usd&include_market_cap=true"
                url = f"https://api.coingecko.com/api/v3/simple/price?{params}"
                gecko_data = requests.get(url).json()
            except Exception as e:
                error = {"error": f"{url} is not available"}
                logger.error(f"Error in [get_gecko_data]: {e} ({error})")
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
                logger.error(f"Error in [get_gecko_data]: {e}")
        return coins_info


class Orderbook:
    def __init__(self, pair, testing=False):
        self.pair = pair
        self.testing = testing
        self.utils = Utils()
        self.templates = Templates()
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
        '''
        Gets a list of orderbooks for all coins related to the pair
        (including wrapped tokens). Since the v2 Orderbook returns
        both segwit and non-segwit orders for UTXO coins, we need to
        exclude those tickers to avoid duplicate uuids in the output.
        '''
        try:
            coin_a = self.pair.as_tuple[0]
            coin_b = self.pair.as_tuple[1]
            coins_a = self.utils.get_related_coins(coin_a)
            coins_b = self.utils.get_related_coins(coin_b)
            if len(coins_a) == 0:
                return []
            if len(coins_b) == 0:
                return []
            orderbooks_list = []
            for coin_a in coins_a:
                for coin_b in coins_b:
                    if coin_a != coin_b:
                        if '-segwit' not in coin_a and '-segwit' not in coin_b:
                            alt_pair = (coin_a, coin_b)
                            orderbook = self.dexapi.orderbook(alt_pair)
                            if orderbook:
                                orderbooks_list.append(orderbook)
            return orderbooks_list
        except Exception as e:
            logger.error(f"Error for {self.pair.as_str}: {e}")
            return []

    def get_and_parse(self, endpoint=False, orderbooks_list=None):
        try:
            orderbook = self.templates.orderbook(self.pair.base, self.pair.quote)
            if orderbooks_list is None:
                orderbooks_list = self.related_orderbooks_list()
            for i in orderbooks_list:
                orderbook["bids"] += i["bids"]
                orderbook["asks"] += i["asks"]
                if isinstance(i["total_asks_base_vol"], int):
                    logger.debug(f"int type for total_asks_base_vol with {self.pair.as_tuple}")
                    base_vol = i["total_asks_base_vol"]
                else:
                    base_vol = i["total_asks_base_vol"]["decimal"]
                if isinstance(i["total_bids_rel_vol"], int):
                    rel_vol = i["total_bids_rel_vol"]
                else:
                    rel_vol = i["total_bids_rel_vol"]["decimal"]

                orderbook["total_asks_base_vol"] += Decimal(base_vol)
                orderbook["total_bids_rel_vol"] += Decimal(rel_vol)
            orderbook["total_asks_base_vol"] = str(orderbook["total_asks_base_vol"])
            orderbook["total_bids_rel_vol"] = str(orderbook["total_bids_rel_vol"])
            bids_converted_list = []
            asks_converted_list = []

        except Exception as e:
            logger.warning(f'Error for {self.pair.as_str}: {e} {i["total_asks_base_vol"]}')
            return self.templates.orderbook(self.pair.base, self.pair.quote)

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


class Pair:
    """Allows for referencing pairs as a string or tuple."""
    def __init__(self, pair, db_path=const.MM2_DB_PATH, testing=False, DB=None):
        try:
            self.DB = DB
            self.testing = testing
            self.db_path = db_path
            self.files = Files(testing=self.testing)
            self.utils = Utils()
            self.templates = Templates()
            self.orderbook = Orderbook(pair=self, testing=self.testing)
            self.swaps = Swaps(pair=self, db_path=self.db_path, testing=self.testing)
            self.gecko_data = self.utils.load_jsonfile(self.files.gecko_data)

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

        except Exception as e:
            logger.warning(f"Error in [Pair]: {e}")

    def trades(self):
        """Returns trades for this pair."""
        try:
            if self.DB is None:
                DB = SqliteDB(self.db_path)
            else:
                DB = self.DB
            DB.conn.row_factory = sqlite3.Row
            DB.sql_cursor = DB.conn.cursor()
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

    def ticker(self, days=1) -> dict:
        try:
            if self.DB is None:
                DB = SqliteDB(self.db_path)
            else:
                DB = self.DB
            DB.conn.row_factory = sqlite3.Row
            DB.sql_cursor = DB.conn.cursor()
            pair_ticker = OrderedDict()
            swaps_for_pair = DB.get_swaps_for_pair(self.as_tuple)
            data = self.swaps.get_volumes_and_prices(swaps_for_pair, days)
            pair_ticker[self.as_str] = OrderedDict()
            pair_ticker[self.as_str]["isFrozen"] = "0"
            last_price = "{:.10f}".format(data["last_price"])
            quote_volume = "{:.10f}".format(data["quote_volume"])
            base_volume = "{:.10f}".format(data["base_volume"])
            pair_ticker[self.as_str]["last_price"] = last_price
            pair_ticker[self.as_str]["quote_volume"] = quote_volume
            pair_ticker[self.as_str]["base_volume"] = base_volume
            return pair_ticker
        except Exception as e:
            logger.warning(f"Error in [Pair.ticker]: {e}")
            return {}

    def summary(self, days: int = 1, orderbook: dict = None) -> dict:
        """Calculates CMC summary endpoint data for a pair"""
        try:
            if self.DB is None:
                DB = SqliteDB(self.db_path)
            else:
                DB = self.DB
            base = self.base
            quote = self.quote
            suffix = self.utils.get_suffix(days)
            data = self.templates.pair_summary(base, quote)
            timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
            swaps_for_pair = DB.get_swaps_for_pair(self.as_tuple, timestamp)

            data["pair_swaps_count"] = len(swaps_for_pair)
            volumes_and_prices = self.swaps.get_volumes_and_prices(swaps_for_pair, days)
            for i in ["last_price", "base_volume", "quote_volume"]:
                value = "{:.10f}".format(volumes_and_prices[i])
                data[i] = value

            for i in [
                "price_change_percent",
                "price_change",
                "highest_price",
                "lowest_price",
            ]:
                value = "{:.10f}".format(volumes_and_prices[f"{i}_{suffix}"])
                data[f"{i}_{suffix}"] = value

        except Exception as e:
            logger.error(f"Error while getting summary for pair {self.as_str}: {e}")

        try:
            # Liquidity
            base_price = self.utils.get_gecko_usd_price(base, self.gecko_data)
            base_volume = volumes_and_prices["base_volume"]
            quote_price = self.utils.get_gecko_usd_price(quote, self.gecko_data)
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
            data["lowest_ask"] = self.utils.find_lowest_ask(orderbook)
            data["highest_bid"] = self.utils.find_highest_bid(orderbook)
            data["base_liquidity_coins"] = self.utils.dec_to_string(
                orderbook["total_asks_base_vol"]
            )
            data["rel_liquidity_coins"] = self.utils.dec_to_string(
                orderbook["total_bids_rel_vol"]
            )
        except Exception as e:
            logger.error(f"Error in [Pair.summary] for {self.as_str}: {e}")
            pass

        try:
            data["base_liquidity_usd"] = Decimal(base_price) * Decimal(
                data["base_liquidity_coins"]
            )
        except KeyError:
            pass

        try:
            data["rel_liquidity_usd"] = Decimal(quote_price) * Decimal(
                data["rel_liquidity_coins"]
            )
        except KeyError as e:
            logger.warning(f"Error in [Pair.summary] for {self.as_str}: {e}")
            pass
        data["pair_liquidity_usd"] = (
            data["rel_liquidity_usd"] + data["base_liquidity_usd"]
        )

        # Value traded in USD
        try:
            data["base_trade_value_usd"] = Decimal(base_price) * Decimal(base_volume)
        except KeyError:
            pass
        try:
            data["rel_trade_value_usd"] = Decimal(quote_price) * Decimal(quote_volume)
        except KeyError as e:
            logger.warning(f"Error in [Pair.summary] for {self.as_str}: {e}")
            pass
        data["pair_trade_value_usd"] = (
            data["base_trade_value_usd"] + data["rel_trade_value_usd"]
        )
        return data


class SqliteDB:
    def __init__(self, path_to_db, dict_format=False, testing=False):
        self.utils = Utils()
        self.files = Files(testing)
        self.testing = testing
        self.conn = sqlite3.connect(path_to_db)
        if dict_format:
            self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        self.gecko_data = self.utils.load_jsonfile(self.files.gecko_data)

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

    def get_last_price_for_pair(self, base: str, quote: str) -> float:
        """
        Takes a pair in the format `KMD_BTC` and returns the
        last trade price for that pair. Response scans both
        buy and sell swaps (e.g. KMD/BTC and BTC/KMD)
        """
        self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()

        try:
            swap_price = None
            swap_time = None
            sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{base}' \
                    AND taker_coin_ticker='{quote}' AND is_success=1 \
                    ORDER BY started_at DESC LIMIT 1;"
            self.sql_cursor.execute(sql)
            resp = self.sql_cursor.fetchone()
            if resp is not None:
                swap_price = Decimal(resp["taker_amount"]) / Decimal(
                    resp["maker_amount"]
                )
                swap_time = resp["started_at"]
        except Exception as e:
            logger.warning(f"Error getting swap_price for {base}/{quote}: {e}")

        try:
            swap_price2 = None
            swap_time2 = None
            sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{quote}' \
                    AND taker_coin_ticker='{base}' AND is_success=1 \
                    ORDER BY started_at DESC LIMIT 1;"
            self.sql_cursor.execute(sql)
            resp2 = self.sql_cursor.fetchone()
            if resp2 is not None:
                swap_price2 = Decimal(resp2["maker_amount"]) / Decimal(
                    resp2["taker_amount"]
                )
                swap_time2 = resp2["started_at"]
        except Exception as e:
            logger.warning(f"Error getting swap_price2 for {base}/{quote}: {e}")

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

    def get_atomicdexio(self) -> dict:
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
    def __init__(self, pair=None, db_path=const.MM2_DB_PATH, testing=False, DB=None):
        self.DB = DB
        self.testing = testing
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
                # TODO: using timestamps as an index works for now,
                # but breaks when two swaps have the same timestamp.
                highest_price = max(swap_prices.values())
                lowest_price = min(swap_prices.values())
                newest_price = swap_prices[max(swap_prices.keys())]
                oldest_price = swap_prices[min(swap_prices.keys())]
                price_change = Decimal(newest_price) - Decimal(oldest_price)
                pct_change = (Decimal(newest_price) - Decimal(oldest_price)) / Decimal(
                    100
                )
                data[f"highest_price_{suffix}"] = highest_price
                data[f"lowest_price_{suffix}"] = lowest_price
                data["last_price"] = newest_price
                data[f"price_change_percent_{suffix}"] = pct_change
                data[f"price_change_{suffix}"] = price_change
            else:
                if self.DB is None:
                    DB = SqliteDB(self.db_path)
                else:
                    DB = self.DB
                if self.pair:
                    data["last_price"] = DB.get_last_price_for_pair(self.pair.base, self.pair.quote)
                else:
                    data["last_price"] = 0
            return data
        except Exception as e:
            logger.error(f"Error in [Swaps.get_volumes_and_prices]: {e}")
            return {}


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
            f"price_change_{suffix}": 0,
        }

    def orderbook(self, base: str, quote: str, v2=False) -> dict:
        data = {
            "pair": f"{base}_{quote}",
            "bids": [],
            "asks": [],
            "total_asks_base_vol": 0,
            "total_bids_rel_vol": 0,
        }
        if v2:
            data.update({
                "total_asks_base_vol": {
                    "decimal": 0
                }
            })
            data.update({
                "total_bids_rel_vol": {
                    "decimal": 0
                }
            })
        return data


class Utils:
    def __init__(self):
        self.files = Files()

    def load_jsonfile(self, path, attempts=5):
        i = 0
        while True:
            i += 1
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except Exception as e:
                if i >= attempts:
                    logger.error(f"Error loading {path}: {e}")
                    return None
                time.sleep(1)

    def dec_to_string(self, value, rounding=8):
        try:
            if isinstance(value, str):
                value = Decimal(value)
            value = str(value.quantize(Decimal(f'1.{"0" * rounding}')))
            return value
        except (ValueError, TypeError):
            return "0"

    def clean_decimal_dict_list(self, data, to_string=False, rounding=8):
        '''
        Works for a list of dicts with no nesting
        (e.g. summary_cache.json)
        '''
        for i in data:
            for j in i:
                if isinstance(i[j], Decimal):
                    if to_string:
                        i[j] = self.dec_to_string(i[j], rounding)
                    else:
                        i[j] = round(float(i[j]), rounding)
        return data

    def clean_decimal_dict(self, data, to_string=False, rounding=8):
        '''
        Works for a simple dict with no nesting
        (e.g. summary_cache.json)
        '''
        for i in data:
            if isinstance(data[i], Decimal):
                if to_string:
                    data[i] = self.dec_to_string(data[i], rounding)
                else:
                    data[i] = round(float(data[i]), rounding)
        return data

    def get_suffix(self, days: int) -> str:
        if days == 1:
            return "24h"
        else:
            return f"{days}d"

    def get_related_coins(self, coin):
        try:
            coin = coin.split("-")[0]
            coins = self.load_jsonfile(self.files.coins)
            return [
                i["coin"] for i in coins
                if (i["coin"] == coin or i["coin"].startswith(f"{coin}-"))
            ]
        except Exception as e:
            logger.error(f"Error getting related coins for {coin}: {e}")
            return []

    def get_liquidity(self, summary_data: dict) -> float:
        """Returns the total liquidity of all pairs in the summary data"""
        try:
            pairs_liquidity = [i["pair_liquidity_usd"] for i in summary_data]
            return sum(pairs_liquidity)
        except Exception as e:
            logger.warning(f"Error getting liquidity for {summary_data}: {e}")
            return 0

    def get_value(self, summary_data: dict) -> float:
        """Returns the total value of all pairs in the summary data"""
        try:
            pairs_value = [i["pair_trade_value_usd"] for i in summary_data]
            return sum(pairs_value)
        except Exception as e:
            logger.warning(f"Error getting total value for {summary_data}: {e}")
            return 0

    def get_chunks(self, data, chunk_length):
        for i in range(0, len(data), chunk_length):
            yield data[i: i + chunk_length]

    def get_gecko_usd_price(self, coin: str, gecko_data) -> float:
        try:
            return gecko_data[coin]["usd_price"]
        except KeyError:
            return 0

    def get_top_pairs(self, pairs_data: list):
        try:
            pairs_data.sort(key=lambda x: x["pair_trade_value_usd"], reverse=True)
            value_data = pairs_data[:5]
            top_pairs_by_value = {}
            [
                top_pairs_by_value.update(
                    {i["trading_pair"]: i["pair_trade_value_usd"]}
                )
                for i in value_data
            ]
            pairs_data.sort(key=lambda x: x["pair_liquidity_usd"], reverse=True)
            liquidity_data = pairs_data[:5]
            top_pairs_by_liquidity = {}
            [
                top_pairs_by_liquidity.update(
                    {i["trading_pair"]: i["pair_liquidity_usd"]}
                )
                for i in liquidity_data
            ]
            pairs_data.sort(key=lambda x: x["pair_swaps_count"], reverse=True)
            swaps_data = pairs_data[:5]
            top_pairs_by_swaps = {}
            [
                top_pairs_by_swaps.update({i["trading_pair"]: i["pair_swaps_count"]})
                for i in swaps_data
            ]

            return {
                "by_value_traded_usd": self.clean_decimal_dict(
                    top_pairs_by_value
                ),
                "by_current_liquidity_usd": self.clean_decimal_dict(
                    top_pairs_by_liquidity
                ),
                "by_swaps_count": self.clean_decimal_dict(top_pairs_by_swaps),
            }
        except Exception as e:
            logger.error(f"Error in [get_top_pairs]: {e}")
            return {"by_volume": [], "by_liquidity": [], "by_swaps": []}

    def find_lowest_ask(self, orderbook: dict) -> str:
        """Returns lowest ask from provided orderbook"""
        lowest = 0
        try:
            for ask in orderbook["asks"]:
                price = Decimal(ask["price"]["decimal"])
                logger.info(f"price: {price}")
                if lowest == "0":
                    lowest = price
                elif Decimal(price) < Decimal(lowest):
                    lowest = price
            logger.info(f"lowest price: {price}")
        except KeyError:
            pass
        return "{:.8f}".format(Decimal(lowest))

    def find_highest_bid(self, orderbook: list) -> str:
        """Returns highest bid from provided orderbook"""
        highest = 0
        try:
            for bid in orderbook["bids"]:
                price = Decimal(bid["price"]["decimal"])
                logger.info(f"price: {price}")
                if Decimal(price) > Decimal(highest):
                    highest = price
            logger.info(f"highest price: {price}")
        except KeyError:
            pass
        return "{:.8f}".format(Decimal(highest))


class DexAPI:
    def __init__(self, testing=False):
        self.testing = testing
        self.utils = Utils()
        self.files = Files(self.testing)
        self.templates = Templates()
        self.coins_config = self.utils.load_jsonfile(self.files.coins_config)

    # tuple, string, string -> list
    # returning orderbook for given trading pair
    def orderbook(self, pair):
        try:
            base = pair[0]
            quote = pair[1]
            if base not in self.coins_config or quote not in self.coins_config:
                return self.templates.orderbook(base, quote, v2=True)
            if self.coins_config[base]["wallet_only"] or self.coins_config[quote]["wallet_only"]:
                return self.templates.orderbook(base, quote, v2=True)
        except Exception as e:
            logger.error(f"Error in [DexAPI.orderbook] for {pair}: {e}")
            return self.templates.orderbook(base, quote, v2=True)
        
        try:
            mm2_host = "http://127.0.0.1:7783"
            params = {
                "mmrpc": "2.0",
                "method": "orderbook",
                "params": {
                    "base": pair[0],
                    "rel": pair[1]
                },
                "id": 42
            }
            r = requests.post(mm2_host, json=params)
            if "result" in json.loads(r.text):
                return json.loads(r.text)["result"]
            if "error_type" in json.loads(r.text):
                error = json.loads(r.text)['error_type']
                logger.debug(f"Error in [DexAPI.orderbook] for {pair}: {error}")
            else:
                logger.info(f"Error in [DexAPI.orderbook] for {pair}: {r.text}")
            return self.templates.orderbook(base, quote, v2=True)
        except Exception as e:
            logger.error(f"Error in [DexAPI.orderbook] for {pair}: {e}")
            logger.info(f"Error in [DexAPI.orderbook] for {pair}: {r.text}")
