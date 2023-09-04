#!/usr/bin/env python3
import os
import time
import json
import sqlite3
import requests
from typing import Any
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
            files=self.files,
            utils=self.utils
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
                DB = self.utils.get_db(self.db_path, self.DB)
                pairs = DB.get_pairs(days)
                logger.debug(f"Calculating atomicdexio stats for {len(pairs)} pairs ({days} days)")
                pair_summaries = [
                    Pair(i, self.db_path, self.testing, DB=DB).summary(days) for i in pairs
                ]
                current_liquidity = self.utils.get_liquidity(pair_summaries)
                if days == 1:
                    data = DB.get_atomicdexio()
                else:
                    swaps = DB.get_timespan_swaps(days)
                    logger.debug(f"{len(swaps)} swaps ({days} days)")
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
                return data
            except Exception as e:
                logger.error(f"{type(e)} Error in [Cache.calc.atomicdexio]: {e}")
                return None

        def gecko_data(self):
            return Gecko().get_gecko_data()

        def pair_summaries(self, summary_days: int = 1, pairs_days: int = 1, clean=False):
            try:
                DB = SqliteDB(self.db_path)
                DB = self.utils.get_db(self.db_path, self.DB)
                pairs = DB.get_pairs(pairs_days)
                logger.debug(f"Calculating {summary_days} day pair summaries for {len(pairs)} pairs traded in last {pairs_days} days")
                data = [Pair(i, self.db_path, self.testing, DB=DB).summary(summary_days) for i in pairs]
                if clean:
                    data = self.utils.clean_decimal_dict_list(data)
                return data
            except Exception as e:
                logger.error(f"{type(e)} Error in [Cache.calc.pair_summaries]: {e}")
                return None

        def ticker(self, days: int = 1):
            try:
                DB = self.utils.get_db(self.db_path, self.DB)
                pairs = DB.get_pairs(days)
                logger.debug(f"Calculating ticker cache for {len(pairs)} pairs ({days} days)")
                return [Pair(i, self.db_path, self.testing, DB=DB).ticker(days) for i in pairs]
            except Exception as e:
                logger.error(f"{type(e)} Error in [Cache.calc.ticker]: {e}")
                return None

    class Save:
        '''
        Updates cache json files.
        '''
        def __init__(self, calc, files, utils, testing=False):
            self.calc = calc
            self.files = files
            self.testing = testing
            self.utils = utils

        def save(self, path, data):
            if not isinstance(data, (dict, list)):
                raise TypeError(f"Invalid data type: {type(data)}, must be dict or list")
            elif self.testing:
                if path in [self.files.gecko_data, self.files.coins_config]:
                    return {"result": f"Validated {path} data"}
            try:
                if isinstance(data, (dict, list)):
                    with open(path, "w+") as f:
                        json.dump(data, f, indent=4)
                        logger.info(f"Updated {path}")
                        return {
                            "result": f"Updated {path}"
                        }
            except Exception as e:
                logger.error(f"{type(e)} Error saving {path}: {e}")
            return None

        def atomicdexio(self, days: int = 1, verbose=False):
            data = self.calc.atomicdexio(days, verbose)
            if data is not None:
                return self.save(self.files.atomicdexio, data)

        def atomicdex_fortnight(self, days: int = 14, verbose=True):
            data = self.calc.atomicdexio(days, verbose)
            if data is not None:
                return self.save(self.files.atomicdex_fortnight, data)

        def coins_config(self, url=const.COINS_CONFIG_URL) -> dict:
            data = self.utils.download_json(url)
            if data is not None:
                return self.save(self.files.coins_config, data)

        def coins(self, url=const.COINS_URL) -> dict:
            data = self.utils.download_json(url)
            if data is not None:
                return self.save(self.files.coins, data)

        def gecko_data(self) -> dict:
            data = self.calc.gecko_data()
            if data is not None:
                return self.save(self.files.gecko_data, data)

        def summary(self, days: int = 1, pairs_days: int = 7):
            data = self.calc.pair_summaries(days, pairs_days, clean=True)
            if data is not None:
                return self.save(self.files.summary, data)

        def ticker(self, days=1):
            data = self.calc.ticker(days)
            if data is not None:
                return self.save(self.files.ticker, data)


class Gecko:
    def __init__(self, testing=False):
        self.testing = testing
        self.utils = Utils()
        self.templates = Templates()
        self.files = Files(self.testing)
        self.coins_config = self.utils.load_jsonfile(self.files.coins_config)
        self.gecko_data = self.load_gecko_data()

    def load_gecko_data(self):
        return self.utils.load_jsonfile(self.files.gecko_data)

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
                coins_info.update({coin: self.templates.gecko_info(coin_id)})
                if native_coin not in coins_info:
                    coins_info.update({native_coin: self.templates.gecko_info(coin_id)})
        return coins_info

    def get_gecko_coins_dict(self, gecko_info: dict, coin_ids: list) -> dict:
        gecko_coins = {}
        for coin_id in coin_ids:
            gecko_coins.update({coin_id: []})
        for coin in gecko_info:
            coin_id = gecko_info[coin]["coingecko_id"]
            gecko_coins[coin_id].append(coin)
        return gecko_coins

    def get_gecko_data(self):
        param_limit = 200
        coin_ids = self.get_gecko_coin_ids_list()
        logger.info(f"Getting Gecko data for {len(coin_ids)} coins")
        coins_info = self.get_gecko_info_dict()
        gecko_coins = self.get_gecko_coins_dict(coins_info, coin_ids)
        coin_id_chunks = list(self.utils.get_chunks(coin_ids, param_limit))
        for chunk in coin_id_chunks:
            logger.info(f"Getting Gecko data chunk {coin_id_chunks.index(chunk)+1}/{len(coin_id_chunks)}")
            chunk_ids = ",".join(chunk)
            try:
                params = f"ids={chunk_ids}&vs_currencies=usd&include_market_cap=true"
                url = f"https://api.coingecko.com/api/v3/simple/price?{params}"
                gecko_data = requests.get(url).json()
            except Exception as e:
                error = {"error": f"{url} is not available"}
                logger.error(f"{type(e)} Error in [get_gecko_data]: {e} ({error})")
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

            except Exception as e:
                logger.error(f"{type(e)} Error in [get_gecko_data]: {e}")
        return coins_info


class Time:
    def __init__(self, testing=False):
        pass

    def now(self):
        return int(time.time())

    def hours_ago(self, num):
        return int(time.time()) - (num * 60 * 60)

    def days_ago(self, num):
        return int(time.time()) - (num * 60 * 60) * 24


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
            logger.error(f"{type(e)} Error in [Orderbook.for_pair]: {e}")
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
            logger.error(f"{type(e)} Error for {self.pair.as_str}: {e}")
            return []

    def get_and_parse(self, endpoint=False, orderbooks_list=None):
        try:
            orderbook = self.templates.orderbook(self.pair.base, self.pair.quote)
            if orderbooks_list is None:
                orderbooks_list = self.related_orderbooks_list()
            for i in orderbooks_list:
                orderbook["bids"] += i["bids"]
                orderbook["asks"] += i["asks"]
                base_vol = i["total_asks_base_vol"]["decimal"]
                rel_vol = i["total_bids_rel_vol"]["decimal"]
                orderbook["total_asks_base_vol"] += Decimal(base_vol)
                orderbook["total_bids_rel_vol"] += Decimal(rel_vol)
            orderbook["total_asks_base_vol"] = self.utils.round_to_str(
                orderbook["total_asks_base_vol"],
                13
            )
            orderbook["total_bids_rel_vol"] = self.utils.round_to_str(
                orderbook["total_bids_rel_vol"],
                13
            )
            bids_converted_list = []
            asks_converted_list = []

        except Exception as e:
            logger.warning(f'Error for {self.pair.as_str}: {e} {i["total_asks_base_vol"]}')
            return self.templates.orderbook(self.pair.base, self.pair.quote)

        try:
            for bid in orderbook["bids"]:
                bid_price = self.utils.round_to_str(bid["price"]["decimal"], 13)
                bid_vol = self.utils.round_to_str(bid["base_max_volume"]["decimal"], 13)
                if endpoint:
                    bids_converted_list.append([bid_price, bid_vol])
                else:
                    bids_converted_list.append({
                        "price": bid_price,
                        "base_max_volume": bid_vol,
                    })
        except KeyError as e:
            logger.warning(f"{type(e)} Error in [get_and_parse_orderbook]: {e}")
            pass

        try:
            for ask in orderbook["asks"]:
                ask_price = self.utils.round_to_str(ask["price"]["decimal"], 13)
                ask_vol = self.utils.round_to_str(ask["base_max_volume"]["decimal"], 13)
                if endpoint:
                    asks_converted_list.append([ask_price, ask_vol])
                else:
                    asks_converted_list.append({
                        "price": ask_price,
                        "base_max_volume": ask_vol
                    })
        except KeyError as e:
            logger.warning(f"{type(e)} Error in [get_and_parse_orderbook]: {e}")
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
            logger.warning(f"{type(e)} Error in [Pair]: {e}")

    def trades(self, days=1):
        """Returns trades for this pair."""
        try:
            trades_info = []
            timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
            DB = self.utils.get_db(self.db_path, self.DB)
            swaps_for_pair = DB.get_swaps_for_pair(self.as_tuple, timestamp)
            logger.info(f"{len(swaps_for_pair)} swaps_for_pair:")
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
            logger.warning(f"{type(e)} Error in [Pair.trades]: {e}")
            return []

    def get_volumes_and_prices(
        self,
        days: int = 1,
    ) -> dict:
        """
        Iterates over list of swaps to get data for CMC summary endpoint
        """
        suffix = self.utils.get_suffix(days)
        data = self.templates.volumes_and_prices(suffix)
        try:
            timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
            swaps_for_pair = self.DB.get_swaps_for_pair(self.as_tuple, timestamp)
            swap_prices = self.swaps.get_swap_prices(swaps_for_pair)
            swaps_volumes = self.swaps.get_swaps_volumes(swaps_for_pair)
            data["base_volume"] = swaps_volumes[0]
            data["quote_volume"] = swaps_volumes[1]
        except Exception as e:
            logger.error(f"{type(e)} Error in [Pair.get_volumes_and_prices]: {e}")

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
                DB = self.utils.get_db(self.db_path, self.DB)
                if self.base and self.quote:
                    data["last_price"] = DB.get_last_price_for_pair(
                        self.base, self.quote
                    )
        except Exception as e:
            logger.error(f"{type(e)} Error in [Pair.get_volumes_and_prices]: {e}")
        return data

    def ticker(self, days=1) -> dict:
        try:
            DB = self.utils.get_db(self.db_path, self.DB)
            DB.conn.row_factory = sqlite3.Row
            DB.sql_cursor = DB.conn.cursor()
            pair_ticker = OrderedDict()
            data = self.get_volumes_and_prices(days)
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
            logger.warning(f"{type(e)} Error in [Pair.ticker]: {e}")
            return {}

    def summary(self, days: int = 1, orderbook: dict = None) -> dict:
        """Calculates CMC summary endpoint data for a pair"""
        try:
            base = self.base
            quote = self.quote
            suffix = self.utils.get_suffix(days)
            data = self.templates.pair_summary(base, quote)
            timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
            swaps_for_pair = self.DB.get_swaps_for_pair(self.as_tuple, timestamp)

            data["pair_swaps_count"] = len(swaps_for_pair)
            volumes_and_prices = self.get_volumes_and_prices(days)
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
            logger.error(f"{type(e)} Error while getting summary for pair {self.as_str}: {e}")

        try:
            # Liquidity
            base_price = self.utils.get_gecko_usd_price(base, self.gecko_data)
            base_volume = volumes_and_prices["base_volume"]
            quote_price = self.utils.get_gecko_usd_price(quote, self.gecko_data)
            quote_volume = volumes_and_prices["quote_volume"]

            data["base_price_usd"] = base_price
            data["rel_price_usd"] = quote_price
            data["base_volume"] = base_volume
            data["rel_volume"] = quote_volume
        except Exception as e:
            logger.error(f"{type(e)} Error in [Pair.summary] for {self.as_str}: {e}")

        try:
            if not orderbook:
                orderbook = self.orderbook.for_pair(endpoint=False)
            base_liquidity_coins = orderbook["total_asks_base_vol"]
            data["lowest_ask"] = self.utils.find_lowest_ask(orderbook)
            data["highest_bid"] = self.utils.find_highest_bid(orderbook)
            data["base_liquidity_coins"] = base_liquidity_coins
            data["rel_liquidity_coins"] = orderbook["total_bids_rel_vol"]
        except Exception as e:
            logger.error(f"{type(e)} Error in [Pair.summary] for {self.as_str}: {e}")
            pass

        try:
            data["base_liquidity_usd"] = Decimal(base_price) * Decimal(base_liquidity_coins)
        except KeyError:
            pass

        try:
            rel_liquidity_coins = data["rel_liquidity_coins"]
            data["rel_liquidity_usd"] = Decimal(quote_price) * Decimal(rel_liquidity_coins)
        except KeyError as e:
            logger.warning(f"{type(e)} Error in [Pair.summary] for {self.as_str}: {e}")
            pass
        rel_liquidity_usd = Decimal(data["rel_liquidity_usd"])
        base_liquidity_usd = Decimal(data["base_liquidity_usd"])
        pair_liquidity_usd = base_liquidity_usd + rel_liquidity_usd
        data["pair_liquidity_usd"] = pair_liquidity_usd

        # Value traded in USD
        try:
            data["base_trade_value_usd"] = Decimal(base_price) * Decimal(base_volume)
        except KeyError:
            pass
        try:
            data["rel_trade_value_usd"] = Decimal(quote_price) * Decimal(quote_volume)
        except KeyError as e:
            logger.warning(f"{type(e)} Error in [Pair.summary] for {self.as_str}: {e}")
            pass
        base_trade_value_usd = Decimal(data["base_trade_value_usd"])
        rel_trade_value_usd = Decimal(data["rel_trade_value_usd"])
        pair_trade_value_usd = rel_trade_value_usd + base_trade_value_usd
        data["pair_trade_value_usd"] = pair_trade_value_usd
        string_fields = [
            "base_liquidity_coins",
            "rel_liquidity_coins",
            "base_volume",
            "rel_volume"
        ]
        data = self.utils.values_to_str(data, string_fields)

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
            logger.warning(f"{type(e)} Error in [get_swaps_for_pair]: {e}")
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
            logger.warning(f"{type(e)} Error getting swap_price for {base}/{quote}: {e}")

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
            logger.warning(f"{type(e)} Error getting swap_price2 for {base}/{quote}: {e}")

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
            logger.error(f"{type(e)} Error in [get_swaps_volumes]: {e}")
            return [0, 0]


class Templates:
    def __init__(self):
        pass

    def gecko_info(self, coin_id):
        return {
            "usd_market_cap": 0,
            "usd_price": 0,
            "coingecko_id": coin_id
        }

    def pair_summary(self, base: str, quote: str) -> dict:
        data = OrderedDict()
        data["trading_pair"] = f"{base}_{quote}"
        data["base_currency"] = base
        data["quote_currency"] = quote
        data["pair_swaps_count"] = 0
        data["base_price_usd"] = 0
        data["rel_price_usd"] = 0
        data["base_volume"] = 0
        data["rel_volume"] = 0
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

    def get_db(self, db_path=None, DB=None):
        if DB is not None:
            return DB
        return SqliteDB(db_path)

    def values_to_str(self, data: dict, string_fields: list) -> dict:
        for field in string_fields:
            if field in data:
                if not isinstance(data[field], str):
                    data[field] = self.round_to_str(data[field])
        return data

    def load_jsonfile(self, path, attempts=5):
        i = 0
        while True:
            i += 1
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except Exception as e:
                if i >= attempts:
                    logger.error(f"{type(e)} Error loading {path}: {e}")
                    return None
                time.sleep(1)

    def download_json(self, url):
        try:
            return requests.get(url).json()
        except Exception as e:
            logger.error(f"{type(e)} Error downloading {url}: {e}")
            return None

    def round_to_str(self, value: Any, rounding=8):
        try:
            if isinstance(value, (str, int, float)):
                value = Decimal(value)
            if isinstance(value, Decimal):
                value = value.quantize(Decimal(f'1.{"0" * rounding}'))
                value = f"{value:.{rounding}f}"
            return value
        except (ValueError, TypeError):
            return "0"
        except Exception as e:
            logger.debug(f"round_to_str: {value} {type(value)} {rounding}")
            logger.error(f"{type(e)} Error in [round_to_str]: {value} {e}")
        return value

    def clean_decimal_dict_list(self, data, to_string=False, rounding=8):
        '''
        Works for a list of dicts with no nesting
        (e.g. summary_cache.json)
        '''
        for i in data:
            for j in i:
                if isinstance(i[j], Decimal):
                    if to_string:
                        i[j] = self.round_to_str(i[j], rounding)
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
                    data[i] = self.round_to_str(data[i], rounding)
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
            logger.error(f"{type(e)} Error getting related coins for {coin}: {e}")
            return []

    def get_liquidity(self, summary_data: dict) -> float:
        """Returns the total liquidity of all pairs in the summary data"""
        try:
            pairs_liquidity = [i["pair_liquidity_usd"] for i in summary_data]
            return sum(pairs_liquidity)
        except Exception as e:
            logger.warning(f"{type(e)} Error getting liquidity for {summary_data}: {e}")
            return 0

    def get_value(self, summary_data: dict) -> float:
        """Returns the total value of all pairs in the summary data"""
        try:
            pairs_value = [i["pair_trade_value_usd"] for i in summary_data]
            return sum(pairs_value)
        except Exception as e:
            logger.warning(f"{type(e)} Error getting total value for {summary_data}: {e}")
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
            logger.error(f"{type(e)} Error in [get_top_pairs]: {e}")
            return {"by_volume": [], "by_liquidity": [], "by_swaps": []}

    def find_lowest_ask(self, orderbook: dict) -> str:
        """Returns lowest ask from provided orderbook"""
        lowest = 0
        try:
            for ask in orderbook["asks"]:
                if not isinstance(ask["price"], Decimal):
                    if "decimal" in ask["price"]:
                        price = Decimal(ask["price"]["decimal"])
                    else:
                        price = Decimal(ask["price"])
                else:
                    price = ask["price"]
                if lowest == 0:
                    lowest = price
                elif Decimal(price) < Decimal(lowest):
                    lowest = price
        except KeyError as e:
            logger.error(e)
        return "{:.8f}".format(Decimal(lowest))

    def find_highest_bid(self, orderbook: list) -> str:
        """Returns highest bid from provided orderbook"""
        highest = 0
        try:
            for bid in orderbook["bids"]:
                if not isinstance(bid["price"], Decimal):
                    if "decimal" in bid["price"]:
                        price = Decimal(bid["price"]["decimal"])
                    else:
                        price = Decimal(bid["price"])
                else:
                    price = bid["price"]
                if highest == 0:
                    highest = price
                elif Decimal(price) < Decimal(highest):
                    highest = price
        except KeyError as e:
            logger.error(e)
        return "{:.8f}".format(Decimal(highest))



class DexAPI:
    def __init__(self, config=const.MM2_JSON, protocol: str = "http", testing=False):
        ip = '127.0.0.1'
        port = 7783
        netid = 7777
        self.protocol = protocol
        if os.path.isfile(config):
            with open(config, "r") as f:
                conf = json.load(f)
            self.userpass = conf["rpc_password"]
            if "rpc_ip" in conf:
                ip = conf["rpc_ip"]
            if "rpcport" in conf:
                port = conf["rpcport"]
            if "netid" in conf:
                self.netid = conf["netid"]
            self.mm2_ip = f"{self.protocol}://{ip}:{port}"
        else:
            logger.error(f"Komodefi SDK config not found at {config}!")
            raise SystemExit(1)
        self.testing = testing
        self.utils = Utils()
        self.files = Files(self.testing)
        self.templates = Templates()
        self.coins_config = self.utils.load_jsonfile(self.files.coins_config)
        version = self.version
        if version == "Error":
            logger.warning(f"Komodefi SDK is not running at {self.mm2_ip}!")
            raise SystemExit(1)
        

    # tuple, string, string -> list
    # returning orderbook for given trading pair
    def orderbook(self, pair):
        if isinstance(pair, str):
            pair = pair.split("_")
        base = pair[0]
        quote = pair[1]
        try:
            if base not in self.coins_config or quote not in self.coins_config:
                return self.templates.orderbook(base, quote, v2=True)
            if self.coins_config[base]["wallet_only"] or self.coins_config[quote]["wallet_only"]:
                return self.templates.orderbook(base, quote, v2=True)
        except Exception as e:
            logger.error(f"{type(e)} Error in [DexAPI.orderbook] for {pair}: {e}")
            return self.templates.orderbook(base, quote, v2=True)

        try:
            params = {
                "base": base,
                "rel": quote
            }
            r = self.rpc("orderbook", params, v2=True)
            if "result" in r.json():
                return json.loads(r.text)["result"]
            if "error_type" in r.json():
                error = r.json()['error_type']
                logger.debug(f"Error in [DexAPI.orderbook] for {pair}: {error}")
            else:
                logger.info(f"Error in [DexAPI.orderbook] for {pair}: {r.json()}")
        except Exception as e:
            logger.error(f"{type(e)} Error in [DexAPI.orderbook] for {pair}: {e}")
            logger.info(f"{type(e)} Error in [DexAPI.orderbook] for {pair}: {r.text}")
        return self.templates.orderbook(base, quote, v2=True)


    def rpc(self, method, params=None, v2=False, wss=False):
        try:
            if not params:
                params = {}
            body = {}
            if v2:
                body.update({
                    "mmrpc": "2.0",
                    "params": params
                })
            elif params:
                body.update(params)
            body.update({
                "userpass": self.userpass,
                "method": method
            })
            return requests.post(self.mm2_ip, json.dumps(body))
        except Exception as e:
            logger.info(method)
            logger.info(params)
            logger.error(f"Error in rpc: {e}")
            return {}

    @property
    def version(self):
        try:
            return self.rpc("version").json()["result"]
        except:
            return "Error"
