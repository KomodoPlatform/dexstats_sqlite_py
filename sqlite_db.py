#!/usr/bin/env python3
import sqlite3
import json
from decimal import Decimal
from datetime import datetime, timedelta
from logger import logger
import models
import const


class sqliteDB:
    def __init__(self, path_to_db, dict_format=False, cache_path="gecko_cache.json"):
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
        available_pairs = self.sql_cursor.fetchall()
        sorted_available_pairs = [tuple(sorted(pair)) for pair in available_pairs]
        logger.debug(
            f"{len(available_pairs)} distinct maker/taker pairs for last {days} days"
        )
        pairs = list(set(sorted_available_pairs))
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

    def get_last_price_for_pair(self, pair: str) -> float:
        """
        Takes a pair in the format `KMD_BTC` and returns the
        last trade price for that pair. Response scans both
        buy and sell swaps (e.g. KMD/BTC and BTC/KMD)
        """
        pair = models.Pair(pair)
        coin_a = pair.base
        coin_b = pair.quote

        sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{coin_a}' \
                AND taker_coin_ticker='{coin_b}' AND is_success=1 \
                ORDER BY started_at DESC LIMIT 1;"
        self.sql_cursor.execute(sql)
        resp = self.sql_cursor.fetchone()
        try:
            swap_price = Decimal(resp["taker_amount"]) / Decimal(resp["maker_amount"])
            swap_time = resp["started_at"]
        except Exception as e:
            logger.warning(f"Error getting swap_price for {pair}: {e}")
            swap_price = None
            swap_time = None

        sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{coin_b}' \
                AND taker_coin_ticker='{coin_a}' AND is_success=1 \
                ORDER BY started_at DESC LIMIT 1;"
        self.sql_cursor.execute(sql)
        resp2 = self.sql_cursor.fetchone()
        try:
            swap_price2 = Decimal(resp2["maker_amount"]) / Decimal(
                resp2["taker_amount"]
            )
            swap_time2 = resp2["started_at"]
        except Exception as e:
            logger.warning(f"Error getting swap_price2 for {pair}: {e}")
            swap_price2 = None
            swap_time2 = None
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


ENV_DB = sqliteDB(const.MM2_DB_PATH)
