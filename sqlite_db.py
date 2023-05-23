#!/usr/bin/env python3
import sqlite3
import requests
import json
import logging
from decimal import Decimal
from datetime import datetime, timedelta
from collections import OrderedDict
from logger import logger

class sqliteDB():
    def __init__(self, path_to_db, dict_format=False):
        self.conn = sqlite3.connect(path_to_db)
        if dict_format: self.conn.row_factory = sqlite3.Row
        self.sql_cursor = self.conn.cursor()
        with open('gecko_cache.json', 'r') as json_file:
            self.gecko_data = json.load(json_file)

    def close(self):
        self.conn.close()
        
    # getting list of pairs with amount of swaps > 0 from db (list of tuples)
    # string -> list (of base, rel tuples)
    def get_pairs(self, days=7):
        # Without a time limit, this is returning too many pairs to send a response before timeout.
        timestamp = int((datetime.now() - timedelta(days)).strftime("%s"))
        sql = f"SELECT DISTINCT maker_coin_ticker, taker_coin_ticker FROM stats_swaps \
                WHERE started_at > {timestamp} AND is_success=1;"
        self.sql_cursor.execute(sql)
        available_pairs = self.sql_cursor.fetchall()
        sorted_available_pairs = [tuple(sorted(pair)) for pair in available_pairs]
        logger.info(f"{len(available_pairs)} distinct maker/taker pairs for last {days} days")
        pairs = list(set(sorted_available_pairs))
        adjusted = []
        for pair in pairs:
            if pair[0] in self.gecko_data:
                if pair[1] in self.gecko_data:
                    if self.gecko_data[pair[1]]["usd_market_cap"] < self.gecko_data[pair[0]]["usd_market_cap"]:
                        pair = (pair[1], pair[0])
                else:
                    pair = (pair[1], pair[0])
            adjusted.append(pair)
        return adjusted


    # tuple, integer - > list (with swap status dicts)
    # select from DB swap statuses for desired pair with timestamps > than provided
    def get_swaps_for_pair(self, pair, timestamp=None):
        if not timestamp:
            timestamp = int((datetime.now() - timedelta(days=1)).strftime("%s"))
        t = (timestamp,pair[0],pair[1],)
        self.sql_cursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND maker_coin_ticker=? AND taker_coin_ticker=? AND is_success=1;", t)
        swap_statuses_a_b = [dict(row) for row in self.sql_cursor.fetchall()]
        for swap in swap_statuses_a_b:
            swap["trade_type"] = "buy"
        self.sql_cursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND taker_coin_ticker=? AND maker_coin_ticker=? AND is_success=1;", t)
        swap_statuses_b_a = [dict(row) for row in self.sql_cursor.fetchall()]
        for swap in swap_statuses_b_a:
            temp_maker_amount = swap["maker_amount"]
            swap["maker_amount"] = swap["taker_amount"]
            swap["taker_amount"] = temp_maker_amount
            swap["trade_type"] = "sell"
        swap_statuses = swap_statuses_a_b + swap_statuses_b_a
        return swap_statuses


    # Last Trade Price
    def get_last_price_for_pair(self, pair):
        if isinstance(pair, str):
            pair = tuple(pair.split("_"))
        sorted_pair = tuple(sorted(pair))
        coin_a = pair[0]
        coin_b = pair[1]

        sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{coin_a}' and taker_coin_ticker='{coin_b}' AND  is_success=1 ORDER BY started_at LIMIT 1;"
        self.sql_cursor.execute(sql)
        resp = self.sql_cursor.fetchone()
        try:
            swap_price = Decimal(resp["taker_amount"]) / Decimal(resp["maker_amount"])
            swap_time = resp["started_at"]
        except:
            swap_price = None

        sql = f"SELECT * FROM stats_swaps WHERE maker_coin_ticker='{coin_b}' and taker_coin_ticker='{coin_a}' AND  is_success=1 ORDER BY started_at LIMIT 1;"
        self.sql_cursor.execute(sql)
        resp2 = self.sql_cursor.fetchone()
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
        if pair[0] != sorted_pair[0] and swap_price != 0:
            swap_price = 1/swap_price
        return swap_price

    def get_adex_summary(self):
        timestamp_24h_ago = int((datetime.now() - timedelta(1)).strftime("%s"))
        timestamp_30d_ago = int((datetime.now() - timedelta(30)).strftime("%s"))
        self.sql_cursor.execute("SELECT * FROM stats_swaps WHERE is_success=1;")
        swaps_all_time = len(self.sql_cursor.fetchall())
        self.sql_cursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND is_success=1;", (timestamp_24h_ago,))
        swaps_24h = len(self.sql_cursor.fetchall())
        self.sql_cursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND is_success=1;", (timestamp_30d_ago,))
        swaps_30d = len(self.sql_cursor.fetchall())
        return {"swaps_all_time": swaps_all_time, "swaps_24h": swaps_24h, "swaps_30d": swaps_30d}