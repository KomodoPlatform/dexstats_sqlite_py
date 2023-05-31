#!/usr/bin/env python3
import os
import json
from dotenv import load_dotenv
from logger import logger
import stats_utils
import sqlite_db

load_dotenv()
MM2_DB_PATH = os.getenv('MM2_DB_PATH')

class CacheLoops():
    def __init__(self):
        pass

    def refresh_gecko_cache(self):
        gecko_data = stats_utils.get_data_from_gecko()
        if "error" not in gecko_data:
            with open('gecko_cache.json', 'w+') as f:
                json.dump(gecko_data, f, indent=4)
                logger.info("Updated gecko_cache.json")
        else:
            logger.warning(f"Error in [cache_gecko_data]: {gecko_data}")


    def refresh_summary_cache(self):
        # Takes around 1:20 minute to run with 300 pairs
        DB = sqlite_db.sqliteDB(MM2_DB_PATH)
        pairs = DB.get_pairs()
        summary_data = []
        for pair in pairs:
            summary_data.append(stats_utils.summary_for_pair(pair, MM2_DB_PATH, days=1))
        with open('summary_cache.json', 'w+') as f:
            json.dump(summary_data, f, indent=4)
            logger.info("Updated summary_cache.json")


    def refresh_ticker_cache(self):
        # Takes around 1:20 minute to run with 300 pairs
        DB = sqlite_db.sqliteDB(MM2_DB_PATH)
        pairs = DB.get_pairs()
        ticker_data = []
        for pair in pairs:
            ticker_data.append(stats_utils.ticker_for_pair(pair, MM2_DB_PATH))
        with open('ticker_cache.json', 'w+') as f:
            json.dump(ticker_data, f, indent=4)
            logger.info("Updated ticker_cache.json")


    def refresh_adex_cache(self):
        data = stats_utils.atomicdex_info(MM2_DB_PATH)
        if data:
            with open('adex_cache.json', 'w+') as cache_file:
                json.dump(data, cache_file, indent=4)
                logger.info("Updated adex_cache.json")


    def refresh_adex_fortnight_cache(self):
        data = stats_utils.atomicdex_timespan_info(MM2_DB_PATH, 14)
        if data:
            with open('adex_fortnight_cache.json', 'w+') as cache_file:
                json.dump(data, cache_file, indent=4)
                logger.info("Updated adex_fortnight_cache.json")
