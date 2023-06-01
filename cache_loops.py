#!/usr/bin/env python3
import json
from logger import logger
import stats_utils
import sqlite_db
import const


class CacheLoops():
    def __init__(self):
        pass

    def refresh_gecko_cache(self) -> dict:
        gecko_data = stats_utils.get_data_from_gecko()
        if "KMD" in gecko_data:
            with open('gecko_cache.json', 'w+') as f:
                json.dump(gecko_data, f, indent=4)
                logger.info("Updated gecko_cache.json")
                return {"result": "Updated gecko_cache.json"}
        else:
            return {"error": "Failed to update gecko_cache.json"}

    def refresh_summary_cache(self, days=1):
        # Takes around 1:20 minute to run with 300 pairs
        result = False
        DB = sqlite_db.sqliteDB(const.MM2_DB_PATH)
        pairs = DB.get_pairs()
        summary_data = []
        for pair in pairs:
            summary_data.append(stats_utils.summary_for_pair(pair, days, DB))
        with open('summary_cache.json', 'w+') as f:
            json.dump(summary_data, f, indent=4)
            logger.info("Updated summary_cache.json")
            result = True
        DB.close()
        return result

    def refresh_ticker_cache(self):
        result = False
        # Takes around 1:20 minute to run with 300 pairs
        DB = sqlite_db.sqliteDB(const.MM2_DB_PATH)
        pairs = DB.get_pairs()
        ticker_data = []
        for pair in pairs:
            ticker_data.append(stats_utils.ticker_for_pair(pair, DB))
        with open('ticker_cache.json', 'w+') as f:
            json.dump(ticker_data, f, indent=4)
            logger.info("Updated ticker_cache.json")
            result = True
        DB.close()
        return result

    def refresh_adex_cache(self):
        DB = sqlite_db.sqliteDB(const.MM2_DB_PATH)
        logger.info("Updating adex_cache.json")
        data = stats_utils.atomicdex_info(DB)
        if data:
            with open('adex_cache.json', 'w+') as cache_file:
                json.dump(data, cache_file, indent=4)
                logger.info("Updated adex_cache.json")
                result = True
        else:
            result = False
        DB.close()
        return result

    def refresh_adex_fortnight_cache(self):
        DB = sqlite_db.sqliteDB(const.MM2_DB_PATH)
        data = stats_utils.atomicdex_timespan_info(DB)
        if data:
            with open('adex_fortnight_cache.json', 'w+') as cache_file:
                json.dump(data, cache_file, indent=4)
                logger.info("Updated adex_fortnight_cache.json")
                result = True
        else:
            result = False
        DB.close()
        return result
