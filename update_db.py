#!/usr/bin/env python3
import sys
import json
import time
import lib_json
import lib_ts_db
import lib_mysql_db
from lib_logger import logger


def mirror_mysql_swaps_db(day_since, localhost=False):
    # TODO: This should be sourced from every seednode running a similar instance of the docker container
    result = lib_mysql_db.get_swaps_data_from_mysql(day_since)
    swaps_count_ts = lib_ts_db.import_mysql_swaps_into_timescaledb(day_since, result, localhost)
    logger.info(f"TimescaleDB swaps table update complete! {swaps_count_ts} records updated")


def mirror_mysql_failed_swaps_db(day_since, localhost=False):
    # TODO: This should be sourced from every seednode running a similar instance of the docker container
    result = lib_mysql_db.get_failed_swaps_data_from_mysql(day_since)
    failed_swaps_count_ts = lib_ts_db.import_mysql_failed_swaps_into_timescaledb(day_since, result, localhost)
    logger.info(f"TimescaleDB failed swaps table update complete! {failed_swaps_count_ts} records updated")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        logger.info("You need to specify action. Options are `update_db`, `update_json`")
        sys.exit()
    if sys.argv[1] not in ["update_db", "update_json"]:
        logger.info("Incorrect action. Options are `update_db`, `update_json`")
        sys.exit()

    if len(sys.argv) == 3:
        days = sys.argv[2]
    else:
        days = 1

    if sys.argv[1] == "update_db":
        mirror_mysql_swaps_db(days, True)
        mirror_mysql_failed_swaps_db(days, True)

    if sys.argv[1] == "update_json":
        lib_json.update_json(days, True)
