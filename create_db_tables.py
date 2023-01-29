#!/usr/bin/env python3
import sys
import lib_sqlite_db
import lib_ts_db
from datetime import timezone
from lib_logger import logger
from lib_helper import days_ago

drop_first = False
if len(sys.argv) > 1:
    if sys.argv[1] == 'drop_first':
        drop_first = True

conn, cursor = lib_ts_db.get_timescaledb(True)
with conn:
    lib_ts_db.create_timescaledb_tables(conn, cursor, drop_first)
    logger.info("Timescale DBs created...")
    sqlite_conn = lib_sqlite_db.get_sqlite('seednode_swaps.db')
    lib_sqlite_db.create_sqlite_table(sqlite_conn, lib_sqlite_db.sql_create_swaps_table)

    sqlite_conn = lib_sqlite_db.get_sqlite('seednode_failed_swaps.db')
    lib_sqlite_db.create_sqlite_table(sqlite_conn, lib_sqlite_db.sql_create_failed_swaps_table)

conn.close()
