#!/usr/bin/env python3
import sys
import json
import time
import lib_db
from datetime import timezone
from lib_logger import logger

drop_first = False
if len(sys.argv) > 1:
    if sys.argv[1] == 'drop_first':
        drop_first = True

conn, cursor = lib_db.get_timescaledb(True)
with conn:
    lib_db.create_timescaledb_tables(conn, cursor, drop_first)

    sqlite_conn = lib_db.get_sqlite('seednode_swaps.db')
    lib_db.create_sqlite_table(sqlite_conn, lib_db.sql_create_swaps_table)

    sqlite_conn = lib_db.get_sqlite('seednode_failed_swaps.db')
    lib_db.create_sqlite_table(sqlite_conn, lib_db.sql_create_failed_swaps_table)

conn.close()
