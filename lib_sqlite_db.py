#!/usr/bin/env python3
import os
import sqlite3
from dotenv import load_dotenv
from datetime import timezone
from lib_logger import logger
from lib_helper import days_ago

load_dotenv()

def get_sqlite(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.error(e)
    return conn


def import_mysql_swaps_into_sqlite(day_since, mysql_swaps_data):
    sqlite_conn = get_sqlite('seednode_swaps.db')
    with sqlite_conn:
        imported = 0
        for x in mysql_swaps_data:
            x = list(x)
            x.append(int(x[2].replace(tzinfo=timezone.utc).timestamp()))
            sql = ''' REPLACE INTO swaps(id,uuid,started_at,taker_coin,taker_amount,
                                        taker_gui,taker_version,taker_pubkey,maker_coin,
                                        maker_amount,maker_gui,maker_version,maker_pubkey,
                                        epoch)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            cur = sqlite_conn.cursor()
            cur.execute(sql, x)
            sqlite_conn.commit()
            imported += 1
    return imported

            
def import_mysql_failed_swaps_into_sqlite(day_since, mysql_failed_swaps_data):
    sqlite_conn = get_sqlite('seednode_failed_swaps.db')
    with sqlite_conn:
        # get existing uuids
        imported = 0
        for x in mysql_failed_swaps_data:
            x = list(x)
            x.append(int(x[1].replace(tzinfo=timezone.utc).timestamp()))            
            sql = ''' REPLACE INTO failed_swaps(uuid,started_at,taker_coin,taker_amount,
                                        taker_error_type,taker_error_msg,taker_gui,taker_version,taker_pubkey,maker_coin,
                                        maker_amount,maker_error_type,maker_error_msg,maker_gui,maker_version,maker_pubkey,
                                        epoch)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            cur = sqlite_conn.cursor()
            cur.execute(sql, x)
            sqlite_conn.commit()
            imported += 1
    return imported