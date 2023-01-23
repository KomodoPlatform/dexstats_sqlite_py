#!/usr/bin/env python3
import sys
import json
import time
import lib_db
from datetime import timezone
from lib_logger import logger


def a_day_ago():
    return int(time.time()) - 24 * 60 * 60

def get_ext_swaps(day_since):
    conn, cursor = lib_db.get_mysql()
    sqlite_conn = lib_db.get_sqlite('seednode_data.db')
    cursor.execute(f"SELECT * FROM swaps WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    result = cursor.fetchall()
    for x in result:
        with sqlite_conn:
            x = list(x)
            x.append(int(x[2].replace(tzinfo=timezone.utc).timestamp()))
            sql = ''' REPLACE INTO swaps(id,uuid,started_at,taker_coin,taker_amount,
                                        taker_gui,taker_version,taker_pubkey,maker_coin,
                                        maker_amount,maker_gui,maker_version,maker_pubkey,
                                        time_stamp)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            cur = sqlite_conn.cursor()
            cur.execute(sql, x)
            sqlite_conn.commit()
    cursor.close()
    conn.close()
    return len(result)
            
def get_ext_failed_swaps(day_since):
    conn, cursor = lib_db.get_mysql()
    sqlite_conn = lib_db.get_sqlite('seednode_data.db')
    cursor.execute(f"SELECT * FROM swaps_failed WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    result = cursor.fetchall()
    for x in result:
        with sqlite_conn:
            x = list(x)
            x.append(int(x[1].replace(tzinfo=timezone.utc).timestamp()))            
            sql = ''' REPLACE INTO failed_swaps(uuid,started_at,taker_coin,taker_amount,
                                        taker_error_type,taker_error_msg,taker_gui,taker_version,taker_pubkey,maker_coin,
                                        maker_amount,maker_error_type,maker_error_msg,maker_gui,maker_version,maker_pubkey,
                                        time_stamp)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            cur = sqlite_conn.cursor()
            cur.execute(sql, x)
            sqlite_conn.commit()
    cursor.close()
    conn.close()
    return len(result)


def get_error_message_id(error_msg):
    if not error_msg:
        return "None"
    if "Waited too long until" in error_msg:
        return "confirmation timeout"
    if "Timeout" in error_msg:
        return "tx timeout"
    if "time_dif" in error_msg:
        return "system time error"
    if "Provided payment tx output doesn't match expected" in error_msg:
        return "tx mismatch"
    if "JsonRpcError" in error_msg:
        return "JsonRpcError"
    if "required at least" in error_msg:
        return "balance error"
        
    return error_msg


def get_maker_taker_error_summary(rows, taker_idx, maker_idx):
    maker_dict = {}
    taker_dict = {}

    for row in rows:
        taker_error_type = row[5]
        taker = row[taker_idx]
        maker_error_type = row[12]
        maker = row[maker_idx]
        taker_error_msg = get_error_message_id(row[6])
        maker_error_msg = get_error_message_id(row[13])
        if not taker: taker = "None"
        if not maker: maker = "None"
        if not taker_error_type: taker_error_type = "None"
        if not maker_error_type: maker_error_type = "None"
        if not taker_error_msg: taker_error_msg = "None"
        if not maker_error_msg: maker_error_msg = "None"

        if taker in taker_dict:
            taker_dict[taker].append(f"{taker_error_type} ({taker_error_msg})")
        else:
            taker_dict.update({taker:[f"{taker_error_type} ({taker_error_msg})"]})

        if maker in maker_dict:
            maker_dict[maker].append(f"{maker_error_type} ({maker_error_msg})")
        else:
            maker_dict.update({maker:[f"{maker_error_type} ({maker_error_msg})"]})

    return {
        "makers": maker_dict,
        "takers": taker_dict
    }


def get_maker_taker_summary(rows, taker_idx, maker_idx):
    maker_dict = {}
    taker_dict = {}

    for row in rows:
        uuid = row[1]
        taker = row[taker_idx]
        maker = row[maker_idx]

        if taker in taker_dict:
            taker_dict[taker].append(uuid)
        else:
            taker_dict.update({taker:[uuid]})

        if maker in maker_dict:
            maker_dict[maker].append(uuid)
        else:
            maker_dict.update({maker:[uuid]})

    for maker in maker_dict:
        maker_dict[maker] = len(maker_dict[maker])
    for taker in taker_dict:
        taker_dict[taker] = len(taker_dict[taker])

    return {
        "makers": maker_dict,
        "takers": taker_dict
    }


def get_failed_pubkey_data(rows):
    return get_maker_taker_error_summary(rows, 9, 16)


def get_failed_coins_data(rows):
    return get_maker_taker_error_summary(rows, 3, 10)


def get_failed_version_data(rows):
    return get_maker_taker_error_summary(rows, 8, 15)


def get_failed_gui_data(rows):
    return get_maker_taker_error_summary(rows, 7, 14)


def get_sum_ave_max(rows, uuid_idx, taker_idx, maker_idx):
    takers = {}
    makers = {}

    for row in rows:
        uuid = row[uuid_idx]
        taker = row[taker_idx]
        maker = row[maker_idx]
        if taker in takers:
            takers[taker].append(uuid)
        else:
            takers.update({taker:[uuid]})
        if maker in makers:
            makers[maker].append(uuid)
        else:
            makers.update({maker:[uuid]})

    takers_active = len(takers)
    makers_active = len(makers)

    taker_max = 0
    maker_max = 0
    for taker in takers:
        if len(takers[taker]) > taker_max:
            taker_max = len(takers[taker])
    for maker in makers:
        if len(makers[maker]) > maker_max:
            maker_max = len(makers[maker])

    return {
        "takers_active": takers_active,
        "makers_active": makers_active,
        "taker_avg": len(rows)/takers_active,
        "maker_avg": len(rows)/makers_active,
        "taker_max": taker_max,
        "maker_max": maker_max
    }


def get_pubkey_data(rows):
    return get_sum_ave_max(rows, 1, 7, 12) # uuid, taker_pubkey, maker_pubkey


def get_coins_data(rows):
    return get_sum_ave_max(rows, 1, 3, 8) # uuid, taker_coin, maker_coin


def get_value_data(rows):
    pass


def get_version_data(rows):
    return get_maker_taker_summary(rows, 6, 11)


def get_gui_data(rows):
    return get_maker_taker_summary(rows, 5, 10)


def get_24hr_swaps(cur, days):
    cur.execute(f"SELECT * FROM swaps WHERE time_stamp > {a_day_ago() * days}")
    rows = cur.fetchall()
    logger.info(f"{len(rows)} records returned for last 24 hours in swaps table")
    return rows


def get_24hr_failed_swaps(cur, days):
    cur.execute(f"SELECT * FROM failed_swaps WHERE time_stamp > {a_day_ago() * days}")
    rows = cur.fetchall()
    logger.info(f"{len(rows)} records returned for last 24 hours in failed_swaps table")
    return rows


def mirror_mysql_swaps_db(day_since):
    swaps_count = get_ext_swaps(day_since)
    logger.info(f"Swaps table update complete! {swaps_count} records updated")
    failed_swaps_count = get_ext_failed_swaps(day_since)
    logger.info(f"Failed swaps table update complete! {failed_swaps_count} records updated")


def update_db(days):
    conn, cursor = lib_db.get_mysql()
    sqlite_conn = lib_db.get_sqlite('seednode_data.db')
    with sqlite_conn:
        if sqlite_conn is not None:
            # create tables if not existing
            lib_db.create_sqlite_table(sqlite_conn, lib_db.sql_create_swaps_table)
            lib_db.create_sqlite_table(sqlite_conn, lib_db.sql_create_failed_swaps_table)
        else:
            logger.error("Error! cannot create the database connection.")

        mirror_mysql_swaps_db(days)
    cursor.close()
    conn.close()


def update_json(days):
    sqlite_conn = lib_db.get_sqlite('seednode_data.db')
    with sqlite_conn:
        cur = sqlite_conn.cursor()

        # Get 24 hour swap stats
        swaps = get_24hr_swaps(cur, days)

        pubkey_data = get_pubkey_data(swaps)
        with open("24hr_pubkey_stats.json", "w+") as f:
            json.dump(pubkey_data, f, indent=4)

        coins_data = get_coins_data(swaps)
        with open("24hr_coins_stats.json", "w+") as f:
            json.dump(coins_data, f, indent=4)

        version_data = get_version_data(swaps)
        with open("24hr_version_stats.json", "w+") as f:
            json.dump(version_data, f, indent=4)

        gui_data = get_gui_data(swaps)
        with open("24hr_gui_stats.json", "w+") as f:
            json.dump(gui_data, f, indent=4)

        value_data = get_value_data(swaps)

        # Get 24 hour swap stats
        failed_swaps = get_24hr_failed_swaps(cur, days)

        failed_pubkey_data = get_failed_pubkey_data(failed_swaps)
        with open("24hr_failed_pubkey_stats.json", "w+") as f:
            json.dump(failed_pubkey_data, f, indent=4)

        failed_coins_data = get_failed_coins_data(failed_swaps)
        with open("24hr_failed_coins_stats.json", "w+") as f:
            json.dump(failed_coins_data, f, indent=4)

        failed_version_data = get_failed_version_data(failed_swaps)
        with open("24hr_failed_version_stats.json", "w+") as f:
            json.dump(failed_version_data, f, indent=4)

        failed_gui_data = get_failed_gui_data(failed_swaps)
        with open("24hr_failed_gui_stats.json", "w+") as f:
            json.dump(failed_gui_data, f, indent=4)


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
        update_db(days)

    if sys.argv[1] == "update_json":
        update_json(days)
