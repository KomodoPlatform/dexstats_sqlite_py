#!/usr/bin/env python3
import sys
import json
import time
import lib_db
from datetime import timezone
from lib_logger import logger


def a_day_ago():
    return int(time.time()) - 24 * 60 * 60


def get_swaps_data_from_mysql(day_since):
    conn, cursor = lib_db.get_mysql()
    cursor.execute(f"SELECT * FROM swaps WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    return cursor.fetchall()


def get_failed_swaps_data_from_mysql(day_since):
    conn, cursor = lib_db.get_mysql()
    cursor.execute(f"SELECT * FROM swaps_failed WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    return cursor.fetchall()


def import_mysql_swaps_into_sqlite(day_since, mysql_swaps_data):
    sqlite_conn = lib_db.get_sqlite('seednode_swaps.db')
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
    sqlite_conn = lib_db.get_sqlite('seednode_failed_swaps.db')
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


def get_timescaledb_uuids(day_since, table, conn, cursor):
    sql = f"SELECT uuid FROM {table} WHERE epoch > {a_day_ago()}"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def import_mysql_swaps_into_timescaledb(day_since, mysql_swaps_data):
    conn, cursor = lib_db.get_timescaledb()
    with conn:
        existing_uuids = get_timescaledb_uuids(day_since, "swaps", conn, cursor)
        imported = 0
        for x in mysql_swaps_data:
            row_data = list(x)[1:]
            if row_data[0] not in existing_uuids:
                row_data.append(int(row_data[1].replace(tzinfo=timezone.utc).timestamp()))
                try:
                    sql = f"INSERT INTO swaps \
                            (uuid, started_at, taker_coin, taker_amount, \
                             taker_gui, taker_version, taker_pubkey, maker_coin, \
                             maker_amount, maker_gui, maker_version, maker_pubkey, epoch) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                    cursor.execute(sql, row_data)
                    conn.commit()
                    imported += 1
                except Exception as e:
                    logger.error(f"Exception in [update_swaps_row]: {e}")
                    logger.error(f"[update_swaps_row] sql: {sql}")
                    logger.error(f"[update_swaps_row] row_data: {row_data}")
                    # input()
                    conn.rollback()
    conn.close()
    return imported


def import_mysql_failed_swaps_into_timescaledb(day_since, mysql_failed_swaps_data):
    conn, cursor = lib_db.get_timescaledb()
    with conn:
        existing_uuids = get_timescaledb_uuids(day_since, "failed_swaps", conn, cursor)
        imported = 0
        for x in mysql_failed_swaps_data:
            row_data = list(x)
            if row_data[0] not in existing_uuids:
                row_data.append(int(row_data[1].replace(tzinfo=timezone.utc).timestamp()))
                # taker_err_msg
                if row_data[5]:
                    row_data[5] = row_data[5].replace("'","")
                # maker_err_msg
                if row_data[12]:
                    row_data[12] = row_data[12].replace("'","")
                try:
                    sql = f"INSERT INTO failed_swaps \
                            (uuid, started_at, taker_coin, taker_amount, \
                             taker_error_type, taker_error_msg, \
                             taker_gui, taker_version, taker_pubkey, maker_coin, \
                             maker_amount, maker_error_type, maker_error_msg, \
                             maker_gui, maker_version, maker_pubkey, epoch) \
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, row_data)
                    conn.commit()
                    imported += 1
                except Exception as e:
                    logger.error(f"Exception in [update_swaps_failed_row]: {e}")
                    logger.error(f"[update_swaps_failed_row] sql: {sql}")
                    logger.error(f"[update_swaps_failed_row] row_data: {row_data}")
                    # input()
                    conn.rollback()
    conn.close()
    return imported


def mirror_mysql_swaps_db(day_since):
    # TODO: This should be sourced from every seednode running a similar instance of the docker container
    result = get_swaps_data_from_mysql(day_since)
    swaps_count_ts = import_mysql_swaps_into_timescaledb(day_since, result)
    swaps_count_lite = import_mysql_swaps_into_sqlite(day_since, result)
    logger.info(f"Sqlite swaps table update complete! {swaps_count_lite} records updated")
    logger.info(f"TimescaleDB swaps table update complete! {swaps_count_ts} records updated")


def mirror_mysql_failed_swaps_db(day_since):
    # TODO: This should be sourced from every seednode running a similar instance of the docker container
    result = get_failed_swaps_data_from_mysql(day_since)
    failed_swaps_count_ts = import_mysql_failed_swaps_into_timescaledb(day_since, result)
    failed_swaps_count_lite = import_mysql_failed_swaps_into_sqlite(day_since, result)
    logger.info(f"Sqlite failed swaps table update complete! {failed_swaps_count_lite} records updated")
    logger.info(f"TimescaleDB failed swaps table update complete! {failed_swaps_count_ts} records updated")


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
        taker = str(row[taker_idx])
        maker = str(row[maker_idx])
        taker_error_type = row[5]
        maker_error_type = row[12]
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
        taker = str(row[taker_idx])
        maker = str(row[maker_idx])

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
        taker = str(row[taker_idx])
        maker = str(row[maker_idx])
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
    sql = f"SELECT * FROM swaps WHERE epoch > {a_day_ago() * int(days)}"
    cur.execute(sql)
    rows = cur.fetchall()
    logger.info(f"{len(rows)} records returned for last 24 hours in swaps table")
    return rows


def get_24hr_failed_swaps(cur, days):
    sql = f"SELECT * FROM failed_swaps WHERE epoch > {a_day_ago() * int(days)}"
    cur.execute(sql)
    rows = cur.fetchall()
    logger.info(f"{len(rows)} records returned for last 24 hours in failed_swaps table")
    return rows


def update_json(days=1):
    conn, cursor = lib_db.get_timescaledb()

    with conn:
        # Get 24 hour swap stats
        swaps = get_24hr_swaps(cursor, days)

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
        failed_swaps = get_24hr_failed_swaps(cursor, days)

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

        logger.info("24hr json stats files updated!")
    conn.close()


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
        mirror_mysql_swaps_db(days)
        mirror_mysql_failed_swaps_db(days)

    if sys.argv[1] == "update_json":
        update_json(days)
