#!/usr/bin/env python3
import json
import lib_ts_db
from lib_logger import logger
from lib_helper import get_error_message_id

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


def get_failed_pubkey_data(rows):
    return get_maker_taker_error_summary(rows, 9, 16)


def get_failed_coins_data(rows):
    return get_maker_taker_error_summary(rows, 3, 10)


def get_failed_version_data(rows):
    return get_maker_taker_error_summary(rows, 8, 15)


def get_failed_gui_data(rows):
    return get_maker_taker_error_summary(rows, 7, 14)


def update_json(days=1, localhost=False):
    conn, cursor = lib_ts_db.get_timescaledb(localhost)

    with conn:
        # Get 24 hour swap stats
        swaps = lib_ts_db.get_24hr_swaps(cursor, days)

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
        failed_swaps = lib_ts_db.get_24hr_failed_swaps(cursor, days)

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


def get_swaps_cache_24hr_by_pair():
    with open('24hr_swaps_cache_by_pair.json', 'r') as json_file:
        return json.load(json_file)

def get_jsonfile_data(filename):
    try:
        with open(filename, 'r') as json_file:
            return json.load(json_file)
    except Exception as e:
        logger.warning(f"Failed to read {filename}: {e}")

def write_jsonfile_data(filename, data):
    try:
        with open(filename, 'w+') as json_file:
            json.dump(data, json_file, indent=2)
        logger.info(f"Updated {filename}!")
    except Exception as e:
        logger.warning(f"Failed to write {filename}: {e}")
