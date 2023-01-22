#!/usr/bin/env python3
import sys
import json
import time
import lib_db
from datetime import timezone

conn, cursor = lib_db.get_mysql()

def a_day_ago():
    return int(time.time()) - 24 * 60 * 60

def get_ext_swaps(day_since):
    
    cursor.execute(f"SELECT * FROM swaps WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    result = cursor.fetchall()
    with sqlite_conn:
        for x in result:
            x = list(x)
            x.append(int(x[2].replace(tzinfo=timezone.utc).timestamp()))
            sql = ''' REPLACE INTO swaps(id,started_at,uuid,taker_coin,taker_amount,
                                        taker_gui,taker_version,taker_pubkey,maker_coin,
                                        maker_amount,maker_gui,maker_version,maker_pubkey,
                                        time_stamp)
                      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
            cur = sqlite_conn.cursor()
            cur.execute(sql, x)
            sqlite_conn.commit()
    return len(result)
            
def get_ext_failed_swaps(day_since):
    
    cursor.execute(f"SELECT * FROM swaps_failed WHERE started_at >= NOW() - INTERVAL {day_since} DAY ORDER BY started_at;")
    result = cursor.fetchall()
    with sqlite_conn:
        for x in result:
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
    return len(result)


(99,
'2023-01-22 06:11:16',
'bd5cb648-893e-4dbf-abc4-8fbfc972dbe8',
'ZEC',
0.00027649,
'MakerPaymentValidateFailed',
"taker_swap:1148] !validate maker payment: utxo_common:3264] Provided payment tx output doesn't match expected Some(TransactionOutput { value: 37323083, script_pubkey: a9147302875f067fd3b9739ab4511d5c370e1106cce287 }) TransactionOutput { value: 373230833, script_pubkey: a9147302875f067fd3b9739ab4511d5c370e1106cce287 }",
'atomicDEX 0.6.0 Android; BT=1671157852',
'ffee29455',
'03d3e9f945aaf65b7360fe18802a3bf0feb69136cc4faa09409c733f9e3023f056',
'LCC',
3.73230834,
'TakerPaymentValidateFailed',
'lp_swap:316] Timeout (4771 > 4770)',
'mpm',
'2.1.9745_dev_7201146b3_Linux_CI',
'0315d9c51c657ab1be4ae9d3ab6e76a619d3bccfe830d5363fa168424c0d044732',
1674367876
)

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




def get_sum_ave_max(rows, uuid_idx, a_idx, b_idx):
    dict_a = {}
    dict_b = {}

    for row in rows:
        uuid = row[uuid_idx]
        a = row[a_idx]
        b = row[b_idx]
        if a in dict_a:
            dict_a[a].append(uuid)
        else:
            dict_a.update({a:[uuid]})
        if b in dict_b:
            dict_b[b].append(uuid)
        else:
            dict_b.update({b:[uuid]})

    a_active = len(dict_a)
    b_active = len(dict_b)

    max_a = 0
    max_b = 0
    for a in dict_a:
        if len(dict_a[a]) > max_a:
            max_a = len(dict_a[a])
    for b in dict_b:
        if len(dict_b[b]) > max_b:
            max_b = len(dict_b[b])

    return {
        "a_active": a_active,
        "b_active": b_active,
        "a_avg": len(rows)/a_active,
        "b_avg": len(rows)/b_active,
        "a_max": max_a,
        "b_max": max_b
    }

def get_failed_pubkey_data(rows):
    maker_dict = {}
    taker_dict = {}

    for row in rows:
        print(row)
        taker_error_type = row[5]
        taker = row[9]
        maker_error_type = row[12]
        maker = row[16]
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

def get_failed_coins_data(rows):
    maker_dict = {}
    taker_dict = {}

    for row in rows:
        print(row)
        taker_error_type = row[5]
        taker = row[3]
        maker_error_type = row[12]
        maker = row[10]
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

def get_failed_version_data(rows):
    maker_dict = {}
    taker_dict = {}

    for row in rows:
        print(row)
        taker_error_type = row[3]
        taker = row[8]
        maker_error_type = row[10]
        maker = row[15]
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

def get_failed_gui_data(rows):
    maker_dict = {}
    taker_dict = {}

    for row in rows:
        print(row)
        taker_error_type = row[5]
        taker = row[7]
        maker_error_type = row[12]
        maker = row[14]
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


def get_pubkey_data(rows):
    resp = get_sum_ave_max(rows, 1, 7, 12) # uuid, taker_pubkey, maker_pubkey

    return {
        "makers": resp["b_active"],
        "takers": resp["a_active"],
        "makers_avg": resp["b_avg"],
        "takers_avg": resp["a_avg"],
        "makers_max": resp["b_max"],
        "takers_max": resp["a_max"]
    }

def get_coins_data(rows):
    resp = get_sum_ave_max(rows, 1, 3, 8) # uuid, taker_coin, maker_coin

    return {
        "makers": resp["b_active"],
        "takers": resp["a_active"],
        "makers_avg": resp["b_avg"],
        "takers_avg": resp["a_avg"],
        "makers_max": resp["b_max"],
        "takers_max": resp["a_max"]
    }

def get_value_data(rows):
    pass

def get_version_data(rows):
    maker_dict = {}
    taker_dict = {}

    for row in rows:
        uuid = row[1]
        taker = row[6]
        maker = row[11]
        if not taker: taker = "None"
        if not maker: maker = "None"

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

def get_gui_data(rows):
    maker_dict = {}
    taker_dict = {}

    for row in rows:
        uuid = row[1]
        taker = row[5]
        maker = row[10]
        if not taker: taker = "None"
        if not maker: maker = "None"

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


def get_24hr_swaps(cur):
    cur.execute(f"SELECT * FROM swaps WHERE time_stamp > {a_day_ago()}")
    rows = cur.fetchall()
    print(f"{len(rows)} records returned for last 24 hours in swaps table")
    return rows

def get_24hr_failed_swaps(cur):
    cur.execute(f"SELECT * FROM failed_swaps WHERE time_stamp > {a_day_ago()}")
    rows = cur.fetchall()
    print(f"{len(rows)} records returned for last 24 hours in failed_swaps table")
    return rows

def mirror_mysql_swaps_db(day_since):
    swaps_count = get_ext_swaps(day_since)
    print(f"Swaps table update complete! {swaps_count} records updated")
    failed_swaps_count = get_ext_failed_swaps(day_since)
    print(f"Failed swaps table update complete! {failed_swaps_count} records updated")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("You need to specify action. Options are `update_db`, `update_json`")
        sys.exit()
    if sys.argv[1] not in ["update_db", "update_json"]:
        print("Incorrect action. Options are `update_db`, `update_json`")
        sys.exit()

    if sys.argv[1] == "update_db":
        sqlite_conn = lib_db.get_sqlite('seednode_data.db')
        with sqlite_conn:
            if sqlite_conn is not None:
                # create tables if not existing
                lib_db.create_sqlite_table(sqlite_conn, lib_db.sql_create_swaps_table)
                lib_db.create_sqlite_table(sqlite_conn, lib_db.sql_create_failed_swaps_table)
            else:
                print("Error! cannot create the database connection.")
            
            mirror_mysql_swaps_db(1)
            cursor.close()
            conn.close()

    if sys.argv[1] == "update_json":
        sqlite_conn = lib_db.get_sqlite('seednode_data.db')
        with sqlite_conn:
            cur = sqlite_conn.cursor()

            # Get 24 hour swap stats
            swaps = get_24hr_swaps(cur)

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
            failed_swaps = get_24hr_failed_swaps(cur)

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

