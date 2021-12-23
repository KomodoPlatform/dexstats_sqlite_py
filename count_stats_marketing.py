import sqlite3
import requests
import json


from stats_utils import get_availiable_pairs, usd_volume_for_swap_statuses

# return True if there are no swaps before given timestamp
def check_if_pair_is_fresh(sql_coursor, pair, timestamp):
    t = (timestamp,pair[0],pair[1],)
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at < ? AND maker_coin_ticker=? AND taker_coin_ticker=? AND is_success=1;", t)
    swap_statuses = [dict(row) for row in sql_coursor.fetchall()]
    if len(swap_statuses) > 1:
        return False
    return True

path_to_db = "MM2.db"

timestamp_2020_start = 1577836800
timestamp_2020_end   = 1609459200
timestamp_2021_start = 1609459201
timestamp_2021_end   = 1640995200

usd_prices = requests.get("https://prices.komodo.live:1313/api/v2/tickers").json()

conn = sqlite3.connect(path_to_db)
conn.row_factory = sqlite3.Row
sql_coursor = conn.cursor()

sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND started_at < ? AND is_success=1;", (timestamp_2020_start, timestamp_2020_end))
swap_statuses_2020 = sql_coursor.fetchall()
swaps_2020 = len(swap_statuses_2020)
usd_volume_2020 = usd_volume_for_swap_statuses(swap_statuses_2020, usd_prices)


sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND started_at < ? AND is_success=1;", (timestamp_2021_start, timestamp_2021_end))
swap_statuses_2021 = sql_coursor.fetchall()
swaps_2021 = len(swap_statuses_2021)
usd_volume_2021 = usd_volume_for_swap_statuses(swap_statuses_2021, usd_prices)


available_pairs = get_availiable_pairs(path_to_db)
# print("Total pairs: " + str(len(available_pairs)))
# pairs_count = 0
# fresh_pairs_count = 0
# fresh_pairs = []
# for pair in available_pairs:
#     print("Checking pair " + str(pair) + " " + str(pairs_count))
#     pairs_count += 1
#     if check_if_pair_is_fresh(sql_coursor, pair, 1609459201):
#         fresh_pairs_count += 1
#         fresh_pairs.append(pair)

pairs_volumes_2021 = {}

for pair in available_pairs:
    print("counting usd volume for " + str(pair))
    t = (timestamp_2021_start,pair[0],pair[1],)
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND maker_coin_ticker=? AND taker_coin_ticker=? AND is_success=1;", t)
    swap_for_pair = sql_coursor.fetchall()
    pair_usd_volume_2021 = usd_volume_for_swap_statuses(swap_for_pair, usd_prices)
    pairs_volumes_2021[pair[0]+"_"+pair[1]] = int(pair_usd_volume_2021)

# sorting by volume
pairs_volumes_2021 = dict(sorted(pairs_volumes_2021.items(), key=lambda item: item[1], reverse=True))

conn.close()


print("Total swaps 2020: " + str(swaps_2020))
print("USD volume 2020: " + str(usd_volume_2020))
print("Total swaps 2021: " + str(swaps_2021))
print("USD volume 2021: " + str(usd_volume_2021))
# print("Total new pairs in 2021: " + str(fresh_pairs_count))
# print("New pairs list: " + str(fresh_pairs))
print("Writing info about volumes for pairs into pair_volumes.json")
with open("pair_volumes.json", "w+") as file:
    file.write(json.dumps(pairs_volumes_2021))

