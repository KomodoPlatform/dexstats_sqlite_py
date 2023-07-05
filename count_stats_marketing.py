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

path_to_db = "/Users/antonlysakov/devel/MM2.db"

timestamp_2020_start = 1577836800
timestamp_2020_end   = 1609459200
timestamp_2021_start = 1609459201
timestamp_2021_end   = 1640995200
timestamp_2022_start = 1640995201
timestamp_2022_end   = 1672527599
timestamp_2023_start = 1672527600
timestamp_2023_end   = 1704063599

usd_prices = requests.get("https://prices.komodo.earth:1313/api/v2/tickers").json()

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


sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND started_at < ? AND is_success=1;", (timestamp_2022_start, timestamp_2022_end))
swap_statuses_2022 = sql_coursor.fetchall()
swaps_2022 = len(swap_statuses_2022)
usd_volume_2022 = usd_volume_for_swap_statuses(swap_statuses_2022, usd_prices)


sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND started_at < ? AND is_success=1;", (timestamp_2023_start, timestamp_2023_end))
swap_statuses_2023 = sql_coursor.fetchall()
swaps_2023 = len(swap_statuses_2023)
usd_volume_2023 = usd_volume_for_swap_statuses(swap_statuses_2023, usd_prices)

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
    swap_for_pair_a = sql_coursor.fetchall()
    pair_usd_volume_2021_a = usd_volume_for_swap_statuses(swap_for_pair_a, usd_prices)
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND taker_coin_ticker=? AND maker_coin_ticker=? AND is_success=1;", t)
    swap_for_pair_b = sql_coursor.fetchall()
    pair_usd_volume_2021_b = usd_volume_for_swap_statuses(swap_for_pair_b, usd_prices)
    pair_usd_volume_2021 = pair_usd_volume_2021_a + pair_usd_volume_2021_b
    pairs_volumes_2021[pair[0]+"_"+pair[1]] = int(pair_usd_volume_2021)

# sorting by volume
pairs_volumes_2021 = dict(sorted(pairs_volumes_2021.items(), key=lambda item: item[1], reverse=True))

pairs_volumes_2022 = {}

for pair in available_pairs:
    print("counting usd volume for " + str(pair))
    t = (timestamp_2022_start,pair[0],pair[1],)
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND maker_coin_ticker=? AND taker_coin_ticker=? AND is_success=1;", t)
    swap_for_pair_a = sql_coursor.fetchall()
    pair_usd_volume_2022_a = usd_volume_for_swap_statuses(swap_for_pair_a, usd_prices)
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND taker_coin_ticker=? AND maker_coin_ticker=? AND is_success=1;", t)
    swap_for_pair_b = sql_coursor.fetchall()
    pair_usd_volume_2022_b = usd_volume_for_swap_statuses(swap_for_pair_b, usd_prices)
    pair_usd_volume_2022 = pair_usd_volume_2022_a + pair_usd_volume_2022_b
    pairs_volumes_2022[pair[0]+"_"+pair[1]] = int(pair_usd_volume_2022)

# sorting by volume
pairs_volumes_2022 = dict(sorted(pairs_volumes_2022.items(), key=lambda item: item[1], reverse=True))

pairs_volumes_2023 = {}

for pair in available_pairs:
    print("counting usd volume for " + str(pair))
    t = (timestamp_2023_start,pair[0],pair[1],)
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND maker_coin_ticker=? AND taker_coin_ticker=? AND is_success=1;", t)
    swap_for_pair_a = sql_coursor.fetchall()
    pair_usd_volume_2023_a = usd_volume_for_swap_statuses(swap_for_pair_a, usd_prices)
    sql_coursor.execute("SELECT * FROM stats_swaps WHERE started_at > ? AND taker_coin_ticker=? AND maker_coin_ticker=? AND is_success=1;", t)
    swap_for_pair_b = sql_coursor.fetchall()
    pair_usd_volume_2023_b = usd_volume_for_swap_statuses(swap_for_pair_b, usd_prices)
    pair_usd_volume_2023 = pair_usd_volume_2023_a + pair_usd_volume_2023_b
    pairs_volumes_2023[pair[0]+"_"+pair[1]] = int(pair_usd_volume_2023)

# sorting by volume
pairs_volumes_2023 = dict(sorted(pairs_volumes_2021.items(), key=lambda item: item[1], reverse=True))

conn.close()


print("Total swaps 2020: " + str(swaps_2020))
print("USD volume 2020: " + str(usd_volume_2020))
print("Total swaps 2021: " + str(swaps_2021))
print("USD volume 2021: " + str(usd_volume_2021))
print("Total swaps 2022: " + str(swaps_2022))
print("USD volume 2022: " + str(usd_volume_2022))
print("Total swaps 2023: " + str(swaps_2023))
print("USD volume 2023: " + str(usd_volume_2023))
# print("Total new pairs in 2021: " + str(fresh_pairs_count))
# print("New pairs list: " + str(fresh_pairs))
print("Writing info about volumes for pairs into pair_volumes_year.json")
with open("pair_volumes_2021.json", "w+") as file:
    file.write(json.dumps(pairs_volumes_2021))
with open("pair_volumes_2022.json", "w+") as file:
    file.write(json.dumps(pairs_volumes_2022))
with open("pair_volumes_2023.json", "w+") as file:
    file.write(json.dumps(pairs_volumes_2023))
