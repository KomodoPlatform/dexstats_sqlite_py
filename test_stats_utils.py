#!/usr/bin/env python3
import stats_utils
import lib_ts_db
import lib_helper

swap_statuses = [
    {'id': 1, 'uuid': 'xxx', 'taker_coin': 'KMD', 'taker_amount': 10, 'taker_gui': 'xxx', 'taker_version': 'xxx', 'taker_pubkey': 'xxx', 'maker_coin': 'SYS', 'maker_amount': 5, 'maker_gui': 'xxx', 'maker_version': 'xxx', 'maker_pubkey': 'xxx', 'started_at': '1970-01-01T00:00:00+00:00', 'epoch': 0, 'trade_type': 'sell'},
    {'id': 2, 'uuid': 'xxx', 'taker_coin': 'SYS', 'taker_amount': 10, 'taker_gui': 'xxx', 'taker_version': 'xxx', 'taker_pubkey': 'xxx', 'maker_coin': 'KMD', 'maker_amount': 5, 'maker_gui': 'xxx', 'maker_version': 'xxx', 'maker_pubkey': 'xxx', 'started_at': '1970-01-01T00:00:00+00:00', 'epoch': 0, 'trade_type': 'buy'},
    {'id': 3, 'uuid': 'xxx', 'taker_coin': 'SYS', 'taker_amount': 10, 'taker_gui': 'xxx', 'taker_version': 'xxx', 'taker_pubkey': 'xxx', 'maker_coin': 'KMD', 'maker_amount': 5, 'maker_gui': 'xxx', 'maker_version': 'xxx', 'maker_pubkey': 'xxx', 'started_at': '1970-01-01T00:00:00+00:00', 'epoch': 0, 'trade_type': 'buy'}
]


def test_count_volumes_and_prices(swap_statuses):
    resp = stats_utils.count_volumes_and_prices(swap_statuses)
    assert resp["base_volume"] == 20
    assert resp["quote_volume"] == 25

if __name__ == '__main__':
    #test_count_volumes_and_prices(swap_statuses)
    conn, cursor = lib_ts_db.get_timescaledb(True)
    #print(lib_ts_db.get_ts_version(cursor))
    #print(lib_ts_db.get_hypertables(cursor))
    resp = lib_ts_db.get_bidirectional_swaps_json_by_pair_since_by_date(cursor, lib_helper.days_ago(7), ["KMD", "SYS"])
    print(resp)
    resp = lib_ts_db.get_pair_volumes_by_day_since_bucket(cursor, lib_helper.days_ago(7), ["KMD", "SYS"])
    print(resp)

