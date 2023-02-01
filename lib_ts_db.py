#!/usr/bin/env python3
import os
import psycopg2
from dotenv import load_dotenv
from datetime import timezone, datetime
import pytz

import lib_helper
from lib_logger import logger
from lib_helper import days_ago

load_dotenv()


def get_timescaledb(localhost=False):
    if localhost: host = os.getenv("POSTGRES_HOST")
    else: host = os.getenv("DOCKER_DB_SERVICE_NAME")
    conn = psycopg2.connect(
        host=host,
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DATABASE")
    )
    cursor = conn.cursor()
    return conn, cursor


def create_timescaledb_tables(conn, cursor, drop_tables=False):

    drop_tables_sql = (
        """
        DROP TABLE swaps;
        """,
        """
        DROP TABLE failed_swaps;
        """
    )

    create_hypertables_sql = (
        """
        CREATE TABLE IF NOT EXISTS swaps (
            id BIGSERIAL,
            uuid TEXT NOT NULL,
            taker_coin TEXT NOT NULL,
            taker_amount DECIMAL NOT NULL,
            taker_gui TEXT,
            taker_version TEXT,
            taker_pubkey TEXT,
            taker_coin_usd_price DECIMAL DEFAULT 0,
            maker_coin TEXT NOT NULL,
            maker_amount DECIMAL NOT NULL,
            maker_gui TEXT,
            maker_version TEXT,
            maker_pubkey TEXT,
            maker_coin_usd_price DECIMAL DEFAULT 0,
            started_at TIMESTAMPTZ NOT NULL,
            epoch INT NOT NULL,
            UNIQUE (id, epoch)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS failed_swaps (
            id BIGSERIAL,
            uuid TEXT NOT NULL,
            taker_coin TEXT,
            taker_amount DECIMAL,
            taker_gui TEXT,
            taker_version TEXT,
            taker_pubkey TEXT,
            taker_error_type TEXT,
            taker_error_msg TEXT,
            maker_coin TEXT,
            maker_amount DECIMAL,
            maker_gui TEXT,
            maker_version TEXT,
            maker_pubkey TEXT,
            maker_error_type TEXT,
            maker_error_msg TEXT,
            started_at TIMESTAMPTZ NOT NULL,
            epoch INT NOT NULL,
            UNIQUE (id, epoch)
        );
        """,
        """
        SELECT create_hypertable('swaps','epoch', chunk_time_interval => 86400);
        """,
        """
        SELECT create_hypertable('failed_swaps','epoch', chunk_time_interval => 86400);
        """
    )
    create_hypertable_indexes_sql = (
        """
        CREATE INDEX idx_swaps_pair_time ON swaps (taker_coin, maker_coin, epoch DESC);
        """,
        """
        CREATE INDEX idx_swaps_maker_coin_time ON swaps (maker_coin, epoch DESC);
        """,
        """
        CREATE INDEX idx_swaps_taker_coin_time ON swaps (taker_coin, epoch DESC);
        """,
        """
        CREATE INDEX idx_swaps_maker_version_time ON swaps (maker_version, epoch DESC);
        """,
        """
        CREATE INDEX idx_swaps_taker_version_time ON swaps (taker_version, epoch DESC);
        """,
        """
        CREATE INDEX idx_swaps_maker_gui_time ON swaps (maker_gui, epoch DESC);
        """,
        """
        CREATE INDEX idx_swaps_taker_gui_time ON swaps (taker_gui, epoch DESC);
        """,
        """
        CREATE INDEX idx_swaps_maker_pubkey_time ON swaps (maker_pubkey, epoch DESC);
        """,
        """
        CREATE INDEX idx_swaps_taker_pubkey_time ON swaps (taker_pubkey, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_pair_time ON failed_swaps (taker_coin, maker_coin, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_maker_coin_time ON failed_swaps (maker_coin, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_taker_coin_time ON failed_swaps (taker_coin, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_maker_version_time ON failed_swaps (maker_version, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_taker_version_time ON failed_swaps (taker_version, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_maker_gui_time ON failed_swaps (maker_gui, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_taker_gui_time ON failed_swaps (taker_gui, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_maker_pubkey_time ON failed_swaps (maker_pubkey, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_taker_pubkey_time ON failed_swaps (taker_pubkey, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_maker_error_type_time ON failed_swaps (maker_error_type, epoch DESC);
        """,
        """
        CREATE INDEX idx_failed_swaps_taker_error_type_time ON failed_swaps (taker_error_type, epoch DESC);
        """
    )
    # TODO: Add materialised views for pairs
    if drop_tables:
        for i in drop_tables_sql:
            try:
                print(i)
                cursor.execute(i)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(e)

    for i in create_hypertables_sql:
        try:
            print(i)
            cursor.execute(i)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)

    for i in create_hypertable_indexes_sql:
        try:
            print(i)
            cursor.execute(i)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)


def get_swap_uuids_since(cursor, epoch):
    sql = f"SELECT uuid FROM swaps WHERE epoch > {epoch};"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def get_failed_swap_uuids_since(cursor, epoch):
    sql = f"SELECT uuid FROM failed_swaps WHERE epoch > {epoch};"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def import_mysql_swaps_into_timescaledb(days, mysql_swaps_data, localhost=False):
    conn, cursor = get_timescaledb(localhost)
    with conn:
        existing_uuids = get_swap_uuids_since(cursor, lib_helper.days_ago(days))
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


def import_mysql_failed_swaps_into_timescaledb(days, mysql_failed_swaps_data, localhost=False):
    conn, cursor = get_timescaledb(localhost)
    with conn:
        existing_uuids = get_failed_swap_uuids_since(cursor, lib_helper.days_ago(days))
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
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
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


def get_swaps_since(cursor, epoch):
    sql = f"SELECT * FROM swaps WHERE epoch > {epoch};"
    cursor.execute(sql)
    rows = cursor.fetchall()
    logger.info(f"{len(rows)} records returned since {epoch} from swaps table")
    return rows


def get_swaps_for_pair_since(cursor, epoch, pair):
    print(pair)
    sql = f"SELECT * FROM swaps WHERE epoch > {epoch} AND maker_coin = '{pair[0]}' AND taker_coin='{pair[1]}';"
    cursor.execute(sql)
    rows = cursor.fetchall()
    logger.info(f"{len(rows)} records returned since {epoch} for {'/'.join(pair)} from swaps table")
    return rows


def get_swaps_json_for_pair_since(cursor, epoch, pair):
    sql = f"SELECT row_to_json(row) FROM (SELECT * FROM swaps WHERE epoch > {epoch} AND maker_coin = '{pair[0]}' AND taker_coin = '{pair[1]}') row;"
    logger.info(sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    rows = [i[0] for i in rows]
    logger.info(f"{len(rows)} records returned since {epoch} for {'/'.join(pair)} from swaps table")
    return rows

def get_bidirectional_swaps_json_by_pair_since(cursor, epoch, pair):
    pair = lib_helper.get_pair_tickers(pair)
    sql = f"SELECT row_to_json(t) FROM ( \
                SELECT * FROM swaps \
                WHERE epoch > {epoch} AND maker_coin IN ('{pair[0]}', '{pair[1]}') \
                AND taker_coin IN ('{pair[0]}', '{pair[1]}')) t;"
    cursor.execute(sql)
    rows = cursor.fetchall()
    rows = [i[0] for i in rows]

    logger.info(f"{len(rows)} records returned since {epoch} for {'/'.join(pair)} & {pair[1]}/{pair[0]}  from swaps table")
    return rows

def get_bidirectional_swaps_json_by_pair_since_by_date(cursor, epoch, pair):
    pair = lib_helper.get_pair_tickers(pair)
    sql = f"SELECT row_to_json(t) FROM ( \
                SELECT * FROM swaps \
                WHERE epoch > {epoch} AND maker_coin IN ('{pair[0]}', '{pair[1]}') \
                AND taker_coin IN ('{pair[0]}', '{pair[1]}')) t;"
    cursor.execute(sql)
    rows = cursor.fetchall()
    rows = [i[0] for i in rows]

    logger.info(f"{len(rows)} records returned since {epoch} for {'/'.join(pair)} & {pair[1]}/{pair[0]}  from swaps table")
    return rows

# HYPER TABLE QUERY
def get_pair_volumes_by_day_since_bucket(cursor, epoch, pair):
    sql = f"SELECT row_to_json(t) FROM ( \
                SELECT time_bucket(86400, epoch) AS date,  \
                    taker_coin, SUM(taker_amount) as sum_taker_volume, \
                    MIN(maker_amount/taker_amount) as min_taker_price, \
                    MAX(maker_amount/taker_amount) as max_taker_price, \
                    maker_coin, SUM(maker_amount) as sum_maker_volume, \
                    MIN(taker_amount/maker_amount) as min_maker_price, \
                    MAX(taker_amount/maker_amount) as max_maker_price, \
                    COUNT(*) as pair_swap_count  \
                FROM swaps \
                WHERE epoch > {epoch} AND maker_coin IN ('{pair[0]}', '{pair[1]}') \
                            AND taker_coin IN ('{pair[0]}', '{pair[1]}') \
                GROUP BY date, maker_coin, taker_coin \
                ORDER BY date DESC \
            ) t;"
    #logger.info(sql)
    cursor.execute(sql)
    rows = cursor.fetchall()
    rows = [i[0] for i in rows]
    for i in rows:
        i.update({"date": datetime.fromtimestamp(i["date"], tz = pytz.UTC).strftime('%Y-%m-%d')})
    return rows


def get_failed_swaps_since(cursor, epoch):
    sql = f"SELECT * FROM failed_swaps WHERE epoch >= {epoch};"
    cursor.execute(sql)
    rows = cursor.fetchall()
    logger.info(f"{len(rows)} records returned since {epoch} from failed_swaps table")
    return rows


def get_swaps_between(cursor, start_epoch, end_epoch):
    sql = f"SELECT * FROM swaps WHERE epoch >= {start_epoch} AND epoch < {end_epoch};"
    cursor.execute(sql)
    rows = cursor.fetchall()
    logger.info(f"{len(rows)} records returned between {start_epoch} - {end_epoch} from swaps table")
    return rows


def get_failed_swaps_between(cursor, start_epoch, end_epoch):
    sql = f"SELECT * FROM failed_swaps WHERE epoch >= {start_epoch} AND epoch < {end_epoch};"
    cursor.execute(sql)
    rows = cursor.fetchall()
    logger.info(f"{len(rows)} records returned between {start_epoch} - {end_epoch} from failed_swaps table")
    return rows


def get_swaps_json_since(cursor, epoch):
    sql = f"SELECT row_to_json(row) FROM (SELECT * FROM swaps WHERE epoch >= {epoch}) row;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def get_failed_swaps_json_since(cursor, epoch):
    sql = f"SELECT row_to_json(row) FROM (SELECT * FROM failed_swaps WHERE epoch >= {epoch}) row;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def get_swaps_json_between(cursor, start_epoch, end_epoch):
    sql = f"SELECT row_to_json(row) FROM (SELECT * FROM swaps WHERE epoch >= {start_epoch} AND epoch < {end_epoch}) row;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def get_failed_swaps_json_between(cursor, start_epoch, end_epoch):
    sql = f"SELECT row_to_json(row) FROM (SELECT * FROM failed_swaps WHERE epoch >= {start_epoch} AND epoch < {end_epoch}) row;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def get_pairs_since(cursor, epoch):
    sql = f"SELECT DISTINCT CONCAT(maker_coin, '/', taker_coin) as trading_pair \
                FROM swaps \
                WHERE epoch >= {epoch} \
                GROUP BY maker_coin, taker_coin \
                ORDER BY trading_pair;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def get_pairs_between(cursor, start_epoch, end_epoch):
    sql = f"SELECT DISTINCT CONCAT(maker_coin, '/', taker_coin) as trading_pair \
                FROM swaps \
                WHERE epoch >= {start_epoch} AND epoch < {end_epoch} \
                GROUP BY maker_coin, taker_coin \
                ORDER BY trading_pair;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def get_swaps_stats_since(cursor, epoch):
    last_price_data = get_pairs_last_price_since(cursor, epoch)
    sql = f"SELECT row_to_json(row) FROM ( \
                SELECT \
                    CONCAT(maker_coin, '/', taker_coin) as trading_pair, \
                    maker_coin as base_currency, taker_coin as quote_currency, \
                    SUM(maker_amount) AS base_volume, SUM(taker_amount) AS quote_volume, \
                    MAX(maker_amount) AS max_maker_amount, MAX(taker_amount) AS max_taker_amount, \
                    AVG(taker_amount/maker_amount) AS avg_maker_price, AVG(maker_amount/taker_amount) AS avg_taker_price, \
                    MAX(taker_amount/maker_amount) AS max_maker_price, MAX(maker_amount/taker_amount) AS max_taker_price, \
                    MIN(taker_amount/maker_amount) AS min_maker_price, MIN(maker_amount/taker_amount) AS min_taker_price, \
                    COUNT(maker_coin) AS count_swaps \
                FROM swaps \
                WHERE epoch >= {lib_helper.days_ago(epoch)} \
                GROUP BY maker_coin, taker_coin \
                ORDER BY trading_pair) \
            row;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    for i in results:
        i.update({"last_price": last_price_data[i["trading_pair"]]})
        print(i)
    logger.info(f"{len(results)} records returned for get_swaps_stats")
    return results


def get_swaps_stats_between(cursor, start_epoch, end_epoch):
    last_price_data = get_pairs_last_price_between(cursor, start_epoch, end_epoch)
    sql = f"SELECT row_to_json(row) FROM ( \
                SELECT \
                    CONCAT(maker_coin, '/', taker_coin) as trading_pair, \
                    maker_coin as base_currency, taker_coin as quote_currency, \
                    SUM(maker_amount) AS base_volume, SUM(taker_amount) AS quote_volume, \
                    MAX(maker_amount) AS max_maker_amount, MAX(taker_amount) AS max_taker_amount, \
                    AVG(taker_amount/maker_amount) AS avg_maker_price, AVG(maker_amount/taker_amount) AS avg_taker_price, \
                    MAX(taker_amount/maker_amount) AS max_maker_price, MAX(maker_amount/taker_amount) AS max_taker_price, \
                    MIN(taker_amount/maker_amount) AS min_maker_price, MIN(maker_amount/taker_amount) AS min_taker_price, \
                    COUNT(maker_coin) AS count_swaps \
                FROM swaps \
                WHERE epoch >= {start_epoch} AND epoch < {end_epoch} \
                GROUP BY maker_coin, taker_coin \
                ORDER BY trading_pair) \
            row;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    for i in results:
        i.update({"last_price": last_price_data[i["trading_pair"]]})
        print(i)
    logger.info(f"{len(results)} records returned for get_swaps_stats")
    return results


def get_24hr_swaps(cursor):
    rows = get_swaps_since(cursor, lib_helper.days_ago(1))
    return rows


def get_24hr_failed_swaps(cursor):
    rows = get_failed_swaps_since(cursor, lib_helper.days_ago(1))
    return rows


def get_24hr_pairs(cursor):
    rows = get_pairs_since(cursor, lib_helper.days_ago(1))
    return rows


def get_30d_swaps(cursor):
    rows = get_swaps_since(cursor, lib_helper.days_ago(30))
    return rows


def get_30d_failed_swaps(cursor):
    rows = get_failed_swaps_since(cursor, lib_helper.days_ago(30))
    return rows


def get_30d_pairs(cursor):
    rows = get_pairs_since(cursor, lib_helper.days_ago(30))
    return rows


def get_all_swaps(cursor):
    rows = get_swaps_since(cursor, 0)
    return rows


def get_all_failed_swaps(cursor):
    rows = get_failed_swaps_since(cursor, 0)
    return rows


def get_all_pairs(cursor):
    rows = get_pairs_since(cursor, 0)
    return rows


def get_pairs_last_price_since(cursor, epoch, as_dict=True):
    sql = f"SELECT row_to_json(row) FROM ( \
                SELECT \
                    CONCAT(maker_coin, '/', taker_coin) as trading_pair, \
                    ROW_NUMBER() OVER ( \
                        PARTITION BY (maker_coin, taker_coin) ORDER BY epoch \
                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING \
                    ) as row_num, \
                     LAST_VALUE(maker_amount/taker_amount) \
                    OVER ( \
                        PARTITION BY (maker_coin, taker_coin) ORDER BY epoch \
                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING \
                    ) as last_price \
                FROM swaps \
                WHERE epoch >= {epoch})\
            row WHERE row_num = 1;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]

    if as_dict:
        results_dict = {}
        for i in results:
            results_dict.update({i["trading_pair"]: i})
        return results_dict
    return results


def get_pairs_last_price_between(cursor, start_epoch, end_epoch, as_dict=True):
    sql = f"SELECT row_to_json(row) FROM ( \
                SELECT \
                    CONCAT(maker_coin, '/', taker_coin) as trading_pair, \
                    ROW_NUMBER() OVER ( \
                        PARTITION BY (maker_coin, taker_coin) ORDER BY epoch \
                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING \
                    ) as row_num, \
                     LAST_VALUE(maker_amount/taker_amount) \
                    OVER ( \
                        PARTITION BY (maker_coin, taker_coin) ORDER BY epoch \
                        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING \
                    ) as last_price \
                FROM swaps \
                WHERE epoch >= {start_epoch} AND epoch < {end_epoch})\
            row WHERE row_num = 1;"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]

    if as_dict:
        results_dict = {}
        for i in results:
            results_dict.update({i["trading_pair"]: i})
        return results_dict
    return results


def get_ts_version(cursor):
    sql = "SELECT extversion FROM pg_extension where extname = 'timescaledb';"
    cursor.execute(sql)
    results = cursor.fetchall()
    return results

def get_hypertables(cursor):
    sql = "SELECT * FROM timescaledb_information.hypertables;"
    cursor.execute(sql)
    results = cursor.fetchall()
    return results



if __name__ == '__main__':
    conn, cursor = get_timescaledb(True)
    get_swaps_stats(cursor, 1)
    conn.close()
