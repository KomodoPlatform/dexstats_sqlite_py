#!/usr/bin/env python3
import os
import psycopg2
from dotenv import load_dotenv
from datetime import timezone
from lib_logger import logger
from lib_helper import days_ago

load_dotenv()

def get_timescaledb(localhost=False):
    if localhost: host = 'localhost'
    else: host = os.getenv("POSTGRES_HOST")
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
            maker_coin TEXT NOT NULL,
            maker_amount DECIMAL NOT NULL,
            maker_gui TEXT,
            maker_version TEXT,
            maker_pubkey TEXT,
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



def get_timescaledb_uuids(days, table, conn, cursor):
    sql = f"SELECT uuid FROM {table} WHERE epoch > {days_ago(int(days))}"
    cursor.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    return results


def import_mysql_swaps_into_timescaledb(day_since, mysql_swaps_data, localhost=False):
    conn, cursor = get_timescaledb(localhost)
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


def import_mysql_failed_swaps_into_timescaledb(day_since, mysql_failed_swaps_data, localhost=False):
    conn, cursor = get_timescaledb(localhost)
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


def get_24hr_swaps(cur, days):
    sql = f"SELECT * FROM swaps WHERE epoch > {days_ago(int(days))}"
    cur.execute(sql)
    rows = cur.fetchall()
    logger.info(f"{len(rows)} records returned for last 24 hours in swaps table")
    return rows


def get_24hr_failed_swaps(cur, days):
    sql = f"SELECT * FROM failed_swaps WHERE epoch > {days_ago(int(days))}"
    cur.execute(sql)
    rows = cur.fetchall()
    logger.info(f"{len(rows)} records returned for last 24 hours in failed_swaps table")
    return rows


def get_swaps_stats(cur, days):
    last_price_data = get_pairs_last_price(cur, days)
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
                WHERE epoch > {days_ago(int(days))} \
                GROUP BY maker_coin, taker_coin \
                ORDER BY trading_pair) \
            row;"
    cur.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    for i in results:
        i.update({"last_price": last_price_data[i["trading_pair"]]})
        print(i)
    logger.info(f"{len(results)} records returned for get_swaps_stats")
    return results


def get_pairs_last_price(cur, days, as_dict=True):
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
                WHERE epoch > {days_ago(int(days))})\
            row WHERE row_num = 1;"
    cur.execute(sql)
    results = [i[0] for i in cursor.fetchall()]
    logger.info(f"{len(results)} records returned for get_pairs_last_price")

    if as_dict:
        results_dict = {}
        for i in results:
            results_dict.update({i["trading_pair"]: i})
        return results_dict
    return results


if __name__ == '__main__':
    conn, cursor = get_timescaledb(True)
    get_swaps_stats(cursor, 1)
    conn.close()