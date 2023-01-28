#!/usr/bin/env python3
import os
import sqlite3
import psycopg2
import mysql.connector
from dotenv import load_dotenv
from lib_logger import logger

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

def get_mysql():
    mysql_conn = mysql.connector.connect(
      host=os.getenv("mysql_hostname"),
      user=os.getenv("mysql_username"),
      passwd=os.getenv("mysql_password"),
      database=os.getenv("mysql_db")
    )
    mysql_cursor = mysql_conn.cursor()
    return mysql_conn, mysql_cursor

def get_sqlite(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        logger.error(e)
    return conn


def create_sqlite_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except sqlite3.Error as e:
        logger.error(e)


sql_create_swaps_table = """CREATE TABLE IF NOT EXISTS swaps (
                                id integer PRIMARY KEY,
                                started_at text NOT NULL,
                                uuid text NOT NULL UNIQUE,
                                taker_coin integer NOT NULL,
                                taker_amount integer NOT NULL,
                                taker_gui text,
                                taker_version text,
                                taker_pubkey text,
                                maker_coin integer NOT NULL,
                                maker_amount integer NOT NULL,
                                maker_gui text,
                                maker_version text,
                                maker_pubkey text,
                                epoch int NOT NULL
                            );"""


sql_create_failed_swaps_table = """CREATE TABLE IF NOT EXISTS failed_swaps (
                                id integer PRIMARY KEY,
                                started_at text NOT NULL,
                                uuid text NOT NULL UNIQUE,
                                taker_coin integer NOT NULL,
                                taker_amount integer NOT NULL,
                                taker_error_type text,
                                taker_error_msg text,
                                taker_gui text,
                                taker_version text,
                                taker_pubkey text,
                                maker_coin integer NOT NULL,
                                maker_amount integer NOT NULL,
                                maker_error_type text,
                                maker_error_msg text,
                                maker_gui text,
                                maker_version text,
                                maker_pubkey text,
                                epoch int NOT NULL
                            );"""


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