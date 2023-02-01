#!/usr/bin/env python3
import time
from decimal import Decimal


def days_ago(days=1):
    return int(time.time()) - (24 * 60 * 60 * int(days))


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


def reverse_string_number(string_number):
    if Decimal(string_number) != 0:
        return "{:.10f}".format(1 / Decimal(string_number))
    else:
        return string_number


def validate_pair(pair):
    pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return False
    return True

def get_pair_tickers(pair):
    if not isinstance(pair, list):
        if pair.find("/") > -1:
            pair = pair.split("/")
        elif pair.find("_") > -1:
            pair = pair.split("_")
    return pair