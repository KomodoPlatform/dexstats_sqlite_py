#!/usr/bin/env python3
import time


def days_ago(days):
    return int(time.time()) - (24 * 60 * 60 * days)


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
