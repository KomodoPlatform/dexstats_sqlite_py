import os
import json
import time
import requests
from decimal import Decimal
from dotenv import load_dotenv
from collections import OrderedDict
from lib_logger import logger
import lib_helper

load_dotenv()

DOCKER_MM2_SERVICE_NAME = os.getenv("DOCKER_MM2_SERVICE_NAME")
if DOCKER_MM2_SERVICE_NAME:
    MM2_RPC_IP = DOCKER_MM2_SERVICE_NAME
else:
    MM2_RPC_IP = "127.0.0.1"

MM2_PORT = os.getenv("MM2_PORT")
if not MM2_PORT:
    MM2_PORT = 7783


# tuple, string, string -> list
# returning orderbook for given trading pair
def get_mm2_orderbook_for_pair(pair):
    try:
        if not isinstance(pair, list):
            if pair.find("/") > -1:
                pair = pair.split("/")
            elif pair.find("_") > -1:
                pair = pair.split("_")
        mm2_host = f"http://{MM2_RPC_IP}:{MM2_PORT}"
        params = {
                  'method': 'orderbook',
                  'base': pair[0],
                  'rel': pair[1]
                 }
        r = requests.post(mm2_host, json=params)
        return json.loads(r.text)
    except Exception as e:
        logger.warning(f"MM2 orderbook request failed! {e}")


# Orderbook Endpoint
def orderbook_for_pair(pair):
    if not lib_helper.validate_pair(pair):
        return {"error": "not valid pair"}
    pair_orderbook = get_mm2_orderbook_for_pair(pair)
    data = parse_orderbook_for_pair(pair_orderbook)
    orderbook_data = OrderedDict()
    orderbook_data["timestamp"] = int(time.time())
    orderbook_data["bids"] = data[0]
    orderbook_data["asks"] = data[1]
    return orderbook_data


def parse_orderbook_for_pair(pair_orderbook):
    bids_converted_list = []
    asks_converted_list = []
    try:
        for bid in pair_orderbook["bids"]:
            converted_bid = []
            converted_bid.append(bid["price"])
            converted_bid.append(bid["maxvolume"])
            bids_converted_list.append(converted_bid)
    except KeyError:
        pass
    try:
        for ask in pair_orderbook["asks"]:
            converted_ask = []
            converted_ask.append(ask["price"])
            converted_ask.append(ask["maxvolume"])
            asks_converted_list.append(converted_ask)
    except KeyError:
        pass
    return bids_converted_list, asks_converted_list


# list -> string
# returning lowest ask from provided orderbook
def find_lowest_ask_for_pair(orderbook):
    lowest_ask = {"price" : "0"}
    try:
        for ask in orderbook["asks"]:
            if lowest_ask["price"] == "0":
                lowest_ask = ask
            elif Decimal(ask["price"]) < Decimal(lowest_ask["price"]):
                lowest_ask = ask
    except KeyError:
        return 0
    return lowest_ask["price"]


# list -> string
# returning highest bid from provided orderbook
def find_highest_bid_for_pair(orderbook):
    highest_bid = {"price" : "0"}
    try:
        for bid in orderbook["bids"]:
            if Decimal(bid["price"]) > Decimal(highest_bid["price"]):
                highest_bid = bid
    except KeyError:
        return 0
    return highest_bid["price"]

