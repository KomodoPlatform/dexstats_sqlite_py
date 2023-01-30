import os
import json
import requests
from decimal import Decimal
from dotenv import load_dotenv
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
    if pair.find("/") > -1:
        pair = pair.split("/")
    logger.info(f"Getting rderbook for {pair}")
    mm2_host = f"http://{MM2_RPC_IP}:{MM2_PORT}"
    params = {
              'method': 'orderbook',
              'base': pair[0],
              'rel': pair[1]
             }
    r = requests.post(mm2_host, json=params)
    return json.loads(r.text)


# list -> string
# returning lowest ask from provided orderbook
def find_lowest_ask(orderbook):
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
def find_highest_bid(orderbook):
    highest_bid = {"price" : "0"}
    try:
        for bid in orderbook["bids"]:
            if Decimal(bid["price"]) > Decimal(highest_bid["price"]):
                highest_bid = bid
    except KeyError:
        return 0
    return highest_bid["price"]


def get_and_parse_orderbook(pair):
    orderbook = get_mm2_orderbook_for_pair(pair)
    bids_converted_list = []
    asks_converted_list = []
    try:
        for bid in orderbook["bids"]:
            converted_bid = []
            converted_bid.append(bid["price"])
            converted_bid.append(bid["maxvolume"])
            bids_converted_list.append(converted_bid)
    except KeyError:
        pass
    try:
        for ask in orderbook["asks"]:
            converted_ask = []
            converted_ask.append(ask["price"])
            converted_ask.append(ask["maxvolume"])
            asks_converted_list.append(converted_ask)
    except KeyError:
        pass
    return bids_converted_list, asks_converted_list


# Orderbook Endpoint
def orderbook_for_pair(pair):
    pair = tuple(map(str, pair.split('_')))
    if len(pair) != 2 or not isinstance(pair[0], str) or not isinstance(pair[0], str):
        return {"error": "not valid pair"}
    data = get_and_parse_orderbook(pair)
    orderbook_data = OrderedDict()
    orderbook_data["timestamp"] = "{}".format(int(datetime.now().strftime("%s")))
    orderbook_data["bids"] = data[0]
    orderbook_data["asks"] = data[1]
    return orderbook_data

