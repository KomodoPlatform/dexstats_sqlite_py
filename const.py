#!/usr/bin/env python3
import os
from dotenv import load_dotenv


load_dotenv()
API_HOST = os.getenv('API_HOST')
API_PORT = os.getenv('API_PORT')
if API_PORT:
    API_PORT = int(API_PORT)
COINS_URL = os.getenv('COINS_URL')
COINS_CONFIG_URL = os.getenv('COINS_CONFIG_URL')
MM2_DB_PATH = os.getenv('MM2_DB_PATH')
SCRIPT_PATH = os.path.realpath(os.path.dirname(__file__))
