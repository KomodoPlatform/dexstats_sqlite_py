#!/usr/bin/env python3
import os
from dotenv import load_dotenv


load_dotenv()
MM2_DB_PATH = os.getenv('MM2_DB_PATH')
API_HOST = os.getenv('API_HOST')
API_PORT = os.getenv('API_PORT')
if API_PORT:
    API_PORT = int(API_PORT)
