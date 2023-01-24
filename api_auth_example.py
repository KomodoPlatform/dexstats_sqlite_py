#!/usr/bin/env python3
import os
import requests
from requests.auth import HTTPBasicAuth
  
from dotenv import load_dotenv
load_dotenv()

API_USER = os.getenv("API_USER")
API_PASS = os.getenv("API_PASS")

response = requests.get('http://localhost:8081/api/v1/private/24hr_coins_stats', auth=HTTPBasicAuth(API_USER, API_PASS))
print(response.json())


response = requests.get('http://localhost:8081/api/v1/private/24hr_coins_stats', auth=HTTPBasicAuth("bad user", "bad_pass"))
print(response.json())