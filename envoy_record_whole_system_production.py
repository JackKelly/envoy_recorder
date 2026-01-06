import os
import time
from pathlib import Path

import polars as pl
import requests
import urllib3
from dotenv import load_dotenv

load_dotenv()  # reads variables from a .env file and sets them in os.environ

# Disable SSL Warnings (Envoy uses self-signed certs)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

token = os.environ["ENPHASE_TOKEN"]
headers = {"Authorization": f"Bearer {token}"}
url = "http://envoy.local/production.json"

data_retrieval_time = round(time.time())

# TODO: Handle exceptions thrown by `get`
response = requests.get(url, headers=headers, verify=False, timeout=10)
response.raise_for_status()
data = response.json()
production_data = data["production"]

assert len(production_data) == 1
production_data = production_data[0]
assert production_data["type"] == "inverters"

columns = ["activeCount", "readingTime", "wNow", "whLifetime", "data_retrieval_time"]

production_data["data_retrieval_time"] = data_retrieval_time
for col in columns:
    assert col in production_data

# Save data to CSV
df = pl.DataFrame([production_data], schema=columns)
csv_filename = Path("whole_system_production.csv")
include_header = not csv_filename.exists()
csv_string = df.write_csv(include_header=include_header)
with csv_filename.open(mode="a") as f:
    f.write(csv_string)
