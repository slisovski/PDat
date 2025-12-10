#!/usr/bin/env python3
import os
import hmac
import hashlib
import base64
import datetime
import requests
import json
from pathlib import Path

API_BASE = "https://wcapi.wildlifecomputers.com/v1"
DATA_RAW = Path("data/wc/raw")
DATA_PROC = Path("data/wc/processed")

for d in [DATA_RAW, DATA_PROC]:
    d.mkdir(parents=True, exist_ok=True)

# Your device IDs:
DEVICE_IDS = [
    "39332", "41730", "41731", "41732", "41734",
    "41737", "41738", "41739", "41740", "41742",
    "41743", "41744", "41745", "41617", "41619",
    "41624", "41620"
]

def wc_signature(secret_key, timestamp, method, url):
    """
    Build HMAC-SHA256 WC signature.
    """
    message = f"{timestamp}{method}{url}"
    digest = hmac.new(
        secret_key.encode(),
        msg=message.encode(),
        digestmod=hashlib.sha256
    ).digest()
    return base64.b64encode(digest).decode()

def fetch_decoded_argos(access_key, secret_key, device_id):
    """
    Fetch WC decoded Argos data for one device.
    """
    method = "GET"
    endpoint = f"/data/argos/decoded/{device_id}"
    url = API_BASE + endpoint

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    signature = wc_signature(secret_key, timestamp, method, endpoint)

    headers = {
        "WC-Access-Key": access_key,
        "WC-Timestamp": timestamp,
        "WC-Signature": signature
    }

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    return r.json()

def main():
    access_key = os.environ["WC_ACCESS_KEY"]
    secret_key = os.environ["WC_SECRET_KEY"]

    print("Fetching Wildlife Computers data…")

    for device in DEVICE_IDS:
        print(f" → Device {device}")

        data = fetch_decoded_argos(access_key, secret_key, device)

        outfile = DATA_RAW / f"wc_decoded_{device}.json"
        with open(outfile, "w") as f:
            json.dump(data, f, indent=2)

    print("✔ DONE")

if __name__ == "__main__":
    main()
