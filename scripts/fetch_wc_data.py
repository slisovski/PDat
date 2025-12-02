#!/usr/bin/env python3

import os
import json
import base64
import hmac
import hashlib
import requests
from datetime import datetime
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, urlencode

# -----------------------------
# SETTINGS
# -----------------------------
TAG_IDS = [
    "39332", "41730", "41731", "41732", "41734", "41737", "41738",
    "41739", "41740", "41742", "41743", "41744", "41745", "41617",
    "41619", "41624", "41620"
]

API_BASE = "https://api.wildlifecomputers.com"
RAW = Path("data/raw_wc")
PROC = Path("data/processed_wc")
LATEST = Path("data/latest_wc")

for d in [RAW, PROC, LATEST]:
    d.mkdir(parents=True, exist_ok=True)

# -----------------------------
# WILDLIFE COMPUTERS HMAC SIGNING
# -----------------------------
def wc_headers(access_key, secret_key, method, url):
    parsed = urlparse(url)
    path = parsed.path
    query = parsed.query

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    string_to_sign = f"{method}\n{path}\n{query}\n{timestamp}"

    signature = hmac.new(
        secret_key.encode(),
        string_to_sign.encode(),
        hashlib.sha256
    ).digest()

    auth_header = f"{access_key}:{base64.b64encode(signature).decode()}"
    
    return {
        "x-wc-date": timestamp,
        "x-wc-authentication": auth_header
    }

# -----------------------------
# FETCH DECODED ARGOS LOCATIONS
# -----------------------------
def fetch_decoded_argos(access_key, secret_key, device_id):
    method = "GET"
    url = f"{API_BASE}/data/argos/decoded/{device_id}"

    headers = wc_headers(access_key, secret_key, method, url)
    r = requests.get(url, headers=headers)

    if r.status_code == 403:
        raise RuntimeError("Authentication failed: check WC keys?")
    
    r.raise_for_status()
    return r.json()


# -----------------------------
# CLEAN/TIDY DATA
# -----------------------------
def tidy_argos(json_data):
    if not json_data:
        return pd.DataFrame()
    
    df = pd.json_normalize(json_data)

    # Convert timestamps
    for col in ["locationDate", "messageDate"]:
        if col in df:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main():
    access_key = os.environ["WC_ACCESS_KEY"]
    secret_key = os.environ["WC_SECRET_KEY"]

    all_frames = []
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for tag in TAG_IDS:
        print(f"Fetching decoded Argos data for device {tag}...")

        raw_json = fetch_decoded_argos(access_key, secret_key, tag)

        # Save raw
        raw_path = RAW / f"argos_decoded_raw_{tag}_{timestamp}.json"
        with open(raw_path, "w") as f:
            json.dump(raw_json, f, indent=2)

        # Processed
        df = tidy_argos(raw_json)
        proc_path = PROC / f"argos_decoded_{tag}.parquet"
        df.to_parquet(proc_path, index=False)

        all_frames.append(df)

    # Combined file
    if all_frames:
        combined = pd.concat(all_frames, ignore_index=True)
        combined.to_parquet(LATEST / "argos_all_devices.parquet", index=False)

    print("✔ DONE – WC Decoded Argos data updated")


if __name__ == "__main__":
    main()
