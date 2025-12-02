#!/usr/bin/env python3
import os
import json
import hashlib
import requests
from datetime import datetime
import pandas as pd
from pathlib import Path

# -----------------------------
# USER SETTINGS
# -----------------------------
UUID_LIST = [
    "1c000006e6", "1c000006e8", "1c000006e9",
    "1c000006ee", "1c000006e3", "1c000006eb",
    "1c000006ed", "1c000006e0", "1c000006e5"
]

API_BASE = "https://www.ecotopiago.com/api/"

DATA_RAW = Path("data/raw")
DATA_PROC = Path("data/processed")
DATA_LATEST = Path("data/latest")

for d in [DATA_RAW, DATA_PROC, DATA_LATEST]:
    d.mkdir(parents=True, exist_ok=True)

# -----------------------------
# AUTHENTICATION
# -----------------------------
def druid_login(username, password):
    raw = f"{username}druid{password}heifeng"
    sha256pwd = hashlib.sha256(raw.encode()).hexdigest()

    url = f"{API_BASE}v2/login"
    payload = {"username": username, "password": sha256pwd}

    r = requests.post(url, json=payload)
    r.raise_for_status()

    token = r.headers.get("x-druid-authentication")
    if not token:
        raise RuntimeError("No authentication token returned from Druid.")

    return token


# -----------------------------
# GET DEVICE INFO (UUID → internal ID)
# -----------------------------
def fetch_device_info(token, uuid_list):
    url = f"{API_BASE}v3/device/many"
    headers = {"X-Druid-Authentication": token}
    body = {"uuid": uuid_list}

    r = requests.post(url, json=body, headers=headers)
    r.raise_for_status()
    devices = r.json()

    # map: uuid → device_id
    uuid_to_id = {}
    for d in devices:
        if "uuid" in d:
            uuid_to_id[d["uuid"]] = d["id"]
    return uuid_to_id, devices


# -----------------------------
# FETCH ALL GPS DATA SINCE DEPLOYMENT
# -----------------------------
def fetch_all_gps(token, device_id):
    url_base = f"{API_BASE}v2/gps/device/{device_id}/page/"
    headers = {
        "X-Druid-Authentication": token,
        "x-result-limit": "1000",
        "x-result-sort": "-timestamp"
    }

    all_records = []
    param = ""   # empty → start from first record

    while True:
        url = url_base + param
        r = requests.get(url, headers=headers)
        if r.status_code == 401:
            raise RuntimeError("Token expired during GPS fetch.")
        r.raise_for_status()

        chunk = r.json()
        if not chunk:
            break

        all_records.extend(chunk)
        # new param = last timestamp in this chunk
        last_timestamp = chunk[-1]["timestamp"]
        param = last_timestamp

    return all_records


# -----------------------------
# CLEAN GPS DATA → tidy table
# -----------------------------
def tidy_gps(raw_json):
    if len(raw_json) == 0:
        return pd.DataFrame()

    df = pd.json_normalize(raw_json)

    # ensure timestamps are parsed
    if "timestamp" in df:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if "updated_at" in df:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    return df


# -----------------------------
# MAIN EXECUTION
# -----------------------------
def main():
    username = os.environ["DRUID_USERNAME"]
    password = os.environ["DRUID_PASSWORD"]

    print("Logging in...")
    token = druid_login(username, password)

    print("Fetching device metadata...")
    uuid_to_id, device_info = fetch_device_info(token, UUID_LIST)

    # Save metadata
    with open(DATA_LATEST / "metadata.json", "w") as f:
        json.dump(device_info, f, indent=2)

    all_dfs = []

    for uuid, dev_id in uuid_to_id.items():
        print(f"Fetching GPS data for device {uuid} (id={dev_id})...")

        raw_gps = fetch_all_gps(token, dev_id)

        # Save raw JSON
        raw_path = DATA_RAW / f"gps_raw_{uuid}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
        with open(raw_path, "w") as f:
            json.dump(raw_gps, f, indent=2)

        # Tidy + save processed
        df = tidy_gps(raw_gps)
        proc_path = DATA_PROC / f"gps_processed_{uuid}.parquet"
        df.to_parquet(proc_path, index=False)

        all_dfs.append(df)

    # Combine all devices
    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_parquet(DATA_LATEST / "gps_all_devices.parquet", index=False)

    print("DONE ✔")


if __name__ == "__main__":
    main()
