#!/usr/bin/env python3
import os
import json
import hashlib
import requests
from datetime import datetime, timezone
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# DEVICE IDS (correct Druid internal IDs)
# ---------------------------------------------------------

DEVICE_IDS = [
    "68f5e8dac3d77b735bd5717b",   # 1c000006e6
    "68f5e8dac3d77b735bd571bf",   # 1c000006e8
    "68f5e8dac3d77b735bd57203",   # 1c000006e9
    "68f5e8dbc3d77b735bd57313",   # 1c000006ee
    "68f5e8dac3d77b735bd570f3",   # 1c000006e3
    "68f5e8dac3d77b735bd57247",   # 1c000006eb
    "68f5e8dac3d77b735bd572cf",   # 1c000006ed
    "68f5e8dac3d77b735bd570af",   # 1c000006e0
    "68f5e8dac3d77b735bd57137"    # 1c000006e5
]

API_BASE = "https://www.ecotopiago.com/api/"

DATA_RAW = Path("data/raw")
DATA_PROC = Path("data/processed")
DATA_LATEST = Path("data/latest")

for d in [DATA_RAW, DATA_PROC, DATA_LATEST]:
    d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# LOGIN
# ---------------------------------------------------------

def druid_login(username, password):
    raw = f"{username} + druid + {password} + heifeng"
    sha = hashlib.sha256(raw.encode()).hexdigest()

    r = requests.post(
        f"{API_BASE}v2/login",
        json={"username": username, "password": sha}
    )
    r.raise_for_status()

    token = r.headers.get("x-druid-authentication")
    if not token:
        raise RuntimeError("No auth token returned.")

    return token


# ---------------------------------------------------------
# FETCH ARGOS GNSS (paginated)
# ---------------------------------------------------------

def fetch_argos_gnss(token, device_id):
    headers = {
        "X-Druid-Authentication": token,
        "x-result-limit": "1000",
        "x-result-sort": "-timestamp"
    }

    url_base = f"{API_BASE}v2/argos_location/device/{device_id}/page/"
    cursor = ""  # empty = first page (most recent)
    all_records = []

    while True:
        url = url_base + cursor
        r = requests.get(url, headers=headers)
        r.raise_for_status()

        chunk = r.json()

        # empty list → finished
        if not chunk:
            break

        all_records.extend(chunk)

        # next cursor = last timestamp
        last_ts = chunk[-1]["timestamp"]
        cursor = last_ts

    return all_records


# ---------------------------------------------------------
# FORMAT DATA
# ---------------------------------------------------------

def tidy_records(raw_json, device_id):
    if not raw_json:
        return pd.DataFrame()

    df = pd.json_normalize(raw_json)
    df["device_id"] = device_id

    for col in ["timestamp", "satellite_timestamp", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    username = os.environ["DRUID_USERNAME"]
    password = os.environ["DRUID_PASSWORD"]

    print("🔑 Logging in...")
    token = druid_login(username, password)
    print("✔ Login OK\n")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    all_dfs = []

    for dev_id in DEVICE_IDS:
        print(f"📡 Fetching Argos GNSS data for {dev_id}...")
        raw = fetch_argos_gnss(token, dev_id)

        raw_file = DATA_RAW / f"argos_gnss_raw_{dev_id}_{timestamp}.json"
        with open(raw_file, "w") as f:
            json.dump(raw, f, indent=2)

        df = tidy_records(raw, dev_id)
        proc_file = DATA_PROC / f"argos_gnss_processed_{dev_id}.parquet"
        df.to_parquet(proc_file, index=False)

        all_dfs.append(df)
        print(f"   ↳ Records fetched: {len(df)}")

    combined = pd.concat(all_dfs, ignore_index=True)
    combined.to_parquet(DATA_LATEST / "argos_gnss_all_devices.parquet", index=False)

    print("\n🎉 DONE — all Argos GNSS data updated successfully!")


if __name__ == "__main__":
    main()
