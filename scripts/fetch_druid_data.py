#!/usr/bin/env python3
import os
import json
import hashlib
import requests
from datetime import datetime, timezone
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------
# DEVICE IDS (correct Druid-internal)
# ---------------------------------------------------------

DEVICE_IDS = [
    "68f5e8dac3d77b735bd5717b",
    "68f5e8dac3d77b735bd571bf",
    "68f5e8dac3d77b735bd57203",
    "68f5e8dbc3d77b735bd57313",
    "68f5e8dac3d77b735bd570f3",
    "68f5e8dac3d77b735bd57247",
    "68f5e8dac3d77b735bd572cf",
    "68f5e8dac3d77b735bd570af",
    "68f5e8dac3d77b735bd57137"
]

API_BASE = "https://www.ecotopiago.com/api/"

# ---------------------------------------------------------
# FOLDER STRUCTURE
# ---------------------------------------------------------

GNSS_RAW  = Path("data/druid/gnss/raw")
GNSS_PROC = Path("data/druid/gnss/processed")
ENV_RAW   = Path("data/druid/env/raw")
ENV_PROC  = Path("data/druid/env/processed")
LATEST    = Path("data/druid/latest")

for d in [GNSS_RAW, GNSS_PROC, ENV_RAW, ENV_PROC, LATEST]:
    d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# LOGIN
# ---------------------------------------------------------

def druid_login(username, password):
    raw = f"{username} + druid + {password} + heifeng"
    sha = hashlib.sha256(raw.encode()).hexdigest()

    r = requests.post(f"{API_BASE}v2/login",
                      json={"username": username, "password": sha})
    r.raise_for_status()

    token = r.headers.get("x-druid-authentication")
    if not token:
        raise RuntimeError("No auth token returned.")

    return token


# ---------------------------------------------------------
# FETCH ARGOS GNSS (paginated)
# ---------------------------------------------------------

def fetch_gnss(token, device_id):
    headers = {
        "X-Druid-Authentication": token,
        "x-result-limit": "1000",
        "x-result-sort": "-timestamp"
    }

    url_base = f"{API_BASE}v2/argos_location/device/{device_id}/page/"
    cursor = ""
    all_records = []

    while True:
        url = url_base + cursor
        r = requests.get(url, headers=headers)
        r.raise_for_status()

        chunk = r.json()
        if not chunk:
            break

        all_records.extend(chunk)
        cursor = chunk[-1]["timestamp"]

    return all_records


# ---------------------------------------------------------
# FETCH ENVIRONMENTAL DATA (paginated)
# ---------------------------------------------------------

def fetch_env(token, device_id):
    headers = {
        "X-Druid-Authentication": token,
        "x-result-limit": "1000",
        "x-result-sort": "-timestamp"
    }

    url_base = f"{API_BASE}v2/argos_summary/device/{device_id}/page/"
    cursor = ""
    all_records = []

    while True:
        url = url_base + cursor
        r = requests.get(url, headers=headers)
        r.raise_for_status()

        chunk = r.json()
        if not chunk:
            break

        all_records.extend(chunk)

        # environmental data may use different timestamp keys
        last_ts = (chunk[-1].get("timestamp")
                   or chunk[-1].get("date")
                   or chunk[-1].get("recorded_at")
                   or None)
        if not last_ts:
            break

        cursor = last_ts

    return all_records


# ---------------------------------------------------------
# CLEAN GNSS
# ---------------------------------------------------------

def tidy_gnss(raw_json, device_id):
    if not raw_json:
        return pd.DataFrame()

    df = pd.json_normalize(raw_json)
    df["device_id"] = device_id

    for col in ["timestamp", "satellite_timestamp", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# ---------------------------------------------------------
# CLEAN ENVIRONMENTAL DATA
# ---------------------------------------------------------

def tidy_env(raw_json, device_id):
    if not raw_json:
        return pd.DataFrame()

    df = pd.json_normalize(raw_json)
    df["device_id"] = device_id

    for col in ["timestamp", "date", "updated_at", "recorded_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    username = os.environ["DRUID_USERNAME"]
    password = os.environ["DRUID_PASSWORD"]

    print("Logging in…")
    token = druid_login(username, password)
    print("✔ Login OK\n")

    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    all_gnss = []
    all_env = []

    for dev in DEVICE_IDS:
        print(f"📡 GNSS for {dev}…")
        gnss_raw = fetch_gnss(token, dev)
        with open(GNSS_RAW / f"gnss_raw_{dev}_{now}.json", "w") as f:
            json.dump(gnss_raw, f, indent=2)
        gnss_df = tidy_gnss(gnss_raw, dev)
        gnss_df.to_parquet(GNSS_PROC / f"gnss_{dev}.parquet", index=False)
        all_gnss.append(gnss_df)
        print(f"  → {len(gnss_df)} GNSS records")

        print(f"🌡 ENV for {dev}…")
        env_raw = fetch_env(token, dev)
        with open(ENV_RAW / f"env_raw_{dev}_{now}.json", "w") as f:
            json.dump(env_raw, f, indent=2)
        env_df = tidy_env(env_raw, dev)
        env_df.to_parquet(ENV_PROC / f"env_{dev}.parquet", index=False)
        all_env.append(env_df)
        print(f"  → {len(env_df)} ENV records\n")

    # Save combined:
    pd.concat(all_gnss, ignore_index=True).to_parquet(
        LATEST / "gnss_all_devices.parquet", index=False)
    pd.concat(all_env, ignore_index=True).to_parquet(
        LATEST / "env_all_devices.parquet", index=False)

    print("🎉 ALL DATA FETCHED AND STORED SUCCESSFULLY")


if __name__ == "__main__":
    main()
