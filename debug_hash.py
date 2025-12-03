import os, hashlib, requests

username = os.environ["DRUID_USERNAME"]
password = os.environ["DRUID_PASSWORD"]

raw = f"{username} + 'druid' + {password} + 'heifeng'"
sha = hashlib.sha256(raw.encode()).hexdigest()

print("raw:", raw)
print("FINAL HASH:", sha)

r = requests.post("https://www.ecotopiago.com/api/v2/login",
                  json={"username": username, "password": sha})

print("Status:", r.status_code)
print("Response:", r.text)
print("Headers:", r.headers)
