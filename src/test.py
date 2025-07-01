import requests
headers = {"User-Agent": "timmarder@gmail.com"}
r = requests.get("https://download.bls.gov/pub/time.series/pr/", headers=headers)
print(r.status_code)
print(r.text[:500])