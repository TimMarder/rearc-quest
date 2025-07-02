import os, json, requests, boto3
from datetime import date

API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; TimMarder-RearcQuest/1.0; "
        "+mailto:timmarder@gmail.com)"
    )
}
s3 = boto3.client("s3")

def fetch_data():
    print("Fetching DataUSA API...")
    res = requests.get(API_URL, headers=HEADERS, timeout=30)
    res.raise_for_status()
    return res.json()

def upload_to_s3(data, *, bucket=None, key=None):
    bucket = bucket or os.environ["BUCKET"]
    key    = key or f"datausa/{date.today()}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
        Metadata={"source": "datausa"},
    )
    print(f"Uploaded to s3://{bucket}/{key}")
    return key


def fetch_and_store(*, bucket=None):
    data = fetch_data()
    return upload_to_s3(data, bucket=bucket)

def main():
    fetch_and_store()

if __name__ == "__main__":
    main()