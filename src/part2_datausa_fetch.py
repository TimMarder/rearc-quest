import os
import json
import requests
import boto3
from datetime import date

API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; TimMarder-RearcQuest/1.0; "
        "+mailto:timmarder@gmail.com)"
    )
}
S3_BUCKET = os.environ["BUCKET"]
FILENAME = f"datausa/{date.today()}.json"
s3 = boto3.client("s3")

def fetch_data():
    print("Fetching DataUSA API...")
    res = requests.get(API_URL, headers=HEADERS, timeout=30)
    res.raise_for_status()
    return res.json()

def upload_to_s3(data):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=FILENAME,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
        Metadata={"source": "datausa"}
    )
    print(f"Uploaded to s3://{S3_BUCKET}/{FILENAME}")

def main():
    data = fetch_data()
    upload_to_s3(data)

if __name__ == "__main__":
    main()