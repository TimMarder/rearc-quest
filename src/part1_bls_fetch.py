import os
import re
import requests
import boto3
from urllib.parse import urljoin

BLS_ROOT = "https://download.bls.gov/pub/time.series/pr/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; TimMarder-RearcQuest/1.0; "
        "+mailto:timmarder@gmail.com)"
    )
}

S3_BUCKET = os.environ.get("BUCKET")  # Set this before running
s3 = boto3.client("s3")

def list_remote_files():
    print("Fetching BLS index...")
    index = requests.get(BLS_ROOT, headers=HEADERS, timeout=30).text
    files = re.findall(
        r'<a\s+href="[^"]*/([^"/]+)"',
        index,
        flags=re.IGNORECASE,
    )
    print(f"Found {len(files)} remote files")
    return files

def list_s3_keys():
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=S3_BUCKET)
    for page in page_iterator:
        for obj in page.get("Contents", []):
            yield obj["Key"]

def sync():
    remote_files = list_remote_files()
    existing_keys = set(list_s3_keys())

    for filename in remote_files:
        if filename.endswith("/") or filename in existing_keys:
            continue  # skip folders and already-uploaded files
        url = urljoin(BLS_ROOT, filename)
        print(f"Fetching {url}")
        res = requests.get(url, headers=HEADERS, timeout=30)
        res.raise_for_status()

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=filename,
            Body=res.content,
            Metadata={"source": "bls"}
        )
        print(f"Uploaded to s3://{S3_BUCKET}/{filename}")

if __name__ == "__main__":
    sync()