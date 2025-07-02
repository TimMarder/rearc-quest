import os
import re
import requests
import boto3
from urllib.parse import urljoin
from datetime import timezone, datetime

BLS_ROOT = "https://download.bls.gov/pub/time.series/pr/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; TimMarder-RearcQuest/1.0; "
        "+mailto:timmarder@gmail.com)"
    )
}
S3_BUCKET = os.environ.get("BUCKET")
s3 = boto3.client("s3")

def get_remote_manifest():
    html = requests.get(BLS_ROOT, headers=HEADERS, timeout=30).text
    rows = re.findall(
        r'(\d+/\d+/\d+\s+\d+:\d+\s+\w+)\s+(\d+)\s+<a\s+href="[^"]*/([^"/]+)"',
        html,
        flags=re.IGNORECASE,
    )
    manifest = {}
    for datestr, size, fname in rows:
        ts = (
            datetime.strptime(datestr, "%m/%d/%Y %I:%M %p")
            .replace(tzinfo=timezone.utc)
        )
        manifest[fname] = (int(size), ts)
    return manifest

def get_s3_manifest():
    paginator = s3.get_paginator("list_objects_v2")
    manifest: dict[str, tuple[int, datetime]] = {}

    for page in paginator.paginate(Bucket=S3_BUCKET):
        for obj in page.get("Contents", []):
            last_modified_utc = obj["LastModified"].astimezone(timezone.utc)
            key = obj["Key"]
            if key.startswith("datausa/"):
                continue
            manifest[key] = (obj["Size"], last_modified_utc)

    return manifest

def sync():
    remote = get_remote_manifest()
    local  = get_s3_manifest()

    for fname, (r_size, r_time) in remote.items():
        l_meta = local.get(fname)
        if l_meta is None:
            action = "ADD"
        elif l_meta[0] != r_size or abs((r_time - l_meta[1]).total_seconds()) > 1:
            action = "UPDATE"
        else:
            continue
        url = urljoin(BLS_ROOT, fname)
        obj = requests.get(url, headers=HEADERS, timeout=60).content
        s3.put_object(Bucket=S3_BUCKET, Key=fname, Body=obj, Metadata={"source": "bls"})
        print(f"{action}: {fname}")

    to_delete = [k for k in local.keys() if k not in remote]
    if to_delete:
        print("DELETE:", ", ".join(to_delete))
        s3.delete_objects(
            Bucket=S3_BUCKET,
            Delete={"Objects": [{"Key": k} for k in to_delete]}
        )

if __name__ == "__main__":
    sync()