import os
from ..part1_bls_fetch import sync as bls_sync
from ..part2_datausa_fetch import fetch_and_store

BUCKET = os.environ["BUCKET"]

def handler(event, context):
    bls_sync()                            # Part 1
    key = fetch_and_store(bucket=BUCKET)  # Part 2
    return {"status": "ok", "new_object": key}