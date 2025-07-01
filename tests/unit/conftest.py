import boto3, pytest, sys
from moto import mock_aws
from pathlib import Path

SRC_PATH = Path(__file__).resolve().parent / "src"
if SRC_PATH.exists():
    sys.path.insert(0, str(SRC_PATH))
BUCKET = "test-bucket"

@pytest.fixture
def s3_bucket():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3