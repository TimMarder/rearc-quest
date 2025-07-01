import boto3, pytest, os
from moto import mock_aws

BUCKET = "test-bucket"

@pytest.fixture
def s3_bucket():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3