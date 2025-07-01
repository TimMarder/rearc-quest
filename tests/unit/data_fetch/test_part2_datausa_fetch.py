import json, requests_mock, os, importlib
os.environ["BUCKET"] = "test-bucket"
from src.data_fetch import part2_datausa_fetch as datausa_fetch

importlib.reload(datausa_fetch)

SAMPLE_JSON = {
    "data": [
        {"Year": 2018, "Population": 327167439},
        {"Year": 2019, "Population": 328239523},
    ]
}

def test_fetch_and_store_writes_json(s3_bucket):
    with requests_mock.Mocker() as m:
        m.get(datausa_fetch.API_URL, json=SAMPLE_JSON)
        key = datausa_fetch.fetch_and_store()
        assert key.startswith("datausa/")

    s3 = s3_bucket
    body = s3.get_object(Bucket=os.environ["BUCKET"], Key=key)["Body"].read()
    data = json.loads(body)
    assert data == SAMPLE_JSON