import json, pytest, boto3
from moto import mock_aws
from src.data_analysis.lambdas import index


@pytest.fixture
def fake_population_data():
    return {
        "data": [
            {"Year": "2013", "Population": 300000000},
            {"Year": "2014", "Population": 310000000},
            {"Year": "2015", "Population": 320000000},
            {"Year": "2016", "Population": 330000000},
            {"Year": "2017", "Population": 340000000},
            {"Year": "2018", "Population": 350000000}
        ]
    }

@pytest.fixture
def fake_bls_data():
    return (
        "series_id\tyear\tperiod\tvalue\n"
        "S1\t2013\tQ1\t100\n"
        "S1\t2014\tQ1\t200\n"
        "S1\t2015\tQ1\t300\n"
        "S1\t2016\tQ1\t400\n"
        "S1\t2017\tQ1\t500\n"
        "S1\t2018\tQ1\t600\n"
    )

@mock_aws
def test_handler(fake_population_data, fake_bls_data, monkeypatch, capsys):
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "my-bucket"
    key_json = "datausa/test.json"
    key_csv = "pr.data.0.Current"
    s3.create_bucket(Bucket=bucket)

    s3.put_object(Bucket=bucket, Key=key_json, Body=json.dumps(fake_population_data))
    s3.put_object(Bucket=bucket, Key=key_csv, Body=fake_bls_data.encode("utf-8"))

    monkeypatch.setenv("BUCKET", bucket)

    event = {
        "Records": [
            {
                "body": json.dumps({
                    "Records": [
                        {"s3": {"object": {"key": key_json}}}
                    ]
                })
            }
        ]
    }

    result = index.handler(event, {})
    assert result["status"] == "analytics-done"
    assert result["key"] == key_json

    captured = capsys.readouterr()
    assert "Mean pop 2013-2018" in captured.out
    assert "Best-year sample" in captured.out