import os
from unittest.mock import patch
os.environ["BUCKET"] = "test-bucket"

from src.data_fetch.lambdas.index import handler

def test_handler_happy_path():
    with patch("src.data_fetch.lambdas.index.bls_sync") as mock_bls, \
         patch("src.data_fetch.lambdas.index.fetch_and_store") as mock_fetch:

        mock_fetch.return_value = "datausa/test.json"

        result = handler({}, {}) 

        mock_bls.assert_called_once()
        mock_fetch.assert_called_once_with(bucket="test-bucket")
        assert result == {
            "status": "ok",
            "new_object": "datausa/test.json",
        }