import requests_mock, os, importlib
from src import part1_bls_fetch as bls_fetch

os.environ["BUCKET"] = "test-bucket"
importlib.reload(bls_fetch) 

def fake_index_html():
    return """
<html><body><pre>
  6/5/2025  8:30 AM          102 <a href="/pub.time.series/pr/pr.class">pr.class</a><br>
  6/5/2025  8:30 AM          123 <a href="/pub.time.series/pr/pr.data.0.Current">pr.data.0.Current</a><br>
</pre></body></html>
"""

def test_sync_adds_new_files(s3_bucket):
    with requests_mock.Mocker() as m:
        # index page
        m.get(bls_fetch.BLS_ROOT, text=fake_index_html())
        # file contents
        m.get(bls_fetch.urljoin(bls_fetch.BLS_ROOT, "pr.class"), text="dummy")
        m.get(bls_fetch.urljoin(bls_fetch.BLS_ROOT, "pr.data.0.Current"), text="abc\t123")

        bls_fetch.sync()

    objs = s3_bucket.list_objects_v2(Bucket=os.environ["BUCKET"])["Contents"]
    keys = sorted(obj["Key"] for obj in objs)
    assert keys == ["pr.class", "pr.data.0.Current"]