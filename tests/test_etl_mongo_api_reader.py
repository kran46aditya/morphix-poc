import sys
import json

sys.path.append('src')

import pandas as pd
import requests

from etl import mongo_api_reader as reader


class DummyResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


def test_fetch_into_dataframe(monkeypatch):
    sample = {"status": "ok", "data": [{"_id": 1, "a": 1}, {"_id": 2, "a": 2}]}

    def fake_post(url, json=None, timeout=None):
        return DummyResponse(sample)

    monkeypatch.setattr(requests, 'post', fake_post)

    df = reader.fetch_into_dataframe(payload={"user_id": "u1"})
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['_id', 'a']
    assert len(df) == 2
