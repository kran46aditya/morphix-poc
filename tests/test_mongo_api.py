import sys
import os
import json

sys.path.append('src')

from api import mongo_api


def test_build_conn_info_with_uri():
    body = mongo_api.MongoRequest(mongo_uri="mongodb://user:pass@localhost:27017/mydb", database="mydb", collection="col", query={"a":1})
    ci = mongo_api._build_conn_info(body)
    assert ci["mongo_uri"].startswith("mongodb://")
    assert ci["database"] == "mydb"


def test_build_conn_info_with_parts_raises():
    body = mongo_api.MongoRequest(username=None, password=None, host=None, port=None, database="db", collection="c")
    try:
        mongo_api._build_conn_info(body)
        assert False, "Expected HTTPException"
    except Exception as e:
        assert hasattr(e, 'status_code')


def test_read_from_mongo_returns_data_only(monkeypatch):
    # Monkeypatch pg_creds.get_credentials to return a stored row
    stored = {
        'user_id': 'u1',
        'mongo_uri': 'mongodb://user:pass@localhost:27017',
        'database': 'db',
        'collection': 'coll',
        'query': {}
    }

    monkeypatch.setattr(mongo_api.pg_creds, 'get_credentials', lambda uid: stored)

    # Monkeypatch pymongo reader
    def fake_read(mongo_uri, database, collection, query=None, limit=10):
        return [{"_id": 1, "a": 1}, {"_id": 2, "a": 2}]

    monkeypatch.setattr(mongo_api.mongo_conn, 'read_with_pymongo', fake_read)

    body = mongo_api.MongoRequest(user_id='u1', database='db', collection='coll')
    resp = mongo_api.read_from_mongo(body)
    assert resp['status'] == 'ok'
    assert 'data' in resp
    assert isinstance(resp['data'], list)

