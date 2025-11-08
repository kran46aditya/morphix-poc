import sys

sys.path.append('src')

import pymongo

from mongodb import connection as conn


def test_read_with_pymongo(monkeypatch):
    try:
        import mongomock
    except Exception:
        print('mongomock not installed; skipping')
        return

    # create a mongomock client and patch MongoClient
    mock_client = mongomock.MongoClient()
    db = mock_client['testdb']
    coll = db['testcoll']
    coll.insert_many([{"_id": 1, "a": 1}, {"_id": 2, "a": 2}])

    def fake_client(uri):
        return mock_client

    monkeypatch.setattr(pymongo, 'MongoClient', fake_client)

    docs = conn.read_with_pymongo(mongo_uri='mongodb://x', database='testdb', collection='testcoll', limit=5)
    assert isinstance(docs, list)
    assert len(docs) == 2
