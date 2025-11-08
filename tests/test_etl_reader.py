import sys
import pandas as pd

sys.path.append('src')

from etl import mongo_api_reader as reader
from etl import MongoDataReader, create_reader_from_connection_info, create_reader_from_credentials


def test_mongo_data_reader_pandas(monkeypatch):
    """Test MongoDataReader with pandas DataFrame output."""
    # Mock the pymongo connection
    def fake_read(mongo_uri, database, collection, query=None, limit=1000):
        return [{"_id": 1, "name": "Alice", "age": 30}, {"_id": 2, "name": "Bob", "age": 25}]

    import mongodb.connection as conn
    monkeypatch.setattr(conn, 'read_with_pymongo', fake_read)

    # Test direct instantiation
    mongo_reader = MongoDataReader("mongodb://test", "testdb", "testcoll")
    df = mongo_reader.read_to_pandas(query={"age": {"$gte": 25}}, limit=100)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["_id", "name", "age"]
    assert df.iloc[0]["name"] == "Alice"


def test_mongo_data_reader_read_method(monkeypatch):
    """Test the unified read method."""
    def fake_read(mongo_uri, database, collection, query=None, limit=1000):
        return [{"_id": 1, "data": "test"}]

    import mongodb.connection as conn
    monkeypatch.setattr(conn, 'read_with_pymongo', fake_read)

    mongo_reader = MongoDataReader("mongodb://test", "testdb", "testcoll")
    
    # Test pandas path
    df = mongo_reader.read(use_spark=False, query={}, limit=50)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_create_reader_from_connection_info():
    """Test factory function for creating reader from connection info."""
    reader = create_reader_from_connection_info("mongodb://localhost:27017", "mydb", "mycoll")
    assert isinstance(reader, MongoDataReader)
    assert reader.mongo_uri == "mongodb://localhost:27017"
    assert reader.database == "mydb"
    assert reader.collection == "mycoll"


def test_create_reader_from_credentials():
    """Test factory function for creating reader from individual credentials."""
    reader = create_reader_from_credentials("user", "pass", "localhost", 27017, "mydb", "mycoll")
    assert isinstance(reader, MongoDataReader)
    assert reader.database == "mydb"
    assert reader.collection == "mycoll"
    assert "mongodb://user:pass@localhost:27017/mydb?authSource=admin" in reader.mongo_uri


def test_read_via_api(monkeypatch):
    """Test reading data via API endpoint."""
    class MockResponse:
        def __init__(self, json_data, status_code=200):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code >= 400:
                raise Exception(f"HTTP {self.status_code}")

    def mock_post(url, json=None, timeout=30):
        return MockResponse({
            "status": "ok",
            "data": [{"_id": 1, "name": "Test"}, {"_id": 2, "name": "Data"}]
        })

    import requests
    monkeypatch.setattr(requests, 'post', mock_post)

    df = reader.read_via_api(payload={"user_id": "test_user"})
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "name" in df.columns
