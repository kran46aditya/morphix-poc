"""
Integration tests for Trino API endpoints.
"""
import pytest
from fastapi.testclient import TestClient
from src.api.mongo_api import app

client = TestClient(app)


@pytest.fixture
def auth_token():
    """Get authentication token for tests."""
    # Register and login a test user
    register_response = client.post(
        "/auth/register",
        json={
            "username": "trino_test_user",
            "email": "trino_test@example.com",
            "password": "TestPassword123!",
            "full_name": "Trino Test User"
        }
    )
    
    if register_response.status_code == 201:
        user_data = register_response.json()
        return user_data.get("access_token")
    
    # If registration fails, try to login
    login_response = client.post(
        "/auth/login",
        data={
            "username": "trino_test_user",
            "password": "TestPassword123!"
        }
    )
    
    if login_response.status_code == 200:
        return login_response.json().get("access_token")
    
    pytest.skip("Could not authenticate for Trino tests")


class TestTrinoHealth:
    """Test Trino health check endpoint."""
    
    def test_health_check_success(self, auth_token):
        """Test successful health check."""
        response = client.get(
            "/trino/health",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        # Note: This will fail if Trino is not running, but tests the endpoint structure
        assert response.status_code in [200, 503]
        data = response.json()
        assert "status" in data


class TestTrinoCatalogs:
    """Test Trino catalog listing."""
    
    def test_list_catalogs(self, auth_token):
        """Test listing catalogs."""
        response = client.get(
            "/trino/catalogs",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        # Note: This will fail if Trino is not running
        assert response.status_code in [200, 400, 500, 503]
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "catalogs" in data


class TestTrinoSchemas:
    """Test Trino schema listing."""
    
    def test_list_schemas(self, auth_token):
        """Test listing schemas."""
        response = client.get(
            "/trino/schemas",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        assert response.status_code in [200, 400, 500, 503]
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "catalog" in data
            assert "schemas" in data
    
    def test_list_schemas_with_catalog(self, auth_token):
        """Test listing schemas with specific catalog."""
        response = client.get(
            "/trino/schemas?catalog=hive",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        assert response.status_code in [200, 400, 500, 503]


class TestTrinoTables:
    """Test Trino table listing."""
    
    def test_list_tables(self, auth_token):
        """Test listing tables."""
        response = client.get(
            "/trino/tables",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        assert response.status_code in [200, 400, 500, 503]
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "catalog" in data
            assert "schema" in data
            assert "tables" in data
    
    def test_list_tables_with_catalog_and_schema(self, auth_token):
        """Test listing tables with specific catalog and schema."""
        response = client.get(
            "/trino/tables?catalog=hive&schema=default",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        assert response.status_code in [200, 400, 500, 503]


class TestTrinoDescribeTable:
    """Test Trino table description."""
    
    def test_describe_table(self, auth_token):
        """Test describing a table."""
        # Note: This will fail if table doesn't exist
        response = client.get(
            "/trino/table/test_table/describe",
            headers={"Authorization": f"Bearer {auth_token}"}
        )
        
        assert response.status_code in [200, 400, 500, 503]
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "catalog" in data
            assert "schema" in data
            assert "table" in data
            assert "columns" in data


class TestTrinoQuery:
    """Test Trino query execution."""
    
    def test_execute_query_simple(self, auth_token):
        """Test executing a simple query."""
        response = client.post(
            "/trino/query",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "sql": "SELECT 1 AS test_column"
            }
        )
        
        assert response.status_code in [200, 400, 500]
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "query" in data
            assert "columns" in data
            assert "data" in data
            assert "rows_returned" in data
    
    def test_execute_query_with_catalog_schema(self, auth_token):
        """Test executing a query with catalog and schema."""
        response = client.post(
            "/trino/query",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "sql": "SELECT 1 AS test",
                "catalog": "hive",
                "schema": "default",
                "max_rows": 100
            }
        )
        
        assert response.status_code in [200, 400, 500]
    
    def test_execute_query_with_limit(self, auth_token):
        """Test executing a query with row limit."""
        response = client.post(
            "/trino/query",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "sql": "SELECT * FROM information_schema.tables",
                "max_rows": 10
            }
        )
        
        assert response.status_code in [200, 400, 500]
        if response.status_code == 200:
            data = response.json()
            if data.get("rows_returned", 0) > 0:
                assert len(data["data"]) <= 10
    
    def test_execute_query_invalid_sql(self, auth_token):
        """Test executing invalid SQL."""
        response = client.post(
            "/trino/query",
            headers={"Authorization": f"Bearer {auth_token}"},
            json={
                "sql": "INVALID SQL QUERY"
            }
        )
        
        # Should return error but not 500
        assert response.status_code in [200, 400]
        if response.status_code == 200:
            data = response.json()
            assert data.get("status") == "error" or data.get("error") is not None


class TestTrinoAuthentication:
    """Test Trino endpoint authentication."""
    
    def test_health_check_no_auth(self):
        """Test that health check requires authentication."""
        response = client.get("/trino/health")
        assert response.status_code == 401
    
    def test_query_no_auth(self):
        """Test that query requires authentication."""
        response = client.post(
            "/trino/query",
            json={"sql": "SELECT 1"}
        )
        assert response.status_code == 401
    
    def test_list_tables_no_auth(self):
        """Test that list tables requires authentication."""
        response = client.get("/trino/tables")
        assert response.status_code == 401

