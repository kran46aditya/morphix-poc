"""
API Usage Examples for Schema Management

This script demonstrates how to use the new API endpoints for:
1. Generating schemas from MongoDB collections
2. Uploading Avro schema files
3. Transforming data with schemas
"""

import requests
import json
import io


# API base URL (adjust as needed)
BASE_URL = "http://localhost:8000"


def example_api_schema_generation():
    """Example: Generate schema from MongoDB collection via API."""
    print("=== API Example: Schema Generation ===")
    
    # Request payload for schema generation
    payload = {
        "mongo_uri": "mongodb://localhost:27017/testdb",
        "database": "testdb",
        "collection": "users",
        "query": {"status": "active"},
        "sample_size": 500,
        "include_constraints": True
    }
    
    try:
        response = requests.post(f"{BASE_URL}/schema/generate", json=payload)
        
        if response.status_code == 200:
            result = response.json()
            print("Schema generated successfully!")
            print(f"Status: {result['status']}")
            
            print("\nSchema Summary:")
            print(json.dumps(result['summary'], indent=2))
            
            print("\nSample Schema Fields:")
            # Show first 3 fields for brevity
            schema_items = list(result['schema'].items())[:3]
            for field_name, field_config in schema_items:
                print(f"  {field_name}: {field_config.get('type')} ({'nullable' if field_config.get('nullable') else 'required'})")
            
            print(f"\nSample Info:")
            print(f"  Rows analyzed: {result['sample_info']['rows_analyzed']}")
            print(f"  Columns: {result['sample_info']['columns']}")
            
        else:
            print(f"Error: {response.status_code}")
            print(response.json())
            
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def example_api_avro_upload():
    """Example: Upload Avro schema file via API."""
    print("\n=== API Example: Avro Schema Upload ===")
    
    # Create a sample Avro schema
    avro_schema = {
        "type": "record",
        "name": "OrderRecord",
        "namespace": "com.ecommerce.orders",
        "doc": "Schema for e-commerce order data",
        "fields": [
            {
                "name": "order_id",
                "type": "string",
                "doc": "Unique order identifier"
            },
            {
                "name": "customer_id",
                "type": "long",
                "doc": "Customer identifier"
            },
            {
                "name": "total_amount",
                "type": "double",
                "doc": "Total order amount"
            },
            {
                "name": "order_date",
                "type": "string",
                "doc": "Order date in ISO format"
            },
            {
                "name": "status",
                "type": {
                    "type": "enum",
                    "name": "OrderStatus",
                    "symbols": ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
                },
                "doc": "Current order status"
            },
            {
                "name": "shipping_address",
                "type": ["null", "string"],
                "default": None,
                "doc": "Shipping address (optional)"
            }
        ]
    }
    
    # Convert to JSON bytes for upload
    avro_content = json.dumps(avro_schema, indent=2)
    
    try:
        # Prepare file for upload
        files = {
            'file': ('order_schema.avsc', io.StringIO(avro_content), 'application/json')
        }
        
        response = requests.post(f"{BASE_URL}/schema/upload-avro", files=files)
        
        if response.status_code == 200:
            result = response.json()
            print("Avro schema uploaded successfully!")
            print(f"Status: {result['status']}")
            
            print("\nSchema Summary:")
            print(json.dumps(result['summary'], indent=2))
            
            print("\nFile Info:")
            print(f"  Filename: {result['file_info']['filename']}")
            print(f"  Size: {result['file_info']['size_bytes']} bytes")
            
            print("\nParsed Schema Fields:")
            for field_name, field_config in result['schema'].items():
                nullable = " (nullable)" if field_config.get('nullable') else ""
                print(f"  {field_name}: {field_config.get('type')}{nullable}")
                
        else:
            print(f"Error: {response.status_code}")
            print(response.json())
            
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def example_api_json_schema_load():
    """Example: Load schema from JSON via API."""
    print("\n=== API Example: JSON Schema Load ===")
    
    # Example: Load our native schema format
    native_schema = {
        "product_id": {
            "type": "string",
            "nullable": False,
            "description": "Unique product identifier"
        },
        "name": {
            "type": "string",
            "nullable": False,
            "max_length": 200
        },
        "price": {
            "type": "float",
            "nullable": False,
            "min_value": 0.01
        },
        "category": {
            "type": "string",
            "nullable": True,
            "is_categorical": True,
            "suggested_values": ["Electronics", "Books", "Clothing", "Home"]
        }
    }
    
    try:
        response = requests.post(f"{BASE_URL}/schema/from-json", json=native_schema)
        
        if response.status_code == 200:
            result = response.json()
            print("Schema loaded successfully!")
            print(f"Status: {result['status']}")
            print(f"Schema Type: {result['schema_type']}")
            
            print("\nSchema Summary:")
            print(json.dumps(result['summary'], indent=2))
            
        else:
            print(f"Error: {response.status_code}")
            print(response.json())
            
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def example_api_transform_with_schema():
    """Example: Transform MongoDB data with schema via API."""
    print("\n=== API Example: Data Transformation with Schema ===")
    
    # Define schema for transformation
    transformation_schema = {
        "user_id": {
            "type": "integer",
            "nullable": False
        },
        "profile_name": {
            "type": "string",
            "nullable": False,
            "max_length": 100
        },
        "profile_contact_email": {
            "type": "string",
            "nullable": True,
            "patterns": ["email"]
        },
        "is_admin": {
            "type": "boolean",
            "nullable": False
        }
    }
    
    payload = {
        "mongo_uri": "mongodb://localhost:27017/testdb",
        "database": "testdb",
        "collection": "users",
        "query": {"active": True},
        "schema": transformation_schema,
        "limit": 10,
        "flatten_data": True,
        "apply_schema": True,
        "strict_schema": False
    }
    
    try:
        response = requests.post(f"{BASE_URL}/mongo/transform", json=payload)
        
        if response.status_code == 200:
            result = response.json()
            print("Data transformation completed!")
            print(f"Status: {result['status']}")
            
            print("\nTransform Info:")
            transform_info = result['transform_info']
            print(f"  Rows processed: {transform_info['rows_processed']}")
            print(f"  Columns: {transform_info['columns']}")
            print(f"  Flattened: {transform_info['flattened']}")
            print(f"  Schema applied: {transform_info['schema_applied']}")
            print(f"  Validation errors: {len(transform_info['validation_errors'])}")
            
            if transform_info['validation_errors']:
                print("\nValidation Errors:")
                for error in transform_info['validation_errors'][:3]:  # Show first 3
                    print(f"    - {error}")
            
            print("\nSample Transformed Data:")
            for i, record in enumerate(result['data'][:2]):  # Show first 2 records
                print(f"  Record {i+1}: {json.dumps(record, indent=4)}")
                
            if 'schema_summary' in result:
                print("\nSchema Summary:")
                print(json.dumps(result['schema_summary'], indent=2))
                
        else:
            print(f"Error: {response.status_code}")
            print(response.json())
            
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def example_api_with_stored_credentials():
    """Example: Use API with stored credentials."""
    print("\n=== API Example: Using Stored Credentials ===")
    
    # First, save credentials (assuming this was done previously)
    user_id = "user123"
    
    # Generate schema using stored credentials
    payload = {
        "user_id": user_id,
        "sample_size": 100,
        "include_constraints": True
    }
    
    try:
        response = requests.post(f"{BASE_URL}/schema/generate", json=payload)
        
        if response.status_code == 200:
            result = response.json()
            print("Schema generated using stored credentials!")
            print(f"Status: {result['status']}")
            
            print("\nSchema Summary:")
            print(json.dumps(result['summary'], indent=2))
            
        elif response.status_code == 404:
            print("Credentials not found for user. Please save credentials first.")
        else:
            print(f"Error: {response.status_code}")
            print(response.json())
            
    except requests.RequestException as e:
        print(f"Request failed: {e}")


def create_sample_avro_file():
    """Create a sample Avro schema file for testing uploads."""
    print("\n=== Creating Sample Avro File ===")
    
    avro_schema = {
        "type": "record",
        "name": "CustomerRecord",
        "namespace": "com.example.crm",
        "doc": "Customer data schema for CRM system",
        "fields": [
            {
                "name": "customer_id",
                "type": "string",
                "doc": "Unique customer identifier"
            },
            {
                "name": "first_name",
                "type": "string",
                "doc": "Customer's first name"
            },
            {
                "name": "last_name",
                "type": "string",
                "doc": "Customer's last name"
            },
            {
                "name": "email",
                "type": ["null", "string"],
                "default": None,
                "doc": "Customer's email address"
            },
            {
                "name": "phone",
                "type": ["null", "string"],
                "default": None,
                "doc": "Customer's phone number"
            },
            {
                "name": "date_of_birth",
                "type": ["null", "string"],
                "default": None,
                "doc": "Customer's date of birth (ISO format)"
            },
            {
                "name": "account_type",
                "type": {
                    "type": "enum",
                    "name": "AccountType",
                    "symbols": ["BASIC", "PREMIUM", "ENTERPRISE"]
                },
                "default": "BASIC",
                "doc": "Customer's account type"
            },
            {
                "name": "is_active",
                "type": "boolean",
                "default": True,
                "doc": "Whether the customer account is active"
            },
            {
                "name": "created_at",
                "type": "string",
                "doc": "Account creation timestamp (ISO format)"
            },
            {
                "name": "tags",
                "type": {
                    "type": "array",
                    "items": "string"
                },
                "default": [],
                "doc": "Customer tags for segmentation"
            }
        ]
    }
    
    # Write to file
    filename = "sample_customer_schema.avsc"
    with open(filename, 'w') as f:
        json.dump(avro_schema, f, indent=2)
    
    print(f"Created sample Avro schema file: {filename}")
    print("You can now test the file upload API with this file.")
    
    return filename


if __name__ == "__main__":
    """Run API examples."""
    
    print("Schema Management API Examples")
    print("=" * 50)
    print("Note: These examples assume the API server is running on localhost:8000")
    print("Start the server with: uvicorn src.api.mongo_api:app --reload")
    print("=" * 50)
    
    # Create a sample file first
    create_sample_avro_file()
    
    # Note: These examples will fail if the API server is not running
    # or if MongoDB is not available. They're provided as reference.
    
    print("\nAPI Examples (will show request structure):")
    print("- Schema generation from collection")
    print("- Avro schema upload")
    print("- JSON schema loading")
    print("- Data transformation with schema")
    print("- Using stored credentials")
    
    # Uncomment the following lines to run actual API calls
    # (requires API server to be running)
    
    # example_api_schema_generation()
    # example_api_avro_upload()
    # example_api_json_schema_load()
    # example_api_transform_with_schema()
    # example_api_with_stored_credentials()
    
    print("\nTo test these APIs:")
    print("1. Start the API server: uvicorn src.api.mongo_api:app --reload")
    print("2. Ensure MongoDB is running and accessible")
    print("3. Uncomment the function calls above")
    print("4. Run this script")