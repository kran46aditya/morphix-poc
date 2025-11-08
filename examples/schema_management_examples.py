"""
Schema Generation and Management Examples

This script demonstrates how to:
1. Generate schemas from MongoDB collection samples
2. Upload and use Avro schema files
3. Transform data with schemas
4. Merge and export schemas
"""

import pandas as pd
import json
from etl.data_transformer import DataTransformer
from etl.schema_generator import SchemaGenerator
from etl.mongo_api_reader import MongoDataReader


def example_1_generate_schema_from_dataframe():
    """Example 1: Generate schema from a sample DataFrame."""
    print("=== Example 1: Generate Schema from DataFrame ===")
    
    # Create sample data that might come from MongoDB
    sample_data = pd.DataFrame({
        'user_id': [1, 2, 3, 4, 5],
        'username': ['alice', 'bob', 'charlie', 'diana', 'eve'],
        'email': ['alice@example.com', 'bob@test.org', None, 'diana@company.com', 'eve@domain.net'],
        'age': [25, 30, None, 35, 28],
        'salary': [50000.0, 65000.5, 70000.0, 85000.25, 55000.0],
        'is_active': [True, False, True, True, False],
        'tags': [['python', 'data'], ['java', 'backend'], ['frontend'], ['ml', 'python'], []],
        'metadata': [
            {'role': 'developer', 'level': 'senior'},
            {'role': 'engineer', 'level': 'mid'},
            {'role': 'designer', 'level': 'junior'},
            {'role': 'scientist', 'level': 'senior'},
            {'role': 'analyst', 'level': 'mid'}
        ]
    })
    
    # Generate schema
    transformer = DataTransformer()
    schema = transformer.generate_schema_from_dataframe(
        sample_data, 
        sample_size=1000,
        include_constraints=True
    )
    
    print("Generated Schema:")
    print(json.dumps(schema, indent=2, default=str))
    
    print("\nSchema Summary:")
    summary = transformer.get_schema_summary()
    print(json.dumps(summary, indent=2))
    
    return schema


def example_2_create_and_use_avro_schema():
    """Example 2: Create and use an Avro schema."""
    print("\n=== Example 2: Create and Use Avro Schema ===")
    
    # Define an Avro schema
    avro_schema = {
        "type": "record",
        "name": "UserRecord",
        "namespace": "com.morphix.users",
        "doc": "Schema for user data from MongoDB",
        "fields": [
            {
                "name": "user_id",
                "type": "long",
                "doc": "Unique user identifier"
            },
            {
                "name": "username",
                "type": "string",
                "doc": "User's login name"
            },
            {
                "name": "email",
                "type": ["null", "string"],
                "default": None,
                "doc": "User's email address (optional)"
            },
            {
                "name": "age",
                "type": ["null", "int"],
                "default": None,
                "doc": "User's age (optional)"
            },
            {
                "name": "salary",
                "type": "double",
                "doc": "User's salary"
            },
            {
                "name": "is_active",
                "type": "boolean",
                "default": True,
                "doc": "Whether user is active"
            },
            {
                "name": "department",
                "type": {
                    "type": "enum",
                    "name": "Department",
                    "symbols": ["ENGINEERING", "SALES", "MARKETING", "HR"]
                },
                "doc": "User's department"
            }
        ]
    }
    
    print("Avro Schema:")
    print(json.dumps(avro_schema, indent=2))
    
    # Load the Avro schema
    transformer = DataTransformer()
    parsed_schema = transformer.load_schema_from_avro(avro_schema)
    
    print("\nParsed Schema:")
    print(json.dumps(parsed_schema, indent=2))
    
    print("\nSchema Summary:")
    summary = transformer.get_schema_summary()
    print(json.dumps(summary, indent=2))
    
    return parsed_schema


def example_3_transform_data_with_schema():
    """Example 3: Transform data using a schema."""
    print("\n=== Example 3: Transform Data with Schema ===")
    
    # Sample nested MongoDB-like data
    raw_data = pd.DataFrame([
        {
            'user_id': '1',
            'profile': {
                'name': 'Alice Smith',
                'contact': {
                    'email': 'alice@example.com',
                    'phone': '+1-555-0123'
                }
            },
            'permissions': ['read', 'write'],
            'created_at': '2023-01-15T10:30:00Z',
            'is_admin': 'true'
        },
        {
            'user_id': '2',
            'profile': {
                'name': 'Bob Johnson',
                'contact': {
                    'email': 'bob@test.org',
                    'phone': None
                }
            },
            'permissions': ['read'],
            'created_at': '2023-02-20T14:45:00Z',
            'is_admin': 'false'
        }
    ])
    
    print("Original Data:")
    print(raw_data.to_string())
    
    # First, flatten the data
    transformer = DataTransformer()
    flattened_df = transformer.flatten_dataframe(raw_data)
    
    print("\nFlattened Data:")
    print(flattened_df.to_string())
    
    # Generate schema from flattened data
    schema = transformer.generate_schema_from_dataframe(flattened_df)
    
    print("\nGenerated Schema (sample):")
    # Show just a few fields for brevity
    sample_fields = dict(list(schema.items())[:3])
    print(json.dumps(sample_fields, indent=2, default=str))
    
    # Apply schema transformations
    transformed_df = transformer.apply_schema(flattened_df)
    
    print("\nTransformed Data:")
    print(transformed_df.to_string())
    
    print(f"\nValidation Errors: {len(transformer.validation_errors)}")
    if transformer.validation_errors:
        for error in transformer.validation_errors[:3]:  # Show first 3 errors
            print(f"  - {error}")
    
    return transformed_df


def example_4_schema_merging():
    """Example 4: Merge multiple schemas."""
    print("\n=== Example 4: Schema Merging ===")
    
    # Schema from first collection/dataset
    schema1 = {
        'user_id': {'type': 'integer', 'nullable': False, 'min_value': 1},
        'name': {'type': 'string', 'nullable': False, 'max_length': 100},
        'email': {'type': 'string', 'nullable': True, 'patterns': ['email']}
    }
    
    # Schema from second collection/dataset
    schema2 = {
        'user_id': {'type': 'integer', 'nullable': False, 'max_value': 999999},
        'email': {'type': 'string', 'nullable': False, 'patterns': ['email']},
        'phone': {'type': 'string', 'nullable': True, 'patterns': ['phone']},
        'department': {'type': 'string', 'nullable': False, 'is_categorical': True}
    }
    
    print("Schema 1:")
    print(json.dumps(schema1, indent=2))
    
    print("\nSchema 2:")
    print(json.dumps(schema2, indent=2))
    
    # Test different merge strategies
    transformer = DataTransformer(schema=schema1)
    
    # Union merge - includes all fields
    union_schema = transformer.merge_schemas(schema2.copy(), strategy="union")
    print("\nUnion Merge Result:")
    print(json.dumps(union_schema, indent=2))
    
    # Reset and try intersection merge
    transformer.schema = schema1.copy()
    intersection_schema = transformer.merge_schemas(schema2.copy(), strategy="intersection")
    print("\nIntersection Merge Result:")
    print(json.dumps(intersection_schema, indent=2))
    
    # Reset and try override merge
    transformer.schema = schema1.copy()
    override_schema = transformer.merge_schemas(schema2.copy(), strategy="override")
    print("\nOverride Merge Result:")
    print(json.dumps(override_schema, indent=2))


def example_5_export_to_avro():
    """Example 5: Export schema to Avro format."""
    print("\n=== Example 5: Export Schema to Avro ===")
    
    # Start with our native schema format
    native_schema = {
        'id': {'type': 'integer', 'nullable': False, 'description': 'Unique identifier'},
        'name': {'type': 'string', 'nullable': False, 'max_length': 100},
        'email': {'type': 'string', 'nullable': True, 'patterns': ['email']},
        'age': {'type': 'integer', 'nullable': True, 'min_value': 0, 'max_value': 150},
        'is_active': {'type': 'boolean', 'nullable': False, 'default': True}
    }
    
    print("Native Schema:")
    print(json.dumps(native_schema, indent=2))
    
    # Export to Avro format
    transformer = DataTransformer(schema=native_schema)
    avro_schema = transformer.export_schema_to_avro(
        record_name="ExportedUserRecord",
        namespace="com.morphix.exported"
    )
    
    print("\nExported Avro Schema:")
    print(json.dumps(avro_schema, indent=2))
    
    # Demonstrate round-trip: Avro -> Native -> Avro
    transformer2 = DataTransformer()
    reimported_schema = transformer2.load_schema_from_avro(avro_schema)
    
    print("\nRe-imported Native Schema:")
    print(json.dumps(reimported_schema, indent=2))


def example_6_api_simulation():
    """Example 6: Simulate API usage patterns."""
    print("\n=== Example 6: API Usage Simulation ===")
    
    # Simulate what happens when user uploads an Avro file
    print("Simulating Avro file upload...")
    
    avro_content = json.dumps({
        "type": "record",
        "name": "ProductRecord",
        "fields": [
            {"name": "product_id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "price", "type": "double"},
            {"name": "category", "type": ["null", "string"], "default": None},
            {"name": "in_stock", "type": "boolean", "default": True}
        ]
    })
    
    # Process uploaded schema
    transformer = DataTransformer()
    schema = transformer.load_schema_from_avro(avro_content)
    
    print("Processed uploaded schema:")
    print(json.dumps(transformer.get_schema_summary(), indent=2))
    
    # Simulate data transformation with the uploaded schema
    print("\nSimulating data transformation...")
    
    sample_product_data = pd.DataFrame([
        {'product_id': 'P001', 'name': 'Laptop', 'price': '999.99', 'category': 'Electronics', 'in_stock': 'yes'},
        {'product_id': 'P002', 'name': 'Book', 'price': '19.95', 'category': None, 'in_stock': 'no'},
        {'product_id': 'P003', 'name': 'Phone', 'price': '599.00', 'category': 'Electronics', 'in_stock': 'true'}
    ])
    
    print("Original data:")
    print(sample_product_data.to_string())
    
    # Apply schema transformations
    transformed_data = transformer.apply_schema(sample_product_data, strict=False)
    
    print("\nTransformed data:")
    print(transformed_data.to_string())
    
    print(f"\nValidation errors: {len(transformer.validation_errors)}")
    for error in transformer.validation_errors:
        print(f"  - {error}")


if __name__ == "__main__":
    """Run all examples."""
    
    print("Schema Generation and Management Examples")
    print("=" * 50)
    
    try:
        example_1_generate_schema_from_dataframe()
        example_2_create_and_use_avro_schema()
        example_3_transform_data_with_schema()
        example_4_schema_merging()
        example_5_export_to_avro()
        example_6_api_simulation()
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        
    except Exception as e:
        print(f"\nError running examples: {str(e)}")
        import traceback
        traceback.print_exc()