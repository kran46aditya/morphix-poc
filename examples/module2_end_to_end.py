#!/usr/bin/env python3
"""
End-to-End Example: Module 2 - Data Transformer

This script demonstrates the complete usage of the second ETL module,
which transforms, flattens, and validates data from Module 1.

Usage examples:
1. Direct data transformation with flattening
2. Schema-based validation and transformation
3. Custom transformation pipelines
4. Integration with Module 1 for complete ETL
"""

import sys
import pandas as pd
from datetime import datetime

sys.path.append('src')

from etl import (
    DataTransformer, ETLPipeline, APIPipeline,
    create_pipeline_from_credentials, create_pipeline_from_uri,
    create_user_profile_schema, create_order_schema, create_event_schema
)


def example_1_basic_flattening():
    """Example 1: Basic data flattening of nested MongoDB documents."""
    print("=== Example 1: Basic Data Flattening ===")
    
    # Simulate complex MongoDB document structure
    mongodb_data = [
        {
            '_id': '507f1f77bcf86cd799439011',
            'user': {
                'name': 'Alice Johnson',
                'email': 'alice@example.com',
                'profile': {
                    'age': 30,
                    'location': {'city': 'New York', 'state': 'NY', 'country': 'USA'},
                    'preferences': ['data-science', 'python', 'machine-learning'],
                    'social': {
                        'twitter': '@alice_data',
                        'linkedin': 'alice-johnson-data'
                    }
                }
            },
            'orders': [
                {'id': 'ord_1', 'amount': 99.99, 'status': 'completed'},
                {'id': 'ord_2', 'amount': 149.50, 'status': 'pending'}
            ],
            'metadata': {
                'created_at': '2024-01-15T10:30:00Z',
                'source': 'web_signup',
                'campaign': 'winter_sale_2024'
            }
        },
        {
            '_id': '507f1f77bcf86cd799439012',
            'user': {
                'name': 'Bob Smith',
                'email': 'bob@example.com',
                'profile': {
                    'age': 25,
                    'location': {'city': 'San Francisco', 'state': 'CA', 'country': 'USA'},
                    'preferences': ['web-development', 'javascript'],
                    'social': {
                        'twitter': '@bob_dev'
                    }
                }
            },
            'orders': [
                {'id': 'ord_3', 'amount': 75.00, 'status': 'completed'}
            ],
            'metadata': {
                'created_at': '2024-01-20T14:45:00Z',
                'source': 'mobile_app',
                'campaign': 'mobile_launch'
            }
        }
    ]
    
    # Create DataFrame from MongoDB data
    df = pd.DataFrame(mongodb_data)
    print(f"Original DataFrame shape: {df.shape}")
    print(f"Original columns: {list(df.columns)}")
    
    # Initialize transformer and flatten
    transformer = DataTransformer()
    flattened_df = transformer.flatten_dataframe(df)
    
    print(f"Flattened DataFrame shape: {flattened_df.shape}")
    print(f"Flattened columns: {list(flattened_df.columns)}")
    print(f"Sample flattened data:")
    print(flattened_df[['user_name', 'user_profile_age', 'user_profile_location_city', 'orders_count']].head())
    print()


def example_2_schema_validation():
    """Example 2: Schema-based validation and transformation."""
    print("=== Example 2: Schema Validation and Transformation ===")
    
    # Define a schema for user data
    user_schema = {
        'user_id': {'type': 'str', 'required': True, 'nullable': False},
        'user_name': {'type': 'str', 'required': True, 'nullable': False},
        'user_email': {'type': 'str', 'required': True, 'pattern': r'^[^@]+@[^@]+\.[^@]+$'},
        'user_age': {'type': 'int', 'min_value': 13, 'max_value': 120},
        'user_location_city': {'type': 'str'},
        'user_location_state': {'type': 'str'},
        'account_status': {'type': 'category', 'categories': ['active', 'inactive', 'suspended']},
        'signup_date': {'type': 'datetime', 'required': True},
        'preferences_count': {'type': 'int', 'min_value': 0}
    }
    
    # Sample data with various issues
    raw_data = [
        {
            'user_id': '  u001  ',  # needs trimming
            'user_name': 'Alice Johnson',
            'user_email': 'alice@example.com',
            'user_age': '30',  # string that needs conversion
            'user_location_city': 'New York',
            'user_location_state': 'NY',
            'account_status': 'active',
            'signup_date': '2024-01-15T10:30:00Z',
            'preferences_count': '3'
        },
        {
            'user_id': 'u002',
            'user_name': '',  # empty string
            'user_email': 'invalid-email',  # invalid format
            'user_age': '150',  # too high
            'user_location_city': 'San Francisco',
            'user_location_state': 'CA',
            'account_status': 'premium',  # not in allowed categories
            'signup_date': '2024-01-20',
            'preferences_count': '0'
        }
    ]
    
    df = pd.DataFrame(raw_data)
    print("Original data with issues:")
    print(df)
    print()
    
    # Create transformer with schema
    transformer = DataTransformer(schema=user_schema)
    
    # Apply transformations (non-strict mode to see warnings)
    try:
        transformed_df = transformer.apply_schema(df, strict=False)
        print("Transformed data:")
        print(transformed_df)
        print(f"Data types after transformation:")
        print(transformed_df.dtypes)
        print()
        
        if transformer.validation_errors:
            print("Validation errors found:")
            for error in transformer.validation_errors:
                print(f"  - {error}")
        print()
        
    except Exception as e:
        print(f"Transformation failed: {e}")
        print()


def example_3_custom_transformations():
    """Example 3: Custom transformation pipeline."""
    print("=== Example 3: Custom Transformation Pipeline ===")
    
    # Sample e-commerce data
    ecommerce_data = [
        {
            'product_name': '  MacBook Pro  ',
            'price': '$1,299.99',
            'category': 'ELECTRONICS',
            'description': 'High-performance laptop for professionals',
            'tags': ['laptop', 'apple', 'professional'],
            'inventory': '50'
        },
        {
            'product_name': 'iPhone 15',
            'price': '$999.00',
            'category': 'electronics',
            'description': 'Latest smartphone with advanced features',
            'tags': ['phone', 'apple', 'mobile'],
            'inventory': '100'
        }
    ]
    
    df = pd.DataFrame(ecommerce_data)
    transformer = DataTransformer()
    
    # Add custom transformations
    def clean_price(df):
        """Remove currency symbols and convert to float."""
        df = df.copy()
        if 'price' in df.columns:
            df['price'] = df['price'].str.replace('$', '').str.replace(',', '').astype(float)
        return df
    
    def standardize_category(df):
        """Standardize category names."""
        df = df.copy()
        if 'category' in df.columns:
            df['category'] = df['category'].str.lower().str.title()
        return df
    
    def add_price_tier(df):
        """Add price tier based on price ranges."""
        df = df.copy()
        if 'price' in df.columns:
            df['price_tier'] = pd.cut(
                df['price'], 
                bins=[0, 100, 500, 1000, float('inf')],
                labels=['Budget', 'Mid-Range', 'Premium', 'Luxury']
            )
        return df
    
    def process_tags(df):
        """Process tags array into individual boolean columns."""
        df = df.copy()
        if 'tags' in df.columns:
            # Get all unique tags
            all_tags = set()
            for tags_list in df['tags']:
                if isinstance(tags_list, list):
                    all_tags.update(tags_list)
            
            # Create boolean columns for each tag
            for tag in all_tags:
                df[f'tag_{tag}'] = df['tags'].apply(
                    lambda x: tag in x if isinstance(x, list) else False
                )
        return df
    
    # Add transformations to pipeline
    transformer.add_transformation(clean_price, 'clean_price')
    transformer.add_transformation(standardize_category, 'standardize_category')
    transformer.add_transformation(add_price_tier, 'add_price_tier')
    transformer.add_transformation(process_tags, 'process_tags')
    
    # Run complete transformation
    result = transformer.transform(df, flatten=False, clean=True, apply_schema=False)
    
    print("Original data:")
    print(df[['product_name', 'price', 'category']].head())
    print()
    
    print("Transformed data:")
    print(result[['product_name', 'price', 'category', 'price_tier']].head())
    print()
    
    # Show tag columns
    tag_columns = [col for col in result.columns if col.startswith('tag_')]
    if tag_columns:
        print("Tag boolean columns:")
        print(result[['product_name'] + tag_columns].head())
    print()


def example_4_complete_etl_pipeline():
    """Example 4: Complete ETL pipeline using both modules."""
    print("=== Example 4: Complete ETL Pipeline (Module 1 + Module 2) ===")
    
    # This example shows how to integrate Module 1 and Module 2
    # Note: This would require actual MongoDB connection in real usage
    
    # Define schema for transformed user data
    schema = create_user_profile_schema()
    
    print("Using predefined user profile schema:")
    for field, config in list(schema.items())[:5]:  # Show first 5 fields
        print(f"  {field}: {config}")
    print("  ... (more fields)")
    print()
    
    # Simulate what the pipeline would do
    print("Pipeline steps (simulated):")
    print("1. Connect to MongoDB using credentials")
    print("2. Read user profile documents")
    print("3. Flatten nested user profile structure")
    print("4. Apply data cleaning and validation")
    print("5. Transform according to schema")
    print("6. Return cleaned, validated DataFrame")
    print()
    
    # Example of how to create and configure the pipeline
    try:
        # This would work with real MongoDB credentials:
        # pipeline = create_pipeline_from_credentials(
        #     username="myuser",
        #     password="mypass", 
        #     host="localhost",
        #     port=27017,
        #     database="userdb",
        #     collection="profiles",
        #     schema=schema
        # )
        # 
        # result = pipeline.run_pipeline(
        #     query={"status": "active"},
        #     limit=1000,
        #     use_spark=False
        # )
        # 
        # print(f"Pipeline would return DataFrame with {len(result)} rows")
        
        print("Pipeline configuration successful (simulation)")
        print("In real usage, this would:")
        print("  - Connect to MongoDB")
        print("  - Read and flatten user documents") 
        print("  - Apply schema validation")
        print("  - Return clean DataFrame ready for downstream processing")
        
    except Exception as e:
        print(f"Pipeline simulation note: {e}")
    
    print()


def example_5_api_based_pipeline():
    """Example 5: API-based ETL pipeline."""
    print("=== Example 5: API-Based ETL Pipeline ===")
    
    # This example shows how to use the API-based pipeline
    schema = create_order_schema()
    
    print("Using predefined order schema for API pipeline:")
    for field, config in list(schema.items())[:5]:
        print(f"  {field}: {config}")
    print("  ... (more fields)")
    print()
    
    try:
        # This would work with running API server:
        # api_pipeline = create_pipeline_from_api(
        #     api_url="http://localhost:8000/mongo/read",
        #     user_id="analytics_user",
        #     schema=schema
        # )
        # 
        # result = api_pipeline.run_pipeline(
        #     flatten=True,
        #     clean=True,
        #     apply_schema=True
        # )
        # 
        # print(f"API pipeline would return DataFrame with {len(result)} rows")
        
        print("API pipeline configuration successful (simulation)")
        print("In real usage with running API server, this would:")
        print("  - Call POST /mongo/read with user_id")
        print("  - Receive JSON data from API")
        print("  - Apply Module 2 transformations")
        print("  - Return transformed DataFrame")
        
    except Exception as e:
        print(f"API pipeline simulation note: {e}")
    
    print()


def example_6_schema_validation_report():
    """Example 6: Detailed schema validation and reporting."""
    print("=== Example 6: Schema Validation and Reporting ===")
    
    # Sample data with various compliance issues
    sample_data = [
        {
            '_id': 'user_001',
            'user_name': 'Alice Johnson',
            'user_email': 'alice@example.com',
            'user_profile_age': 30,
            'created_at': '2024-01-15T10:30:00Z',
            'user_profile_location_city': 'New York',
            'extra_field': 'This should not be here'
        },
        {
            '_id': 'user_002',
            'user_name': None,  # Required field is null
            'user_email': 'invalid-email',  # Invalid format
            'user_profile_age': 200,  # Exceeds max value
            'created_at': 'invalid-date',  # Invalid date format
            'user_profile_location_city': 'Los Angeles'
        }
    ]
    
    df = pd.DataFrame(sample_data)
    schema = create_user_profile_schema()
    schema['_strict_columns'] = True  # Only allow schema columns
    
    transformer = DataTransformer(schema=schema)
    
    # Generate schema summary
    print("Schema summary for input data:")
    summary = transformer.get_schema_summary(df)
    for field, info in summary.items():
        print(f"  {field}: {info['type']}, {info['null_count']} nulls, {info['unique_count']} unique")
    print()
    
    # Validate schema compliance
    print("Schema compliance validation:")
    validation = transformer.validate_schema_compliance(df)
    print(f"Status: {validation['status']}")
    
    if validation['errors']:
        print("Errors:")
        for error in validation['errors']:
            print(f"  - {error}")
    
    if validation['warnings']:
        print("Warnings:")
        for warning in validation['warnings']:
            print(f"  - {warning}")
    
    print(f"Validated {validation['columns_validated']}/{validation['total_columns']} columns")
    print()


def main():
    """Run all examples to demonstrate Module 2 capabilities."""
    print("Data Transformer - Module 2 End-to-End Examples")
    print("=" * 55)
    
    example_1_basic_flattening()
    example_2_schema_validation()
    example_3_custom_transformations()
    example_4_complete_etl_pipeline()
    example_5_api_based_pipeline()
    example_6_schema_validation_report()
    
    print("=" * 55)
    print("Examples completed successfully!")
    print("\nModule 2 features demonstrated:")
    print("✓ Data flattening for nested MongoDB documents")
    print("✓ Schema-based validation and transformation")
    print("✓ Custom transformation pipelines")
    print("✓ Integration with Module 1 for complete ETL")
    print("✓ API-based data processing")
    print("✓ Comprehensive validation and reporting")
    print("\nTo run with real data:")
    print("1. Configure MongoDB connection in Module 1")
    print("2. Start FastAPI server for API-based examples")
    print("3. Define appropriate schemas for your data")
    print("4. Uncomment the actual pipeline execution code")


if __name__ == "__main__":
    main()