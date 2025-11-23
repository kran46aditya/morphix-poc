"""
Great Expectations Suite Builder

Generates baseline expectation suites from sample DataFrames.
"""

import pandas as pd
from typing import Dict, Any, Optional
from pathlib import Path
import os

try:
    import great_expectations as gx
    from great_expectations.core import ExpectationSuite
    from great_expectations.dataset import PandasDataset
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False
    # Create mock classes for type hints when GX is not available
    ExpectationSuite = None
    PandasDataset = None

from ..utils.logging import get_logger

logger = get_logger(__name__)


def generate_suite(sample_df: pd.DataFrame, suite_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Generate baseline expectation suite from sample DataFrame.
    
    Args:
        sample_df: Sample DataFrame to analyze
        suite_name: Optional name for the suite
        
    Returns:
        Expectation suite dictionary or None if GX is not available
    """
    if not GX_AVAILABLE:
        logger.warning(
            "Great Expectations not available. Install with: pip install great-expectations",
            extra={
                'event_type': 'gx_not_available',
                'suite_name': suite_name
            }
        )
        return None
    
    if sample_df.empty:
        logger.warning(
            "Cannot generate suite from empty DataFrame",
            extra={
                'event_type': 'gx_empty_dataframe',
                'suite_name': suite_name
            }
        )
        return None
    
    try:
        # Create GX dataset from pandas DataFrame
        gx_dataset = PandasDataset(sample_df)
        
        # Generate suite name if not provided
        if not suite_name:
            suite_name = f"suite_{sample_df.shape[1]}_columns"
        
        # Build baseline expectations
        expectations = []
        
        # Column existence expectations
        for col in sample_df.columns:
            expectations.append({
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": col}
            })
        
        # Type expectations based on pandas dtypes
        for col in sample_df.columns:
            dtype = str(sample_df[col].dtype)
            if 'int' in dtype:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": col, "type_": "int64"}
                })
            elif 'float' in dtype:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": col, "type_": "float64"}
                })
            elif 'bool' in dtype:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": col, "type_": "bool"}
                })
            elif 'datetime' in dtype:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": col, "type_": "datetime64[ns]"}
                })
            else:
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": col, "type_": "object"}
                })
        
        # Null expectations
        for col in sample_df.columns:
            null_count = sample_df[col].isnull().sum()
            null_percentage = (null_count / len(sample_df)) * 100
            
            if null_percentage == 0:
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col}
                })
            elif null_percentage < 10:
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col, "mostly": 0.9}
                })
        
        # Numeric column expectations
        numeric_cols = sample_df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            col_data = sample_df[col].dropna()
            if len(col_data) > 0:
                min_val = float(col_data.min())
                max_val = float(col_data.max())
                
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": col,
                        "min_value": min_val,
                        "max_value": max_val,
                        "mostly": 1.0
                    }
                })
        
        # String length expectations
        string_cols = sample_df.select_dtypes(include=['object']).columns
        for col in string_cols:
            col_data = sample_df[col].dropna().astype(str)
            if len(col_data) > 0:
                min_len = int(col_data.str.len().min())
                max_len = int(col_data.str.len().max())
                
                if min_len == max_len:
                    expectations.append({
                        "expectation_type": "expect_column_value_lengths_to_equal",
                        "kwargs": {"column": col, "value": min_len}
                    })
                else:
                    expectations.append({
                        "expectation_type": "expect_column_value_lengths_to_be_between",
                        "kwargs": {
                            "column": col,
                            "min_value": min_len,
                            "max_value": max_len
                        }
                    })
        
        # Unique value expectations (for low cardinality columns)
        for col in sample_df.columns:
            unique_count = sample_df[col].nunique()
            total_count = len(sample_df)
            if total_count > 0:
                uniqueness_ratio = unique_count / total_count
                if uniqueness_ratio == 1.0 and total_count > 1:
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_unique",
                        "kwargs": {"column": col}
                    })
        
        # Build suite dictionary
        suite_dict = {
            "suite_name": suite_name,
            "expectations": expectations,
            "meta": {
                "generated_from": "sample_dataframe",
                "row_count": len(sample_df),
                "column_count": len(sample_df.columns),
                "columns": list(sample_df.columns)
            }
        }
        
        logger.info(
            f"Generated expectation suite '{suite_name}' with {len(expectations)} expectations",
            extra={
                'event_type': 'gx_suite_generated',
                'suite_name': suite_name,
                'expectation_count': len(expectations),
                'column_count': len(sample_df.columns)
            }
        )
        
        return suite_dict
        
    except Exception as e:
        logger.error(
            f"Failed to generate expectation suite: {e}",
            exc_info=True,
            extra={
                'event_type': 'gx_suite_generation_error',
                'suite_name': suite_name,
                'error': str(e)
            }
        )
        return None

