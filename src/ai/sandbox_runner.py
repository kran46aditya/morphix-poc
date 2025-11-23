"""
Sandbox Runner for AI Repair Suggestions

Applies repair suggestions to sample data and validates with GX suite.
"""

from typing import Dict, Any, List, Optional
import pandas as pd

from ..quality.gx_builder import generate_suite
from ..quality.gx_runner import run_suite
from ..utils.logging import get_logger

logger = get_logger(__name__)


class SandboxRunner:
    """Runs repair suggestions in a sandbox environment."""
    
    def __init__(self):
        """Initialize sandbox runner."""
        self.logger = get_logger(__name__)
    
    def apply_suggestion_to_sample(self, suggestion: Dict[str, Any], 
                                   sample_docs: List[Dict]) -> pd.DataFrame:
        """Apply repair suggestion to sample documents.
        
        Args:
            suggestion: Repair suggestion dictionary
            sample_docs: Sample documents to transform
            
        Returns:
            Transformed DataFrame
        """
        try:
            # Convert sample docs to DataFrame
            df = pd.DataFrame(sample_docs)
            
            if df.empty:
                return df
            
            field = suggestion.get("field")
            suggestion_text = suggestion.get("suggestion", "")
            
            # Parse and apply suggestion
            # This is a simplified implementation - in production, use AST parsing
            if "pd.to_numeric" in suggestion_text:
                # Numeric conversion
                df[field] = pd.to_numeric(df[field], errors='coerce')
            elif ".astype('float64')" in suggestion_text:
                df[field] = df[field].astype('float64')
            elif ".astype(str)" in suggestion_text:
                df[field] = df[field].astype(str)
            elif "Add new field" in suggestion_text:
                # Add new field with default value
                new_type = suggestion.get("new_type")
                if new_type == "integer":
                    df[field] = 0
                elif new_type == "float":
                    df[field] = 0.0
                elif new_type == "string":
                    df[field] = ""
                elif new_type == "boolean":
                    df[field] = False
                else:
                    df[field] = None
            elif "Remove field" in suggestion_text:
                # Remove field
                if field in df.columns:
                    df = df.drop(columns=[field])
            
            self.logger.info(
                f"Applied repair suggestion to field: {field}",
                extra={
                    'event_type': 'repair_suggestion_applied',
                    'field': field,
                    'suggestion': suggestion_text
                }
            )
            
            return df
            
        except Exception as e:
            self.logger.error(
                f"Failed to apply repair suggestion: {e}",
                exc_info=True,
                extra={
                    'event_type': 'repair_suggestion_apply_error',
                    'field': suggestion.get("field"),
                    'error': str(e)
                }
            )
            # Return original DataFrame on error
            return pd.DataFrame(sample_docs)
    
    def run_gx_suite_on_transformed(self, transformed_df: pd.DataFrame,
                                    collection: Optional[str] = None) -> Dict[str, Any]:
        """Run GX suite on transformed DataFrame.
        
        Args:
            transformed_df: Transformed DataFrame
            collection: Collection name (optional)
            
        Returns:
            GX suite run results
        """
        try:
            # Generate suite from transformed data
            suite = generate_suite(transformed_df, suite_name=f"{collection}_transformed_suite" if collection else "transformed_suite")
            
            if not suite:
                return {
                    "passed": False,
                    "error": "Failed to generate GX suite"
                }
            
            # Run suite
            result = run_suite(suite, transformed_df, collection=collection, save_results=False)
            
            self.logger.info(
                f"GX suite run on transformed data: {'passed' if result.get('passed') else 'failed'}",
                extra={
                    'event_type': 'gx_suite_run_on_transformed',
                    'collection': collection,
                    'passed': result.get('passed', False)
                }
            )
            
            return result
            
        except Exception as e:
            self.logger.error(
                f"Failed to run GX suite on transformed data: {e}",
                exc_info=True,
                extra={
                    'event_type': 'gx_suite_run_error',
                    'collection': collection,
                    'error': str(e)
                }
            )
            return {
                "passed": False,
                "error": str(e)
            }

