"""
Great Expectations Suite Runner

Runs expectation suites against DataFrames and persists results.
"""

import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime
import json
import os

try:
    import great_expectations as gx
    from great_expectations.dataset import PandasDataset
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False
    PandasDataset = None

from ..utils.logging import get_logger

logger = get_logger(__name__)


def run_suite(suite: Dict[str, Any], df: pd.DataFrame, 
              collection: Optional[str] = None,
              save_results: bool = True) -> Dict[str, Any]:
    """Run expectation suite against DataFrame.
    
    Args:
        suite: Expectation suite dictionary
        df: DataFrame to validate
        collection: Collection name for saving results (optional)
        save_results: Whether to save results to metadata directory
        
    Returns:
        Dictionary with passed/failed expectations and summary
    """
    if not GX_AVAILABLE:
        logger.warning(
            "Great Expectations not available. Install with: pip install great-expectations",
            extra={
                'event_type': 'gx_not_available',
                'collection': collection
            }
        )
        return {
            "passed": False,
            "failed_expectations": [],
            "successful_expectations": [],
            "summary": {
                "error": "Great Expectations not installed"
            }
        }
    
    if df.empty:
        logger.warning(
            "Cannot run suite on empty DataFrame",
            extra={
                'event_type': 'gx_empty_dataframe',
                'collection': collection
            }
        )
        return {
            "passed": False,
            "failed_expectations": [],
            "successful_expectations": [],
            "summary": {
                "error": "Empty DataFrame"
            }
        }
    
    try:
        # Create GX dataset
        gx_dataset = PandasDataset(df)
        
        # Get expectations from suite
        expectations = suite.get("expectations", [])
        
        # Run each expectation
        successful_expectations = []
        failed_expectations = []
        
        for exp in expectations:
            exp_type = exp.get("expectation_type")
            kwargs = exp.get("kwargs", {})
            
            try:
                # Execute expectation
                result = getattr(gx_dataset, exp_type)(**kwargs)
                
                if result.success:
                    successful_expectations.append({
                        "expectation_type": exp_type,
                        "kwargs": kwargs,
                        "success": True
                    })
                else:
                    failed_expectations.append({
                        "expectation_type": exp_type,
                        "kwargs": kwargs,
                        "success": False,
                        "result": result.to_json_dict() if hasattr(result, 'to_json_dict') else str(result)
                    })
                    
            except Exception as e:
                # Expectation execution failed
                failed_expectations.append({
                    "expectation_type": exp_type,
                    "kwargs": kwargs,
                    "success": False,
                    "error": str(e)
                })
        
        # Calculate summary
        total_expectations = len(expectations)
        passed_count = len(successful_expectations)
        failed_count = len(failed_expectations)
        pass_rate = (passed_count / total_expectations * 100) if total_expectations > 0 else 0
        
        passed = failed_count == 0
        
        result_summary = {
            "passed": passed,
            "total_expectations": total_expectations,
            "successful_expectations": passed_count,
            "failed_expectations": failed_count,
            "pass_rate": round(pass_rate, 2),
            "suite_name": suite.get("suite_name", "unknown"),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "row_count": len(df),
            "column_count": len(df.columns)
        }
        
        # Save results if requested
        if save_results and collection:
            _save_gx_results(collection, result_summary, successful_expectations, failed_expectations)
        
        logger.info(
            f"GX suite run completed: {passed_count}/{total_expectations} passed",
            extra={
                'event_type': 'gx_suite_run_completed',
                'collection': collection,
                'suite_name': suite.get("suite_name"),
                'passed': passed,
                'pass_rate': pass_rate,
                'total_expectations': total_expectations
            }
        )
        
        return {
            "passed": passed,
            "failed_expectations": failed_expectations,
            "successful_expectations": successful_expectations,
            "summary": result_summary
        }
        
    except Exception as e:
        logger.error(
            f"Failed to run expectation suite: {e}",
            exc_info=True,
            extra={
                'event_type': 'gx_suite_run_error',
                'collection': collection,
                'suite_name': suite.get("suite_name", "unknown"),
                'error': str(e)
            }
        )
        return {
            "passed": False,
            "failed_expectations": [],
            "successful_expectations": [],
            "summary": {
                "error": str(e)
            }
        }


def _save_gx_results(collection: str, summary: Dict[str, Any], 
                    successful_expectations: List[Dict], 
                    failed_expectations: List[Dict]):
    """Save GX results to metadata directory.
    
    Args:
        collection: Collection name
        summary: Result summary
        successful_expectations: List of successful expectations
        failed_expectations: List of failed expectations
    """
    try:
        # Create metadata directory structure
        metadata_base = Path(os.getenv("METADATA_BASE", "/metadata"))
        metadata_dir = metadata_base / "quality" / collection
        metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate timestamp-based filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        result_file = metadata_dir / f"{timestamp}.json"
        
        # Prepare result document
        result_doc = {
            "collection": collection,
            "summary": summary,
            "successful_expectations": successful_expectations,
            "failed_expectations": failed_expectations
        }
        
        # Write to file
        with open(result_file, 'w') as f:
            json.dump(result_doc, f, indent=2, default=str)
        
        logger.info(
            f"GX results saved to {result_file}",
            extra={
                'event_type': 'gx_results_saved',
                'collection': collection,
                'result_file': str(result_file),
                'passed': summary.get("passed", False)
            }
        )
        
    except Exception as e:
        logger.warning(
            f"Failed to save GX results: {e}",
            exc_info=True,
            extra={
                'event_type': 'gx_results_save_error',
                'collection': collection,
                'error': str(e)
            }
        )

