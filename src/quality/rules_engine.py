"""
Data quality rules engine with built-in rule library.
"""

from typing import List, Dict, Any
import pandas as pd
from pydantic import BaseModel
from enum import Enum
import re
import logging

logger = logging.getLogger(__name__)


class RuleType(str, Enum):
    NULL_THRESHOLD = "null_threshold"
    TYPE_CHECK = "type_check"
    RANGE_CHECK = "range_check"
    PATTERN_MATCH = "pattern_match"
    UNIQUENESS = "uniqueness"
    FRESHNESS = "freshness"
    CUSTOM = "custom"


class QualityRule(BaseModel):
    """Single quality rule definition."""
    rule_id: str
    rule_type: RuleType
    column: str
    parameters: Dict[str, Any]
    severity: str = "warning"  # error, warning, info


class QualityResult(BaseModel):
    """Result of quality check."""
    rule_id: str
    passed: bool
    severity: str
    message: str
    failed_count: int
    total_count: int
    failed_values: List[Any] = []


class QualityRulesEngine:
    """Execute data quality rules."""
    
    BUILT_IN_RULES = {
        "email_format": {
            "type": "pattern_match",
            "pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        },
        "phone_format": {
            "type": "pattern_match",
            "pattern": r'^\+?[\d\s\-\(\)]{10,}$'
        },
        "no_nulls": {
            "type": "null_threshold",
            "max_null_percent": 0
        },
        "mostly_complete": {
            "type": "null_threshold",
            "max_null_percent": 10
        }
    }
    
    def apply_rules(
        self,
        df: pd.DataFrame,
        rules: List[QualityRule]
    ) -> List[QualityResult]:
        """
        Execute all quality rules on DataFrame.
        
        For each rule:
        1. Apply appropriate check based on rule_type
        2. Collect failures
        3. Calculate metrics
        4. Generate result
        
        Args:
            df: DataFrame to check
            rules: List of quality rules
            
        Returns:
            List of quality results
        """
        results = []
        for rule in rules:
            try:
                result = self._execute_rule(df, rule)
                results.append(result)
            except Exception as e:
                logger.error(f"Error executing rule {rule.rule_id}: {e}")
                results.append(QualityResult(
                    rule_id=rule.rule_id,
                    passed=False,
                    severity=rule.severity,
                    message=f"Error executing rule: {str(e)}",
                    failed_count=0,
                    total_count=len(df)
                ))
        
        return results
    
    def _execute_rule(
        self,
        df: pd.DataFrame,
        rule: QualityRule
    ) -> QualityResult:
        """Execute single rule."""
        if rule.rule_type == RuleType.NULL_THRESHOLD:
            return self._check_null_threshold(df, rule)
        elif rule.rule_type == RuleType.TYPE_CHECK:
            return self._check_type(df, rule)
        elif rule.rule_type == RuleType.RANGE_CHECK:
            return self._check_range(df, rule)
        elif rule.rule_type == RuleType.PATTERN_MATCH:
            return self._check_pattern(df, rule)
        elif rule.rule_type == RuleType.UNIQUENESS:
            return self._check_uniqueness(df, rule)
        elif rule.rule_type == RuleType.FRESHNESS:
            return self._check_freshness(df, rule)
        else:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message=f"Unknown rule type: {rule.rule_type}",
                failed_count=0,
                total_count=len(df)
            )
    
    def _check_null_threshold(
        self,
        df: pd.DataFrame,
        rule: QualityRule
    ) -> QualityResult:
        """Check if nulls exceed threshold."""
        if rule.column not in df.columns:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message=f"Column '{rule.column}' not found",
                failed_count=0,
                total_count=len(df)
            )
        
        null_count = df[rule.column].isnull().sum()
        total_count = len(df)
        null_percent = (null_count / total_count * 100) if total_count > 0 else 0
        
        max_null_percent = rule.parameters.get("max_null_percent", 0)
        passed = null_percent <= max_null_percent
        
        return QualityResult(
            rule_id=rule.rule_id,
            passed=passed,
            severity=rule.severity,
            message=f"Null percentage: {null_percent:.2f}% (threshold: {max_null_percent}%)",
            failed_count=null_count if not passed else 0,
            total_count=total_count
        )
    
    def _check_type(
        self,
        df: pd.DataFrame,
        rule: QualityRule
    ) -> QualityResult:
        """Validate column dtype matches expected."""
        if rule.column not in df.columns:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message=f"Column '{rule.column}' not found",
                failed_count=0,
                total_count=len(df)
            )
        
        expected_type = rule.parameters.get("expected_type")
        actual_type = str(df[rule.column].dtype)
        
        # Type mapping
        type_mapping = {
            "int": ["int64", "int32", "int16", "int8"],
            "float": ["float64", "float32"],
            "string": ["object"],
            "bool": ["bool", "boolean"]
        }
        
        passed = False
        if expected_type in type_mapping:
            passed = actual_type in type_mapping[expected_type]
        else:
            passed = actual_type == expected_type
        
        return QualityResult(
            rule_id=rule.rule_id,
            passed=passed,
            severity=rule.severity,
            message=f"Expected type: {expected_type}, Actual: {actual_type}",
            failed_count=0 if passed else 1,
            total_count=1
        )
    
    def _check_range(
        self,
        df: pd.DataFrame,
        rule: QualityRule
    ) -> QualityResult:
        """Ensure values within min/max bounds."""
        if rule.column not in df.columns:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message=f"Column '{rule.column}' not found",
                failed_count=0,
                total_count=len(df)
            )
        
        min_value = rule.parameters.get("min_value")
        max_value = rule.parameters.get("max_value")
        
        col_data = df[rule.column].dropna()
        
        if len(col_data) == 0:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=True,
                severity=rule.severity,
                message="Column is empty",
                failed_count=0,
                total_count=len(df)
            )
        
        failed_mask = pd.Series([False] * len(df))
        
        if min_value is not None:
            failed_mask |= (df[rule.column] < min_value)
        if max_value is not None:
            failed_mask |= (df[rule.column] > max_value)
        
        failed_count = failed_mask.sum()
        passed = failed_count == 0
        
        failed_values = df[rule.column][failed_mask].head(10).tolist() if not passed else []
        
        return QualityResult(
            rule_id=rule.rule_id,
            passed=passed,
            severity=rule.severity,
            message=f"Failed count: {failed_count} (min: {min_value}, max: {max_value})",
            failed_count=int(failed_count),
            total_count=len(df),
            failed_values=failed_values
        )
    
    def _check_pattern(
        self,
        df: pd.DataFrame,
        rule: QualityRule
    ) -> QualityResult:
        """Regex validation."""
        if rule.column not in df.columns:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message=f"Column '{rule.column}' not found",
                failed_count=0,
                total_count=len(df)
            )
        
        pattern = rule.parameters.get("pattern")
        if not pattern:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message="Pattern not provided",
                failed_count=0,
                total_count=len(df)
            )
        
        col_data = df[rule.column].dropna()
        
        if len(col_data) == 0:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=True,
                severity=rule.severity,
                message="Column is empty",
                failed_count=0,
                total_count=len(df)
            )
        
        # Check pattern match
        matches = col_data.astype(str).str.match(pattern, na=False)
        failed_count = (~matches).sum()
        passed = failed_count == 0
        
        failed_values = col_data[~matches].head(10).tolist() if not passed else []
        
        return QualityResult(
            rule_id=rule.rule_id,
            passed=passed,
            severity=rule.severity,
            message=f"Failed count: {failed_count} (pattern: {pattern})",
            failed_count=int(failed_count),
            total_count=len(col_data),
            failed_values=failed_values
        )
    
    def _check_uniqueness(
        self,
        df: pd.DataFrame,
        rule: QualityRule
    ) -> QualityResult:
        """Check for duplicates."""
        if rule.column not in df.columns:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message=f"Column '{rule.column}' not found",
                failed_count=0,
                total_count=len(df)
            )
        
        col_data = df[rule.column].dropna()
        unique_count = col_data.nunique()
        total_count = len(col_data)
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0
        
        return QualityResult(
            rule_id=rule.rule_id,
            passed=passed,
            severity=rule.severity,
            message=f"Duplicates: {duplicate_count} (unique: {unique_count}, total: {total_count})",
            failed_count=duplicate_count,
            total_count=total_count
        )
    
    def _check_freshness(
        self,
        df: pd.DataFrame,
        rule: QualityRule
    ) -> QualityResult:
        """Ensure data is recent (compare timestamp)."""
        if rule.column not in df.columns:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message=f"Column '{rule.column}' not found",
                failed_count=0,
                total_count=len(df)
            )
        
        max_age_hours = rule.parameters.get("max_age_hours", 24)
        
        try:
            # Convert to datetime if needed
            col_data = pd.to_datetime(df[rule.column], errors='coerce').dropna()
            
            if len(col_data) == 0:
                return QualityResult(
                    rule_id=rule.rule_id,
                    passed=False,
                    severity=rule.severity,
                    message="No valid timestamps found",
                    failed_count=len(df),
                    total_count=len(df)
                )
            
            from datetime import datetime, timedelta
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            
            stale_count = (col_data < cutoff_time).sum()
            passed = stale_count == 0
            
            return QualityResult(
                rule_id=rule.rule_id,
                passed=passed,
                severity=rule.severity,
                message=f"Stale records: {stale_count} (max age: {max_age_hours} hours)",
                failed_count=int(stale_count),
                total_count=len(col_data)
            )
        except Exception as e:
            return QualityResult(
                rule_id=rule.rule_id,
                passed=False,
                severity=rule.severity,
                message=f"Error checking freshness: {str(e)}",
                failed_count=0,
                total_count=len(df)
            )
    
    def calculate_quality_score(
        self,
        results: List[QualityResult]
    ) -> float:
        """
        Calculate weighted quality score (0-100).
        
        Weights:
        - error severity: 10 points
        - warning severity: 5 points
        - info severity: 1 point
        
        Score = 100 - (sum of deductions)
        
        Args:
            results: List of quality results
            
        Returns:
            Quality score (0-100)
        """
        if not results:
            return 100.0
        
        total_deduction = 0.0
        
        for result in results:
            if result.passed:
                continue
            
            if result.severity == "error":
                deduction = 10.0
            elif result.severity == "warning":
                deduction = 5.0
            else:
                deduction = 1.0
            
            # Weight by failure rate
            failure_rate = result.failed_count / max(result.total_count, 1)
            total_deduction += deduction * failure_rate
        
        score = max(0.0, 100.0 - total_deduction)
        return round(score, 2)
    
    def generate_report(
        self,
        results: List[QualityResult]
    ) -> Dict[str, Any]:
        """
        Generate quality report.
        
        Includes:
        - Overall score
        - Pass/fail counts by severity
        - Top 10 issues
        - Recommendations for fixes
        
        Args:
            results: List of quality results
            
        Returns:
            Quality report dictionary
        """
        score = self.calculate_quality_score(results)
        
        passed_count = sum(1 for r in results if r.passed)
        failed_count = sum(1 for r in results if not r.passed)
        
        error_count = sum(1 for r in results if not r.passed and r.severity == "error")
        warning_count = sum(1 for r in results if not r.passed and r.severity == "warning")
        info_count = sum(1 for r in results if not r.passed and r.severity == "info")
        
        # Top issues (sorted by severity and failure count)
        failed_results = [r for r in results if not r.passed]
        failed_results.sort(
            key=lambda x: (
                0 if x.severity == "error" else 1 if x.severity == "warning" else 2,
                -x.failed_count
            )
        )
        top_issues = failed_results[:10]
        
        return {
            "overall_score": score,
            "total_rules": len(results),
            "passed_count": passed_count,
            "failed_count": failed_count,
            "error_count": error_count,
            "warning_count": warning_count,
            "info_count": info_count,
            "top_issues": [
                {
                    "rule_id": r.rule_id,
                    "severity": r.severity,
                    "message": r.message,
                    "failed_count": r.failed_count
                }
                for r in top_issues
            ],
            "recommendations": self._generate_recommendations(failed_results)
        }
    
    def _generate_recommendations(
        self,
        failed_results: List[QualityResult]
    ) -> List[str]:
        """Generate recommendations for fixing issues."""
        recommendations = []
        
        for result in failed_results:
            if "null" in result.message.lower():
                recommendations.append(
                    f"Column '{result.rule_id}' has high null percentage. Consider data cleaning or default values."
                )
            elif "pattern" in result.message.lower():
                recommendations.append(
                    f"Column '{result.rule_id}' has pattern mismatches. Review data format."
                )
            elif "duplicate" in result.message.lower():
                recommendations.append(
                    f"Column '{result.rule_id}' has duplicates. Consider deduplication."
                )
        
        return recommendations[:5]  # Top 5 recommendations

