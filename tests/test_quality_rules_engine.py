"""
Unit tests for data quality rules engine.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.quality.rules_engine import (
    QualityRulesEngine,
    QualityRule,
    RuleType,
    QualityResult
)


class TestQualityRulesEngine:
    """Test cases for QualityRulesEngine class."""
    
    def test_initialization(self):
        """Test engine initialization."""
        engine = QualityRulesEngine()
        assert engine is not None
        assert hasattr(engine, 'BUILT_IN_RULES')
    
    def test_null_threshold_rule_pass(self):
        """Test null threshold rule that passes."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, None, 40]  # 20% nulls
        })
        
        rule = QualityRule(
            rule_id="age_completeness",
            rule_type=RuleType.NULL_THRESHOLD,
            column="age",
            parameters={"max_null_percent": 25},
            severity="warning"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is True
        assert result.failed_count == 0
        assert "20.00%" in result.message
    
    def test_null_threshold_rule_fail(self):
        """Test null threshold rule that fails."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, None, None, None, None]  # 80% nulls
        })
        
        rule = QualityRule(
            rule_id="age_completeness",
            rule_type=RuleType.NULL_THRESHOLD,
            column="age",
            parameters={"max_null_percent": 20},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 4
        assert "80.00%" in result.message
    
    def test_type_check_rule_pass(self):
        """Test type check rule that passes."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'age': [25, 30, 35, 40, 45]
        })
        
        rule = QualityRule(
            rule_id="age_type",
            rule_type=RuleType.TYPE_CHECK,
            column="age",
            parameters={"expected_type": "int"},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is True
    
    def test_type_check_rule_fail(self):
        """Test type check rule that fails."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'age': ['25', '30', '35', '40', '45']  # Strings instead of ints
        })
        
        rule = QualityRule(
            rule_id="age_type",
            rule_type=RuleType.TYPE_CHECK,
            column="age",
            parameters={"expected_type": "int"},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is False
    
    def test_range_check_rule_pass(self):
        """Test range check rule that passes."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'score': [85, 90, 95, 88, 92]
        })
        
        rule = QualityRule(
            rule_id="score_range",
            rule_type=RuleType.RANGE_CHECK,
            column="score",
            parameters={"min_value": 0, "max_value": 100},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is True
        assert result.failed_count == 0
    
    def test_range_check_rule_fail(self):
        """Test range check rule that fails."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'score': [85, 90, 150, 88, -10]  # Out of range values
        })
        
        rule = QualityRule(
            rule_id="score_range",
            rule_type=RuleType.RANGE_CHECK,
            column="score",
            parameters={"min_value": 0, "max_value": 100},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 2
        assert len(result.failed_values) == 2
    
    def test_pattern_match_rule_pass(self):
        """Test pattern match rule that passes."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'email': ['alice@example.com', 'bob@test.com', 'charlie@domain.org']
        })
        
        rule = QualityRule(
            rule_id="email_format",
            rule_type=RuleType.PATTERN_MATCH,
            column="email",
            parameters={"pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is True
    
    def test_pattern_match_rule_fail(self):
        """Test pattern match rule that fails."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'email': ['alice@example.com', 'invalid-email', 'bob@test.com', 'bad-format']
        })
        
        rule = QualityRule(
            rule_id="email_format",
            rule_type=RuleType.PATTERN_MATCH,
            column="email",
            parameters={"pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 2
    
    def test_uniqueness_rule_pass(self):
        """Test uniqueness rule that passes."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5]
        })
        
        rule = QualityRule(
            rule_id="id_uniqueness",
            rule_type=RuleType.UNIQUENESS,
            column="id",
            parameters={},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is True
    
    def test_uniqueness_rule_fail(self):
        """Test uniqueness rule that fails."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'id': [1, 2, 2, 3, 3]  # Duplicates
        })
        
        rule = QualityRule(
            rule_id="id_uniqueness",
            rule_type=RuleType.UNIQUENESS,
            column="id",
            parameters={},
            severity="error"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 2
    
    def test_freshness_rule_pass(self):
        """Test freshness rule that passes."""
        engine = QualityRulesEngine()
        now = datetime.now()
        df = pd.DataFrame({
            'updated_at': [
                now - timedelta(hours=1),
                now - timedelta(hours=2),
                now - timedelta(hours=3)
            ]
        })
        
        rule = QualityRule(
            rule_id="data_freshness",
            rule_type=RuleType.FRESHNESS,
            column="updated_at",
            parameters={"max_age_hours": 24},
            severity="warning"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is True
    
    def test_freshness_rule_fail(self):
        """Test freshness rule that fails."""
        engine = QualityRulesEngine()
        now = datetime.now()
        df = pd.DataFrame({
            'updated_at': [
                now - timedelta(hours=1),
                now - timedelta(days=2),  # Too old
                now - timedelta(days=3)   # Too old
            ]
        })
        
        rule = QualityRule(
            rule_id="data_freshness",
            rule_type=RuleType.FRESHNESS,
            column="updated_at",
            parameters={"max_age_hours": 24},
            severity="warning"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is False
        assert result.failed_count == 2
    
    def test_apply_rules_multiple(self):
        """Test applying multiple rules."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({
            'email': ['alice@example.com', 'invalid', 'bob@test.com'],
            'age': [25, 30, None],
            'score': [85, 150, 90]  # One out of range
        })
        
        rules = [
            QualityRule(
                rule_id="email_format",
                rule_type=RuleType.PATTERN_MATCH,
                column="email",
                parameters={"pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'},
                severity="error"
            ),
            QualityRule(
                rule_id="age_completeness",
                rule_type=RuleType.NULL_THRESHOLD,
                column="age",
                parameters={"max_null_percent": 50},
                severity="warning"
            ),
            QualityRule(
                rule_id="score_range",
                rule_type=RuleType.RANGE_CHECK,
                column="score",
                parameters={"min_value": 0, "max_value": 100},
                severity="error"
            )
        ]
        
        results = engine.apply_rules(df, rules)
        
        assert len(results) == 3
        assert results[0].passed is False  # Email format fails
        assert results[1].passed is True   # Age completeness passes
        assert results[2].passed is False  # Score range fails
    
    def test_calculate_quality_score(self):
        """Test quality score calculation."""
        engine = QualityRulesEngine()
        
        results = [
            QualityResult(
                rule_id="rule1",
                passed=True,
                severity="error",
                message="Passed",
                failed_count=0,
                total_count=100
            ),
            QualityResult(
                rule_id="rule2",
                passed=False,
                severity="error",
                message="Failed",
                failed_count=10,
                total_count=100
            ),
            QualityResult(
                rule_id="rule3",
                passed=False,
                severity="warning",
                message="Failed",
                failed_count=5,
                total_count=100
            )
        ]
        
        score = engine.calculate_quality_score(results)
        
        # Should be weighted average based on passed/failed rules
        # With 1 passed and 2 failed, score should be less than 100
        assert 0 <= score <= 100
        assert score < 100  # Not all rules passed
    
    def test_generate_report(self):
        """Test report generation."""
        engine = QualityRulesEngine()
        
        results = [
            QualityResult(
                rule_id="rule1",
                passed=True,
                severity="error",
                message="Passed",
                failed_count=0,
                total_count=100
            ),
            QualityResult(
                rule_id="rule2",
                passed=False,
                severity="error",
                message="Failed",
                failed_count=10,
                total_count=100
            ),
            QualityResult(
                rule_id="rule3",
                passed=False,
                severity="warning",
                message="Failed",
                failed_count=5,
                total_count=100
            )
        ]
        
        report = engine.generate_report(results)
        
        assert 'overall_score' in report
        assert 'total_rules' in report
        assert 'failed_count' in report or 'failed_rules' in report
        assert 'top_issues' in report
        assert report['total_rules'] == 3
        # Check that we have failed rules
        failed_count = report.get('failed_count', report.get('failed_rules', 0))
        assert failed_count == 2
        assert len(report['top_issues']) == 2  # Two failed rules
    
    def test_apply_rules_empty_dataframe(self):
        """Test applying rules to empty DataFrame."""
        engine = QualityRulesEngine()
        df = pd.DataFrame()
        
        rule = QualityRule(
            rule_id="test_rule",
            rule_type=RuleType.NULL_THRESHOLD,
            column="nonexistent",
            parameters={"max_null_percent": 10},
            severity="warning"
        )
        
        # Should handle gracefully
        result = engine._execute_rule(df, rule)
        assert result is not None
    
    def test_apply_rules_missing_column(self):
        """Test applying rules when column doesn't exist."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({'other_column': [1, 2, 3]})
        
        rule = QualityRule(
            rule_id="test_rule",
            rule_type=RuleType.NULL_THRESHOLD,
            column="nonexistent",
            parameters={"max_null_percent": 10},
            severity="warning"
        )
        
        result = engine._execute_rule(df, rule)
        assert result.passed is False
        assert "not found" in result.message.lower() or "error" in result.message.lower()
    
    def test_apply_rules_error_handling(self):
        """Test error handling in apply_rules."""
        engine = QualityRulesEngine()
        df = pd.DataFrame({'col': [1, 2, 3]})
        
        # Create a rule that will cause an error
        rule = QualityRule(
            rule_id="bad_rule",
            rule_type=RuleType.CUSTOM,
            column="col",
            parameters={"invalid": "params"},
            severity="error"
        )
        
        results = engine.apply_rules(df, [rule])
        
        assert len(results) == 1
        assert results[0].passed is False
        # Message should indicate unknown rule type
        assert "unknown" in results[0].message.lower() or "error" in results[0].message.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

