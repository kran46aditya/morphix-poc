"""Pre-built quality rule templates."""

from .rules_engine import QualityRule, RuleType


RULE_TEMPLATES = {
    "email_validation": QualityRule(
        rule_id="email_check",
        rule_type=RuleType.PATTERN_MATCH,
        column="email",
        parameters={"pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'},
        severity="error"
    ),
    "phone_validation": QualityRule(
        rule_id="phone_check",
        rule_type=RuleType.PATTERN_MATCH,
        column="phone",
        parameters={"pattern": r'^\+?[\d\s\-\(\)]{10,}$'},
        severity="warning"
    ),
    "no_nulls": QualityRule(
        rule_id="no_nulls",
        rule_type=RuleType.NULL_THRESHOLD,
        column="id",
        parameters={"max_null_percent": 0},
        severity="error"
    ),
    "mostly_complete": QualityRule(
        rule_id="mostly_complete",
        rule_type=RuleType.NULL_THRESHOLD,
        column="description",
        parameters={"max_null_percent": 10},
        severity="warning"
    ),
    "unique_id": QualityRule(
        rule_id="unique_id",
        rule_type=RuleType.UNIQUENESS,
        column="id",
        parameters={},
        severity="error"
    ),
    "fresh_data": QualityRule(
        rule_id="fresh_data",
        rule_type=RuleType.FRESHNESS,
        column="updated_at",
        parameters={"max_age_hours": 24},
        severity="warning"
    )
}

