"""
Policy Engine for Morphix

Reads YAML policy configuration and enforces rules on operations.
"""

from typing import Dict, Any, List, Optional
from pathlib import Path
import os
import yaml

from ..utils.logging import get_logger

logger = get_logger(__name__)


class PolicyEngine:
    """Policy engine for enforcing data governance rules."""
    
    def __init__(self, policy_file: Optional[str] = None):
        """Initialize policy engine.
        
        Args:
            policy_file: Path to policy YAML file (defaults to /config/policy.yaml)
        """
        if policy_file is None:
            policy_file = os.getenv("POLICY_FILE", "/config/policy.yaml")
        
        self.policy_file = Path(policy_file)
        self.policy: Dict[str, Any] = {}
        self.logger = get_logger(__name__)
        
        # Load policy
        self._load_policy()
    
    def _load_policy(self):
        """Load policy from YAML file."""
        try:
            if self.policy_file.exists():
                with open(self.policy_file, 'r') as f:
                    self.policy = yaml.safe_load(f) or {}
                
                self.logger.info(
                    f"Policy loaded from {self.policy_file}",
                    extra={
                        'event_type': 'policy_loaded',
                        'policy_file': str(self.policy_file)
                    }
                )
            else:
                self.logger.warning(
                    f"Policy file not found: {self.policy_file}. Using default empty policy.",
                    extra={
                        'event_type': 'policy_file_not_found',
                        'policy_file': str(self.policy_file)
                    }
                )
                self.policy = {}
        except Exception as e:
            self.logger.error(
                f"Failed to load policy: {e}",
                exc_info=True,
                extra={
                    'event_type': 'policy_load_error',
                    'policy_file': str(self.policy_file),
                    'error': str(e)
                }
            )
            self.policy = {}
    
    def reload_policy(self):
        """Reload policy from file."""
        self._load_policy()
    
    def check_blocked_fields(self, fields: List[str], collection: Optional[str] = None) -> Dict[str, Any]:
        """Check if any fields are blocked.
        
        Args:
            fields: List of field names to check
            collection: Collection name (optional, for collection-specific rules)
            
        Returns:
            Dictionary with blocked fields and violations
        """
        blocked_fields = self.policy.get("blocked_fields", [])
        collection_blocked = []
        
        if collection:
            collection_policy = self.policy.get("collections", {}).get(collection, {})
            collection_blocked = collection_policy.get("blocked_fields", [])
        
        # Combine global and collection-specific blocked fields
        all_blocked = set(blocked_fields) | set(collection_blocked)
        
        # Find violations
        violations = [field for field in fields if field in all_blocked]
        
        result = {
            "allowed": len(violations) == 0,
            "blocked_fields": list(all_blocked),
            "violations": violations
        }
        
        if violations:
            self.logger.warning(
                f"Blocked fields detected: {violations}",
                extra={
                    'event_type': 'policy_blocked_fields_violation',
                    'collection': collection,
                    'violations': violations
                }
            )
        
        return result
    
    def check_restricted_operations(self, operation: str, collection: Optional[str] = None) -> Dict[str, Any]:
        """Check if an operation is restricted.
        
        Args:
            operation: Operation name (e.g., "delete", "update", "write")
            collection: Collection name (optional, for collection-specific rules)
            
        Returns:
            Dictionary with operation status and restrictions
        """
        restricted_operations = self.policy.get("restricted_operations", [])
        collection_restricted = []
        
        if collection:
            collection_policy = self.policy.get("collections", {}).get(collection, {})
            collection_restricted = collection_policy.get("restricted_operations", [])
        
        # Combine global and collection-specific restrictions
        all_restricted = set(restricted_operations) | set(collection_restricted)
        
        is_restricted = operation in all_restricted
        
        result = {
            "allowed": not is_restricted,
            "operation": operation,
            "restricted": is_restricted
        }
        
        if is_restricted:
            self.logger.warning(
                f"Restricted operation detected: {operation}",
                extra={
                    'event_type': 'policy_restricted_operation_violation',
                    'collection': collection,
                    'operation': operation
                }
            )
        
        return result
    
    def check_manual_approval_required(self, operation: str, collection: Optional[str] = None) -> Dict[str, Any]:
        """Check if manual approval is required for an operation.
        
        Args:
            operation: Operation name
            collection: Collection name (optional)
            
        Returns:
            Dictionary with approval requirement status
        """
        require_approval = self.policy.get("require_manual_approval", [])
        collection_approval = []
        
        if collection:
            collection_policy = self.policy.get("collections", {}).get(collection, {})
            collection_approval = collection_policy.get("require_manual_approval", [])
        
        # Combine global and collection-specific requirements
        all_require_approval = set(require_approval) | set(collection_approval)
        
        requires_approval = operation in all_require_approval
        
        result = {
            "requires_approval": requires_approval,
            "operation": operation,
            "approval_required": requires_approval
        }
        
        if requires_approval:
            self.logger.info(
                f"Manual approval required for operation: {operation}",
                extra={
                    'event_type': 'policy_approval_required',
                    'collection': collection,
                    'operation': operation
                }
            )
        
        return result
    
    def enforce_policy(self, operation: str, fields: Optional[List[str]] = None,
                      collection: Optional[str] = None,
                      approved: bool = False) -> Dict[str, Any]:
        """Enforce all policy checks for an operation.
        
        Args:
            operation: Operation name
            fields: List of fields involved (optional)
            collection: Collection name (optional)
            approved: Whether operation has been manually approved (default: False)
            
        Returns:
            Dictionary with enforcement result
        """
        violations = []
        warnings = []
        
        # Check blocked fields
        if fields:
            field_check = self.check_blocked_fields(fields, collection)
            if not field_check["allowed"]:
                violations.append({
                    "type": "blocked_fields",
                    "details": field_check
                })
        
        # Check restricted operations
        operation_check = self.check_restricted_operations(operation, collection)
        if not operation_check["allowed"]:
            violations.append({
                "type": "restricted_operation",
                "details": operation_check
            })
        
        # Check manual approval requirement
        approval_check = self.check_manual_approval_required(operation, collection)
        if approval_check["requires_approval"] and not approved:
            violations.append({
                "type": "approval_required",
                "details": approval_check
            })
        
        # Determine if operation is allowed
        allowed = len(violations) == 0
        
        result = {
            "allowed": allowed,
            "violations": violations,
            "warnings": warnings,
            "operation": operation,
            "collection": collection
        }
        
        if not allowed:
            self.logger.error(
                f"Policy enforcement failed for operation: {operation}",
                extra={
                    'event_type': 'policy_enforcement_failed',
                    'operation': operation,
                    'collection': collection,
                    'violations': violations
                }
            )
        else:
            self.logger.info(
                f"Policy enforcement passed for operation: {operation}",
                extra={
                    'event_type': 'policy_enforcement_passed',
                    'operation': operation,
                    'collection': collection
                }
            )
        
        return result

