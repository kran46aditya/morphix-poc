"""
Transform Plan Manager

Manages versioned transform plans with rollback capability.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
import json
import os
import hashlib

from ..utils.logging import get_logger

logger = get_logger(__name__)


class PlanManager:
    """Manages versioned transform plans."""
    
    def __init__(self, metadata_base: Optional[Path] = None):
        """Initialize plan manager.
        
        Args:
            metadata_base: Base metadata directory (defaults to METADATA_BASE env var or /metadata)
        """
        if metadata_base is None:
            metadata_base = Path(os.getenv("METADATA_BASE", "/metadata"))
        
        self.metadata_base = metadata_base
        self.plans_dir = metadata_base / "plans"
        self.plans_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger(__name__)
    
    def create_plan(self, collection: str, input_schema_hash: str, 
                   output_schema_hash: str, operations_list: List[Dict[str, Any]],
                   version: Optional[int] = None) -> Dict[str, Any]:
        """Create a new transform plan.
        
        Args:
            collection: Collection name
            input_schema_hash: Hash of input schema
            output_schema_hash: Hash of output schema
            operations_list: List of transformation operations
            version: Plan version (auto-incremented if not provided)
            
        Returns:
            Plan dictionary
        """
        try:
            # Get next version if not provided
            if version is None:
                version = self._get_next_version(collection)
            
            # Create plan
            plan = {
                "plan_id": self._generate_plan_id(collection, version),
                "collection": collection,
                "version": version,
                "input_schema_hash": input_schema_hash,
                "output_schema_hash": output_schema_hash,
                "operations": operations_list,
                "created_at": datetime.utcnow().isoformat() + "Z",
                "approved": False,
                "applied": False
            }
            
            # Generate rollback plan
            rollback_plan = self._generate_rollback_plan(plan)
            plan["rollback_plan"] = rollback_plan
            
            # Save plan
            plan_dir = self.plans_dir / collection
            plan_dir.mkdir(parents=True, exist_ok=True)
            plan_file = plan_dir / f"{version}.json"
            
            with open(plan_file, 'w') as f:
                json.dump(plan, f, indent=2, default=str)
            
            self.logger.info(
                f"Created transform plan: {plan['plan_id']}",
                extra={
                    'event_type': 'transform_plan_created',
                    'plan_id': plan['plan_id'],
                    'collection': collection,
                    'version': version,
                    'operation_count': len(operations_list)
                }
            )
            
            return plan
            
        except Exception as e:
            self.logger.error(
                f"Failed to create transform plan: {e}",
                exc_info=True,
                extra={
                    'event_type': 'transform_plan_create_error',
                    'collection': collection,
                    'error': str(e)
                }
            )
            raise
    
    def _generate_rollback_plan(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate rollback plan by computing inverse operations.
        
        Args:
            plan: Transform plan dictionary
            
        Returns:
            List of inverse operations for rollback
        """
        rollback_ops = []
        operations = plan.get("operations", [])
        
        # Reverse operations in reverse order
        for op in reversed(operations):
            op_type = op.get("type")
            field = op.get("field")
            
            if op_type == "type_conversion":
                # Rollback: convert back to original type
                original_type = op.get("original_type")
                rollback_ops.append({
                    "type": "type_conversion",
                    "field": field,
                    "target_type": original_type,
                    "original_type": op.get("target_type")
                })
            
            elif op_type == "add_field":
                # Rollback: remove field
                rollback_ops.append({
                    "type": "remove_field",
                    "field": field
                })
            
            elif op_type == "remove_field":
                # Rollback: add field back with original value (if stored)
                original_value = op.get("original_value")
                rollback_ops.append({
                    "type": "add_field",
                    "field": field,
                    "default_value": original_value
                })
            
            elif op_type == "fill_null":
                # Rollback: set back to null (cannot fully rollback, but mark for review)
                rollback_ops.append({
                    "type": "set_null",
                    "field": field,
                    "note": "Partial rollback - original null values cannot be restored"
                })
        
        return rollback_ops
    
    def get_plan(self, collection: str, version: int) -> Optional[Dict[str, Any]]:
        """Get transform plan by version.
        
        Args:
            collection: Collection name
            version: Plan version
            
        Returns:
            Plan dictionary or None if not found
        """
        try:
            plan_file = self.plans_dir / collection / f"{version}.json"
            
            if not plan_file.exists():
                return None
            
            with open(plan_file, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            self.logger.error(
                f"Failed to get transform plan: {e}",
                exc_info=True,
                extra={
                    'event_type': 'transform_plan_get_error',
                    'collection': collection,
                    'version': version,
                    'error': str(e)
                }
            )
            return None
    
    def list_plans(self, collection: str) -> List[Dict[str, Any]]:
        """List all plans for a collection.
        
        Args:
            collection: Collection name
            
        Returns:
            List of plan dictionaries
        """
        plans = []
        plan_dir = self.plans_dir / collection
        
        if not plan_dir.exists():
            return plans
        
        try:
            for plan_file in sorted(plan_dir.glob("*.json")):
                with open(plan_file, 'r') as f:
                    plan = json.load(f)
                    plans.append(plan)
            
            return sorted(plans, key=lambda p: p.get("version", 0), reverse=True)
            
        except Exception as e:
            self.logger.error(
                f"Failed to list transform plans: {e}",
                exc_info=True,
                extra={
                    'event_type': 'transform_plan_list_error',
                    'collection': collection,
                    'error': str(e)
                }
            )
            return []
    
    def approve_plan(self, collection: str, version: int, approved_by: str) -> bool:
        """Approve a transform plan.
        
        Args:
            collection: Collection name
            version: Plan version
            approved_by: User who approved
            
        Returns:
            True if successful, False otherwise
        """
        try:
            plan = self.get_plan(collection, version)
            if not plan:
                return False
            
            plan["approved"] = True
            plan["approved_by"] = approved_by
            plan["approved_at"] = datetime.utcnow().isoformat() + "Z"
            
            # Save updated plan
            plan_file = self.plans_dir / collection / f"{version}.json"
            with open(plan_file, 'w') as f:
                json.dump(plan, f, indent=2, default=str)
            
            self.logger.info(
                f"Transform plan approved: {plan['plan_id']} by {approved_by}",
                extra={
                    'event_type': 'transform_plan_approved',
                    'plan_id': plan['plan_id'],
                    'collection': collection,
                    'version': version,
                    'approved_by': approved_by
                }
            )
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"Failed to approve transform plan: {e}",
                exc_info=True,
                extra={
                    'event_type': 'transform_plan_approve_error',
                    'collection': collection,
                    'version': version,
                    'error': str(e)
                }
            )
            return False
    
    def mark_plan_applied(self, collection: str, version: int) -> bool:
        """Mark a plan as applied.
        
        Args:
            collection: Collection name
            version: Plan version
            
        Returns:
            True if successful, False otherwise
        """
        try:
            plan = self.get_plan(collection, version)
            if not plan:
                return False
            
            plan["applied"] = True
            plan["applied_at"] = datetime.utcnow().isoformat() + "Z"
            
            # Save updated plan
            plan_file = self.plans_dir / collection / f"{version}.json"
            with open(plan_file, 'w') as f:
                json.dump(plan, f, indent=2, default=str)
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"Failed to mark plan as applied: {e}",
                exc_info=True,
                extra={
                    'event_type': 'transform_plan_mark_applied_error',
                    'collection': collection,
                    'version': version,
                    'error': str(e)
                }
            )
            return False
    
    def _get_next_version(self, collection: str) -> int:
        """Get next version number for a collection.
        
        Args:
            collection: Collection name
            
        Returns:
            Next version number
        """
        plans = self.list_plans(collection)
        if not plans:
            return 1
        
        max_version = max(p.get("version", 0) for p in plans)
        return max_version + 1
    
    def _generate_plan_id(self, collection: str, version: int) -> str:
        """Generate deterministic plan ID.
        
        Args:
            collection: Collection name
            version: Plan version
            
        Returns:
            Plan ID
        """
        combined = f"{collection}_v{version}_{datetime.utcnow().strftime('%Y%m%d')}"
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()[:16]

