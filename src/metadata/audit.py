"""
Audit Trail Module

Records all AI repair suggestions, approvals, and transformations with tamper-evidence.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import json
import os
import hashlib

from ..utils.logging import get_logger

logger = get_logger(__name__)


class AuditTrail:
    """Audit trail for tracking all operations with tamper-evidence."""
    
    def __init__(self, metadata_base: Optional[Path] = None):
        """Initialize audit trail.
        
        Args:
            metadata_base: Base metadata directory (defaults to METADATA_BASE env var or /metadata)
        """
        if metadata_base is None:
            metadata_base = Path(os.getenv("METADATA_BASE", "/metadata"))
        
        self.metadata_base = metadata_base
        self.audit_dir = metadata_base / "audit"
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger(__name__)
    
    def record_suggestion(self, job_id: str, suggestion: Dict[str, Any], 
                         gx_report: Dict[str, Any], approved_by: Optional[str] = None,
                         timestamp: Optional[datetime] = None) -> str:
        """Record AI repair suggestion with GX report.
        
        Args:
            job_id: Job identifier
            suggestion: Repair suggestion dictionary
            gx_report: GX validation report
            approved_by: User who approved (optional)
            timestamp: Timestamp (defaults to now)
            
        Returns:
            Audit record ID
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Create audit record
        audit_record = {
            "audit_id": self._generate_audit_id(job_id, timestamp),
            "job_id": job_id,
            "timestamp": timestamp.isoformat() + "Z",
            "type": "repair_suggestion",
            "suggestion": suggestion,
            "gx_report": gx_report,
            "approved_by": approved_by,
            "approved": approved_by is not None
        }
        
        # Compute hash for tamper-evidence
        record_hash = self._compute_hash(audit_record)
        audit_record["hash"] = record_hash
        
        # Save to file
        audit_file = self.audit_dir / f"{audit_record['audit_id']}.json"
        with open(audit_file, 'w') as f:
            json.dump(audit_record, f, indent=2, default=str)
        
        self.logger.info(
            f"Audit record created: {audit_record['audit_id']}",
            extra={
                'event_type': 'audit_record_created',
                'audit_id': audit_record['audit_id'],
                'job_id': job_id,
                'approved': approved_by is not None
            }
        )
        
        return audit_record['audit_id']
    
    def record_approval(self, audit_id: str, approved_by: str, 
                       timestamp: Optional[datetime] = None) -> bool:
        """Record approval of a repair suggestion.
        
        Args:
            audit_id: Audit record ID
            approved_by: User who approved
            timestamp: Timestamp (defaults to now)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            audit_file = self.audit_dir / f"{audit_id}.json"
            
            if not audit_file.exists():
                self.logger.error(
                    f"Audit record not found: {audit_id}",
                    extra={
                        'event_type': 'audit_record_not_found',
                        'audit_id': audit_id
                    }
                )
                return False
            
            # Read existing record
            with open(audit_file, 'r') as f:
                audit_record = json.load(f)
            
            # Update approval
            if timestamp is None:
                timestamp = datetime.utcnow()
            
            audit_record["approved_by"] = approved_by
            audit_record["approved"] = True
            audit_record["approval_timestamp"] = timestamp.isoformat() + "Z"
            
            # Recompute hash
            old_hash = audit_record.get("hash")
            audit_record["hash"] = self._compute_hash(audit_record)
            audit_record["previous_hash"] = old_hash
            
            # Write back
            with open(audit_file, 'w') as f:
                json.dump(audit_record, f, indent=2, default=str)
            
            self.logger.info(
                f"Audit record approved: {audit_id} by {approved_by}",
                extra={
                    'event_type': 'audit_record_approved',
                    'audit_id': audit_id,
                    'approved_by': approved_by
                }
            )
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"Failed to record approval: {e}",
                exc_info=True,
                extra={
                    'event_type': 'audit_approval_error',
                    'audit_id': audit_id,
                    'error': str(e)
                }
            )
            return False
    
    def verify_record(self, audit_id: str) -> Dict[str, Any]:
        """Verify audit record integrity.
        
        Args:
            audit_id: Audit record ID
            
        Returns:
            Verification result dictionary
        """
        try:
            audit_file = self.audit_dir / f"{audit_id}.json"
            
            if not audit_file.exists():
                return {
                    "valid": False,
                    "error": "Record not found"
                }
            
            # Read record
            with open(audit_file, 'r') as f:
                audit_record = json.load(f)
            
            # Extract stored hash
            stored_hash = audit_record.pop("hash", None)
            
            # Recompute hash
            computed_hash = self._compute_hash(audit_record)
            
            # Verify
            is_valid = stored_hash == computed_hash
            
            # Restore hash
            audit_record["hash"] = stored_hash
            
            result = {
                "valid": is_valid,
                "stored_hash": stored_hash,
                "computed_hash": computed_hash,
                "tampered": not is_valid
            }
            
            if not is_valid:
                self.logger.warning(
                    f"Audit record tampered: {audit_id}",
                    extra={
                        'event_type': 'audit_record_tampered',
                        'audit_id': audit_id
                    }
                )
            
            return result
            
        except Exception as e:
            self.logger.error(
                f"Failed to verify audit record: {e}",
                exc_info=True,
                extra={
                    'event_type': 'audit_verification_error',
                    'audit_id': audit_id,
                    'error': str(e)
                }
            )
            return {
                "valid": False,
                "error": str(e)
            }
    
    def _generate_audit_id(self, job_id: str, timestamp: datetime) -> str:
        """Generate deterministic audit record ID.
        
        Args:
            job_id: Job identifier
            timestamp: Timestamp
            
        Returns:
            Audit record ID
        """
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S_%f")
        combined = f"{job_id}_{timestamp_str}"
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()[:16]
    
    def _compute_hash(self, record: Dict[str, Any]) -> str:
        """Compute SHA256 hash of audit record for tamper-evidence.
        
        Args:
            record: Audit record dictionary
            
        Returns:
            SHA256 hash
        """
        # Create deterministic JSON representation
        record_copy = record.copy()
        # Remove hash if present for computation
        record_copy.pop("hash", None)
        record_copy.pop("previous_hash", None)
        
        # Sort keys for deterministic hashing
        record_str = json.dumps(record_copy, sort_keys=True, default=str)
        return hashlib.sha256(record_str.encode('utf-8')).hexdigest()
    
    def get_audit_records(self, job_id: Optional[str] = None, 
                         limit: int = 100) -> List[Dict[str, Any]]:
        """Get audit records.
        
        Args:
            job_id: Filter by job ID (optional)
            limit: Maximum number of records to return
            
        Returns:
            List of audit records
        """
        records = []
        
        try:
            for audit_file in sorted(self.audit_dir.glob("*.json"), reverse=True)[:limit]:
                with open(audit_file, 'r') as f:
                    record = json.load(f)
                
                if job_id is None or record.get("job_id") == job_id:
                    records.append(record)
            
            return records
            
        except Exception as e:
            self.logger.error(
                f"Failed to get audit records: {e}",
                exc_info=True,
                extra={
                    'event_type': 'audit_get_records_error',
                    'error': str(e)
                }
            )
            return []

