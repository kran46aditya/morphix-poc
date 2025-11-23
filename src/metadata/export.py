"""
Pilot Export Module

Exports lineage graphs, GX summaries, audit traces, and applied plans.
"""

from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
import json
import os

from ..utils.logging import get_logger

logger = get_logger(__name__)


class PilotExporter:
    """Exports metadata for pilot/demo purposes."""
    
    def __init__(self, metadata_base: Optional[Path] = None):
        """Initialize exporter.
        
        Args:
            metadata_base: Base metadata directory (defaults to METADATA_BASE env var or /metadata)
        """
        if metadata_base is None:
            metadata_base = Path(os.getenv("METADATA_BASE", "/metadata"))
        
        self.metadata_base = metadata_base
        self.logger = get_logger(__name__)
    
    def export_all(self, output_dir: Path, collection: Optional[str] = None) -> Dict[str, Any]:
        """Export all metadata (lineage, GX summaries, audit traces, plans).
        
        Args:
            output_dir: Output directory for export
            collection: Optional collection filter
            
        Returns:
            Export summary dictionary
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        export_summary = {
            "export_timestamp": datetime.utcnow().isoformat() + "Z",
            "collection": collection,
            "exports": {}
        }
        
        # Export lineage
        lineage_data = self._export_lineage(collection)
        if lineage_data:
            lineage_file = output_dir / "lineage.json"
            with open(lineage_file, 'w') as f:
                json.dump(lineage_data, f, indent=2, default=str)
            export_summary["exports"]["lineage"] = str(lineage_file)
        
        # Export GX summaries
        gx_data = self._export_gx_summaries(collection)
        if gx_data:
            gx_file = output_dir / "gx_summaries.json"
            with open(gx_file, 'w') as f:
                json.dump(gx_data, f, indent=2, default=str)
            export_summary["exports"]["gx_summaries"] = str(gx_file)
        
        # Export audit traces
        audit_data = self._export_audit_traces(collection)
        if audit_data:
            audit_file = output_dir / "audit_traces.json"
            with open(audit_file, 'w') as f:
                json.dump(audit_data, f, indent=2, default=str)
            export_summary["exports"]["audit_traces"] = str(audit_file)
        
        # Export applied plans
        plans_data = self._export_applied_plans(collection)
        if plans_data:
            plans_file = output_dir / "applied_plans.json"
            with open(plans_file, 'w') as f:
                json.dump(plans_data, f, indent=2, default=str)
            export_summary["exports"]["applied_plans"] = str(plans_file)
        
        # Export rollback plans
        rollback_data = self._export_rollback_plans(collection)
        if rollback_data:
            rollback_file = output_dir / "rollback_plans.json"
            with open(rollback_file, 'w') as f:
                json.dump(rollback_data, f, indent=2, default=str)
            export_summary["exports"]["rollback_plans"] = str(rollback_file)
        
        # Write summary
        summary_file = output_dir / "export_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(export_summary, f, indent=2, default=str)
        
        self.logger.info(
            f"Export completed to {output_dir}",
            extra={
                'event_type': 'pilot_export_completed',
                'output_dir': str(output_dir),
                'collection': collection,
                'export_count': len(export_summary["exports"])
            }
        )
        
        return export_summary
    
    def _export_lineage(self, collection: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Export lineage graph data.
        
        Args:
            collection: Optional collection filter
            
        Returns:
            Lineage data dictionary
        """
        # Placeholder - in production, this would query Atlas or lineage store
        return {
            "nodes": [],
            "edges": [],
            "collection": collection,
            "export_timestamp": datetime.utcnow().isoformat() + "Z"
        }
    
    def _export_gx_summaries(self, collection: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Export GX quality summaries.
        
        Args:
            collection: Optional collection filter
            
        Returns:
            List of GX summary dictionaries
        """
        summaries = []
        quality_dir = self.metadata_base / "quality"
        
        if not quality_dir.exists():
            return None
        
        try:
            if collection:
                collection_dir = quality_dir / collection
                if collection_dir.exists():
                    for result_file in collection_dir.glob("*.json"):
                        with open(result_file, 'r') as f:
                            result = json.load(f)
                            summaries.append(result.get("summary", {}))
            else:
                for collection_dir in quality_dir.iterdir():
                    if collection_dir.is_dir():
                        for result_file in collection_dir.glob("*.json"):
                            with open(result_file, 'r') as f:
                                result = json.load(f)
                                summaries.append(result.get("summary", {}))
            
            return summaries if summaries else None
            
        except Exception as e:
            self.logger.error(
                f"Failed to export GX summaries: {e}",
                exc_info=True,
                extra={
                    'event_type': 'gx_export_error',
                    'collection': collection,
                    'error': str(e)
                }
            )
            return None
    
    def _export_audit_traces(self, collection: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Export audit traces.
        
        Args:
            collection: Optional collection filter (via job_id)
            
        Returns:
            List of audit trace dictionaries
        """
        try:
            from .audit import AuditTrail
            audit_trail = AuditTrail(self.metadata_base)
            
            # Get audit records (filtering by collection would require job_id mapping)
            records = audit_trail.get_audit_records(limit=1000)
            
            return records if records else None
            
        except Exception as e:
            self.logger.error(
                f"Failed to export audit traces: {e}",
                exc_info=True,
                extra={
                    'event_type': 'audit_export_error',
                    'collection': collection,
                    'error': str(e)
                }
            )
            return None
    
    def _export_applied_plans(self, collection: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Export applied transform plans.
        
        Args:
            collection: Optional collection filter
            
        Returns:
            List of applied plan dictionaries
        """
        try:
            from ..transform_plans.plan_manager import PlanManager
            plan_manager = PlanManager(self.metadata_base)
            
            if collection:
                plans = plan_manager.list_plans(collection)
                applied_plans = [p for p in plans if p.get("applied", False)]
                return applied_plans if applied_plans else None
            else:
                # Get all applied plans across all collections
                applied_plans = []
                plans_dir = self.metadata_base / "plans"
                
                if plans_dir.exists():
                    for collection_dir in plans_dir.iterdir():
                        if collection_dir.is_dir():
                            collection_name = collection_dir.name
                            plans = plan_manager.list_plans(collection_name)
                            applied = [p for p in plans if p.get("applied", False)]
                            applied_plans.extend(applied)
                
                return applied_plans if applied_plans else None
                
        except Exception as e:
            self.logger.error(
                f"Failed to export applied plans: {e}",
                exc_info=True,
                extra={
                    'event_type': 'plans_export_error',
                    'collection': collection,
                    'error': str(e)
                }
            )
            return None
    
    def _export_rollback_plans(self, collection: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """Export rollback plans.
        
        Args:
            collection: Optional collection filter
            
        Returns:
            List of rollback plan dictionaries
        """
        try:
            from ..transform_plans.plan_manager import PlanManager
            plan_manager = PlanManager(self.metadata_base)
            
            rollback_plans = []
            
            if collection:
                plans = plan_manager.list_plans(collection)
                for plan in plans:
                    rollback = plan.get("rollback_plan", [])
                    if rollback:
                        rollback_plans.append({
                            "plan_id": plan.get("plan_id"),
                            "collection": collection,
                            "version": plan.get("version"),
                            "rollback_plan": rollback
                        })
            else:
                plans_dir = self.metadata_base / "plans"
                if plans_dir.exists():
                    for collection_dir in plans_dir.iterdir():
                        if collection_dir.is_dir():
                            collection_name = collection_dir.name
                            plans = plan_manager.list_plans(collection_name)
                            for plan in plans:
                                rollback = plan.get("rollback_plan", [])
                                if rollback:
                                    rollback_plans.append({
                                        "plan_id": plan.get("plan_id"),
                                        "collection": collection_name,
                                        "version": plan.get("version"),
                                        "rollback_plan": rollback
                                    })
            
            return rollback_plans if rollback_plans else None
            
        except Exception as e:
            self.logger.error(
                f"Failed to export rollback plans: {e}",
                exc_info=True,
                extra={
                    'event_type': 'rollback_export_error',
                    'collection': collection,
                    'error': str(e)
                }
            )
            return None

