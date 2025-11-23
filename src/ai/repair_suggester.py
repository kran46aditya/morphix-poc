"""
AI Repair Suggester

Suggests data repair strategies based on schema diffs and sample documents.
Human-gated: no external AI calls without explicit ENV key.
"""

from typing import Dict, Any, List, Optional
import os
import pandas as pd

from ..utils.logging import get_logger

logger = get_logger(__name__)


class RepairSuggester:
    """Suggests data repair strategies for schema changes."""
    
    def __init__(self, ai_mode: Optional[str] = None):
        """Initialize repair suggester.
        
        Args:
            ai_mode: AI mode - "local" (rule-based), "remote" (external API), or None (disabled)
                     Defaults to AIMODE environment variable or "local"
        """
        self.ai_mode = ai_mode or os.getenv("AIMODE", "local")
        self.logger = get_logger(__name__)
        
        if self.ai_mode not in ["local", "remote"]:
            self.logger.warning(
                f"Invalid AIMODE: {self.ai_mode}. Defaulting to 'local'",
                extra={
                    'event_type': 'ai_mode_invalid',
                    'ai_mode': self.ai_mode
                }
            )
            self.ai_mode = "local"
    
    def suggest_repairs(self, schema_diff: List[Dict[str, Any]], 
                       sample_docs: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
        """Suggest repairs based on schema diff and sample documents.
        
        Args:
            schema_diff: List of schema difference entries from diff_schemas()
            sample_docs: Optional sample documents for analysis
            
        Returns:
            List of repair suggestions with field, old_type, new_type, suggestion, rationale
        """
        suggestions = []
        
        for diff in schema_diff:
            action = diff.get("action")
            field = diff.get("field")
            old_type = diff.get("old_type")
            new_type = diff.get("new_type")
            
            if action == "type_change":
                suggestion = self._suggest_type_change_repair(field, old_type, new_type, sample_docs)
                if suggestion:
                    suggestions.append(suggestion)
            elif action == "add":
                suggestion = self._suggest_add_field_repair(field, new_type, sample_docs)
                if suggestion:
                    suggestions.append(suggestion)
            elif action == "remove":
                suggestion = self._suggest_remove_field_repair(field, old_type)
                if suggestion:
                    suggestions.append(suggestion)
        
        self.logger.info(
            f"Generated {len(suggestions)} repair suggestions",
            extra={
                'event_type': 'repair_suggestions_generated',
                'ai_mode': self.ai_mode,
                'suggestion_count': len(suggestions)
            }
        )
        
        return suggestions
    
    def _suggest_type_change_repair(self, field: str, old_type: str, new_type: str,
                                    sample_docs: Optional[List[Dict]] = None) -> Optional[Dict[str, Any]]:
        """Suggest repair for type change.
        
        Args:
            field: Field name
            old_type: Old type
            new_type: New type
            sample_docs: Sample documents (optional)
            
        Returns:
            Repair suggestion dictionary or None
        """
        # Rule-based suggestions (local mode)
        if old_type == "string" and new_type == "integer":
            return {
                "field": field,
                "old_type": old_type,
                "new_type": new_type,
                "suggestion": f"Convert string to integer using pd.to_numeric({field}, errors='coerce')",
                "rationale": "String field changed to integer. Use numeric conversion with coercion for invalid values."
            }
        elif old_type == "string" and new_type == "float":
            return {
                "field": field,
                "old_type": old_type,
                "new_type": new_type,
                "suggestion": f"Convert string to float using pd.to_numeric({field}, errors='coerce')",
                "rationale": "String field changed to float. Use numeric conversion with coercion."
            }
        elif old_type == "integer" and new_type == "float":
            return {
                "field": field,
                "old_type": old_type,
                "new_type": new_type,
                "suggestion": f"Convert integer to float using {field}.astype('float64')",
                "rationale": "Integer field changed to float. Direct type conversion is safe."
            }
        elif old_type == "integer" and new_type == "string":
            return {
                "field": field,
                "old_type": old_type,
                "new_type": new_type,
                "suggestion": f"Convert integer to string using {field}.astype(str)",
                "rationale": "Integer field changed to string. Direct type conversion."
            }
        elif old_type == "float" and new_type == "string":
            return {
                "field": field,
                "old_type": old_type,
                "new_type": new_type,
                "suggestion": f"Convert float to string using {field}.astype(str)",
                "rationale": "Float field changed to string. Direct type conversion."
            }
        else:
            return {
                "field": field,
                "old_type": old_type,
                "new_type": new_type,
                "suggestion": f"Manual review required for type change from {old_type} to {new_type}",
                "rationale": f"Complex type change from {old_type} to {new_type} requires manual intervention."
            }
    
    def _suggest_add_field_repair(self, field: str, new_type: str,
                                 sample_docs: Optional[List[Dict]] = None) -> Optional[Dict[str, Any]]:
        """Suggest repair for added field.
        
        Args:
            field: Field name
            new_type: New field type
            sample_docs: Sample documents (optional)
            
        Returns:
            Repair suggestion dictionary or None
        """
        return {
            "field": field,
            "old_type": None,
            "new_type": new_type,
            "suggestion": f"Add new field '{field}' with type {new_type} and default value based on type",
            "rationale": f"New field '{field}' added. Set appropriate default value for type {new_type}."
        }
    
    def _suggest_remove_field_repair(self, field: str, old_type: str) -> Optional[Dict[str, Any]]:
        """Suggest repair for removed field.
        
        Args:
            field: Field name
            old_type: Old field type
            
        Returns:
            Repair suggestion dictionary or None
        """
        return {
            "field": field,
            "old_type": old_type,
            "new_type": None,
            "suggestion": f"Remove field '{field}' from data or archive it",
            "rationale": f"Field '{field}' was removed from schema. Consider archiving or removing from data."
        }
    
    def _call_external_ai(self, prompt: str) -> Optional[str]:
        """Call external AI API (only if AIMODE=remote and API key is set).
        
        Args:
            prompt: Prompt for AI
            
        Returns:
            AI response or None
        """
        if self.ai_mode != "remote":
            return None
        
        api_key = os.getenv("AI_API_KEY")
        if not api_key:
            self.logger.warning(
                "AI_API_KEY not set. Cannot call external AI.",
                extra={
                    'event_type': 'ai_api_key_missing'
                }
            )
            return None
        
        # Placeholder for external AI call
        # In production, this would call OpenAI, Anthropic, etc.
        self.logger.info(
            "External AI call would be made here (not implemented in PoC)",
            extra={
                'event_type': 'ai_external_call_placeholder',
                'prompt_length': len(prompt)
            }
        )
        
        return None

