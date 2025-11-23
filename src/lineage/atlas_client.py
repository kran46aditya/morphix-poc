"""
Apache Atlas Client for Lineage Tracking

Pushes dataset metadata and lineage information to Apache Atlas.
"""

from typing import Dict, Any, Optional, List
import json
import os
from datetime import datetime

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from ..utils.logging import get_logger

logger = get_logger(__name__)


class AtlasClient:
    """Client for Apache Atlas integration."""
    
    def __init__(self, atlas_url: Optional[str] = None, 
                 username: Optional[str] = None,
                 password: Optional[str] = None):
        """Initialize Atlas client.
        
        Args:
            atlas_url: Apache Atlas REST API URL (defaults to ATLAS_URL env var)
            username: Atlas username (defaults to ATLAS_USERNAME env var)
            password: Atlas password (defaults to ATLAS_PASSWORD env var)
        """
        self.atlas_url = atlas_url or os.getenv("ATLAS_URL", "http://localhost:21000")
        self.username = username or os.getenv("ATLAS_USERNAME")
        self.password = password or os.getenv("ATLAS_PASSWORD")
        self.logger = get_logger(__name__)
        
        # Remove trailing slash
        if self.atlas_url.endswith('/'):
            self.atlas_url = self.atlas_url[:-1]
    
    def _make_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Make HTTP request to Atlas API.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            data: Request payload (optional)
            
        Returns:
            Response JSON or None if request failed
        """
        if not REQUESTS_AVAILABLE:
            self.logger.warning(
                "Requests library not available. Install with: pip install requests",
                extra={
                    'event_type': 'atlas_requests_not_available',
                    'endpoint': endpoint
                }
            )
            return None
        
        url = f"{self.atlas_url}/api/atlas/v2{endpoint}"
        auth = None
        
        if self.username and self.password:
            from requests.auth import HTTPBasicAuth
            auth = HTTPBasicAuth(self.username, self.password)
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, auth=auth, timeout=30)
            elif method.upper() == "POST":
                response = requests.post(url, json=data, auth=auth, timeout=30)
            elif method.upper() == "PUT":
                response = requests.put(url, json=data, auth=auth, timeout=30)
            else:
                self.logger.error(
                    f"Unsupported HTTP method: {method}",
                    extra={
                        'event_type': 'atlas_unsupported_method',
                        'method': method,
                        'endpoint': endpoint
                    }
                )
                return None
            
            response.raise_for_status()
            return response.json() if response.content else {}
            
        except requests.exceptions.RequestException as e:
            self.logger.error(
                f"Atlas API request failed: {e}",
                exc_info=True,
                extra={
                    'event_type': 'atlas_request_error',
                    'method': method,
                    'endpoint': endpoint,
                    'error': str(e)
                }
            )
            return None
    
    def push_dataset(self, entity_name: str, schema_url: str, 
                    additional_attributes: Optional[Dict[str, Any]] = None) -> bool:
        """Push dataset entity to Atlas.
        
        Args:
            entity_name: Name of the dataset entity
            schema_url: URL or path to schema definition
            additional_attributes: Additional entity attributes (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create dataset entity definition
            entity_def = {
                "typeName": "DataSet",
                "attributes": {
                    "qualifiedName": entity_name,
                    "name": entity_name,
                    "schemaUrl": schema_url,
                    "created": datetime.utcnow().isoformat() + "Z"
                }
            }
            
            # Add additional attributes
            if additional_attributes:
                entity_def["attributes"].update(additional_attributes)
            
            # Create entity
            result = self._make_request("POST", "/entity", data=entity_def)
            
            if result:
                self.logger.info(
                    f"Dataset entity pushed to Atlas: {entity_name}",
                    extra={
                        'event_type': 'atlas_dataset_pushed',
                        'entity_name': entity_name,
                        'schema_url': schema_url
                    }
                )
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(
                f"Failed to push dataset to Atlas: {e}",
                exc_info=True,
                extra={
                    'event_type': 'atlas_dataset_push_error',
                    'entity_name': entity_name,
                    'error': str(e)
                }
            )
            return False
    
    def push_lineage(self, source: str, transform: str, target: str,
                    transform_type: str = "ETL",
                    additional_attributes: Optional[Dict[str, Any]] = None) -> bool:
        """Push lineage relationship to Atlas.
        
        Args:
            source: Source dataset qualified name
            transform: Transform process name
            target: Target dataset qualified name
            transform_type: Type of transformation (default: "ETL")
            additional_attributes: Additional lineage attributes (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create process entity for the transformation
            process_entity = {
                "typeName": "Process",
                "attributes": {
                    "qualifiedName": transform,
                    "name": transform,
                    "typeName": transform_type,
                    "created": datetime.utcnow().isoformat() + "Z"
                }
            }
            
            # Add additional attributes
            if additional_attributes:
                process_entity["attributes"].update(additional_attributes)
            
            # Create process entity first
            process_result = self._make_request("POST", "/entity", data=process_entity)
            
            if not process_result:
                return False
            
            # Create lineage relationship
            lineage_def = {
                "typeName": "ProcessDataFlow",
                "attributes": {
                    "qualifiedName": f"{source}->{transform}->{target}",
                    "inputs": [{"qualifiedName": source}],
                    "outputs": [{"qualifiedName": target}],
                    "process": {"qualifiedName": transform}
                }
            }
            
            lineage_result = self._make_request("POST", "/relationship", data=lineage_def)
            
            if lineage_result:
                self.logger.info(
                    f"Lineage pushed to Atlas: {source} -> {transform} -> {target}",
                    extra={
                        'event_type': 'atlas_lineage_pushed',
                        'source': source,
                        'transform': transform,
                        'target': target
                    }
                )
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(
                f"Failed to push lineage to Atlas: {e}",
                exc_info=True,
                extra={
                    'event_type': 'atlas_lineage_push_error',
                    'source': source,
                    'transform': transform,
                    'target': target,
                    'error': str(e)
                }
            )
            return False
    
    def get_entity(self, qualified_name: str, entity_type: str = "DataSet") -> Optional[Dict]:
        """Get entity from Atlas by qualified name.
        
        Args:
            qualified_name: Entity qualified name
            entity_type: Entity type (default: "DataSet")
            
        Returns:
            Entity definition or None if not found
        """
        try:
            endpoint = f"/entity/uniqueAttribute/type/{entity_type}?attr:qualifiedName={qualified_name}"
            result = self._make_request("GET", endpoint)
            return result
        except Exception as e:
            self.logger.error(
                f"Failed to get entity from Atlas: {e}",
                exc_info=True,
                extra={
                    'event_type': 'atlas_get_entity_error',
                    'qualified_name': qualified_name,
                    'entity_type': entity_type,
                    'error': str(e)
                }
            )
            return None

