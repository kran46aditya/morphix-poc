"""
Schema version registry with PostgreSQL backend.

Tracks schema evolution over time.
"""

from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, Text, Boolean, UniqueConstraint, Index
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, List, Any
from datetime import datetime
import logging
import json

from .schema_evaluator import SchemaChange

logger = logging.getLogger(__name__)

Base = declarative_base()


class SchemaVersion(Base):
    """Schema version model."""
    __tablename__ = "schema_versions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(255), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    schema = Column(JSON, nullable=False)
    changes = Column(JSON)  # What changed from previous version
    change_type = Column(String(50))  # SAFE/WARNING/BREAKING
    applied_at = Column(DateTime, default=datetime.utcnow)
    applied_by = Column(String(255))
    rollback_sql = Column(Text)  # SQL to undo changes
    
    __table_args__ = (
        UniqueConstraint('table_name', 'version', name='uq_schema_versions_table_version'),
        Index('idx_schema_versions_latest', 'table_name', 'version'),
    )


class SchemaRegistry:
    """
    Centralized schema version management.
    
    Features:
    - Store all schema versions
    - Get schema by version or latest
    - Track schema lineage
    - Generate rollback scripts
    
    Example:
        registry = SchemaRegistry(database_url)
        
        # Register new version
        registry.register_version(
            table_name="users",
            schema=new_schema,
            changes=[SchemaChange(...)],
            applied_by="cdc_worker"
        )
        
        # Get latest schema
        latest = registry.get_latest_schema("users")
        
        # Get specific version
        v2 = registry.get_schema("users", version=2)
    """
    
    def __init__(self, database_url: str):
        """
        Initialize registry.
        
        Args:
            database_url: PostgreSQL connection URL
            
        Raises:
            Exception: If database connection fails
        """
        try:
            # Create SQLAlchemy engine
            self.engine = create_engine(
                database_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # Create session factory
            self.SessionLocal = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False
            )
            
            # Create tables if not exist
            Base.metadata.create_all(self.engine)
            
            logger.info("SchemaRegistry initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize SchemaRegistry: {e}")
            raise
    
    def register_version(
        self,
        table_name: str,
        schema: Dict[str, Any],
        changes: List[SchemaChange],
        applied_by: str = "system",
        rollback_sql: Optional[str] = None
    ) -> int:
        """
        Register new schema version.
        
        Args:
            table_name: Table name
            schema: New schema dictionary
            changes: List of schema changes
            applied_by: Who applied the changes
            rollback_sql: Optional SQL to rollback changes
            
        Returns:
            New version number
        """
        session: Optional[Session] = None
        try:
            session = self.SessionLocal()
            
            # Get current max version
            max_version = session.query(SchemaVersion.version).filter_by(
                table_name=table_name
            ).order_by(SchemaVersion.version.desc()).first()
            
            new_version = (max_version[0] + 1) if max_version else 1
            
            # Determine change type
            change_types = [c.change_type.value for c in changes]
            if any('breaking' in ct for ct in change_types):
                change_type = 'BREAKING'
            elif any('warning' in ct for ct in change_types):
                change_type = 'WARNING'
            else:
                change_type = 'SAFE'
            
            # Serialize changes
            changes_json = [
                {
                    'field_name': c.field_name,
                    'change_type': c.change_type.value,
                    'old_type': c.old_type,
                    'new_type': c.new_type,
                    'description': c.description
                }
                for c in changes
            ]
            
            # Create new version record
            version_record = SchemaVersion(
                table_name=table_name,
                version=new_version,
                schema=schema,
                changes=changes_json,
                change_type=change_type,
                applied_by=applied_by,
                rollback_sql=rollback_sql
            )
            
            session.add(version_record)
            session.commit()
            
            logger.info(
                f"Registered schema version {new_version} for table {table_name}",
                extra={
                    "table_name": table_name,
                    "version": new_version,
                    "change_type": change_type,
                    "changes_count": len(changes)
                }
            )
            
            return new_version
            
        except SQLAlchemyError as e:
            if session:
                session.rollback()
            logger.error(f"Database error registering schema version: {e}")
            raise
        except Exception as e:
            if session:
                session.rollback()
            logger.error(f"Unexpected error registering schema version: {e}")
            raise
        finally:
            if session:
                session.close()
    
    def get_latest_schema(
        self,
        table_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get latest schema version.
        
        Args:
            table_name: Table name
            
        Returns:
            Latest schema dictionary or None if not found
        """
        session: Optional[Session] = None
        try:
            session = self.SessionLocal()
            
            version_record = session.query(SchemaVersion).filter_by(
                table_name=table_name
            ).order_by(SchemaVersion.version.desc()).first()
            
            if version_record:
                return version_record.schema
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"Database error getting latest schema: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting latest schema: {e}")
            return None
        finally:
            if session:
                session.close()
    
    def get_schema(
        self,
        table_name: str,
        version: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get specific schema version.
        
        Args:
            table_name: Table name
            version: Version number
            
        Returns:
            Schema dictionary or None if not found
        """
        session: Optional[Session] = None
        try:
            session = self.SessionLocal()
            
            version_record = session.query(SchemaVersion).filter_by(
                table_name=table_name,
                version=version
            ).first()
            
            if version_record:
                return version_record.schema
            return None
            
        except SQLAlchemyError as e:
            logger.error(f"Database error getting schema version: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting schema version: {e}")
            return None
        finally:
            if session:
                session.close()
    
    def get_version_history(
        self,
        table_name: str
    ) -> List[SchemaVersion]:
        """
        Get all versions for table.
        
        Args:
            table_name: Table name
            
        Returns:
            List of SchemaVersion records
        """
        session: Optional[Session] = None
        try:
            session = self.SessionLocal()
            
            versions = session.query(SchemaVersion).filter_by(
                table_name=table_name
            ).order_by(SchemaVersion.version.asc()).all()
            
            return versions
            
        except SQLAlchemyError as e:
            logger.error(f"Database error getting version history: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting version history: {e}")
            return []
        finally:
            if session:
                session.close()
    
    def get_latest_version_number(
        self,
        table_name: str
    ) -> int:
        """
        Get latest version number for table.
        
        Args:
            table_name: Table name
            
        Returns:
            Latest version number (0 if no versions exist)
        """
        session: Optional[Session] = None
        try:
            session = self.SessionLocal()
            
            max_version = session.query(SchemaVersion.version).filter_by(
                table_name=table_name
            ).order_by(SchemaVersion.version.desc()).first()
            
            return max_version[0] if max_version else 0
            
        except SQLAlchemyError as e:
            logger.error(f"Database error getting latest version number: {e}")
            return 0
        except Exception as e:
            logger.error(f"Unexpected error getting latest version number: {e}")
            return 0
        finally:
            if session:
                session.close()
    
    def close(self) -> None:
        """Close database connections."""
        if hasattr(self, 'engine'):
            self.engine.dispose()
            logger.info("SchemaRegistry connections closed")

