from typing import Optional, Dict, Any, List, Union
from fastapi import FastAPI, HTTPException, UploadFile, File, Depends
from pydantic import BaseModel, Field
import json
import numpy as np
import pandas as pd
from datetime import datetime

from ..mongodb import connection as mongo_conn
from ..spark import mongo_reader as spark_reader
from ..postgres import credentials as pg_creds
from ..etl.data_transformer import DataTransformer
from ..etl.mongo_api_reader import MongoDataReader, create_reader_from_connection_info

# Import new modules
from ..auth import auth_router, get_current_active_user, UserResponse
from ..hudi_writer import HudiWriter, HudiTableManager, HudiTableConfig, HudiWriteConfig
from ..jobs import (
    JobManager, BatchJobProcessor, StreamJobProcessor, JobScheduler,
    BatchJobConfig, StreamJobConfig, JobStatus, JobType
)
from ..jobs.models import JobResult

app = FastAPI(title="Morphix ETL Platform", version="1.0.0")

# Include authentication router
app.include_router(auth_router)

# Include Trino router (optional)
try:
    from .trino_api import trino_router
    app.include_router(trino_router)
except ImportError as e:
    import logging
    logging.warning(f"Trino router not available (ImportError): {e}")
except Exception as e:
    import logging
    logging.warning(f"Trino router registration failed: {e}")
    # Continue without Trino endpoints

# Global scheduler instance (initialized on startup)
job_scheduler: Optional[JobScheduler] = None


@app.on_event("startup")
async def startup_event():
    """Initialize and start services on API startup."""
    global job_scheduler
    
    # Initialize and start job scheduler
    try:
        from config.settings import get_settings
        settings = get_settings()
        if settings.job.scheduler_enabled:
            job_scheduler = JobScheduler()
            job_scheduler.start_scheduler()
            app.state.job_scheduler = job_scheduler
    except Exception:
        pass  # Continue without scheduler


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup services on API shutdown."""
    global job_scheduler
    
    if job_scheduler:
        try:
            job_scheduler.stop_scheduler()
        except Exception:
            pass


def clean_for_json(obj: Any) -> Any:
    """Recursively convert pandas/numpy types to JSON-serializable Python types.
    
    Handles:
    - numpy.bool_ -> bool
    - numpy.int64/int32 -> int
    - numpy.float64/float32 -> float (with NaN/Inf handling)
    - pandas Timestamp -> ISO string
    - NaN/Inf/-Inf -> null
    - Nested lists and dicts
    """
    # Handle None first
    if obj is None:
        return None
    
    # Handle NaN and Inf values
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    
    try:
        if isinstance(obj, (float, np.floating)) and (np.isnan(obj) or np.isinf(obj)):
            return None
    except (TypeError, ValueError):
        pass
    
    # Handle numpy/pandas scalar types
    if isinstance(obj, np.generic):
        try:
            scalar_value = obj.item()
            return clean_for_json(scalar_value)
        except (ValueError, AttributeError):
            pass
    
    # Handle numpy bool
    if type(obj).__name__ == 'bool_' or isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    
    # Handle numpy integers
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8, np.uint64, np.uint32)):
        return int(obj)
    
    # Handle numpy floats
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        val = float(obj)
        if np.isnan(val) or np.isinf(val):
            return None
        return val
    
    # Handle pandas Timestamp/Timedelta
    if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    
    # Handle pandas nullable types (Int64, Float64, etc.)
    if hasattr(obj, 'item') and hasattr(obj, 'isna'):
        try:
            if pd.isna(obj):
                return None
            return clean_for_json(obj.item())
        except (ValueError, AttributeError, TypeError):
            pass
    
    # Handle dictionaries
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    
    # Handle lists and tuples
    if isinstance(obj, (list, tuple, pd.Series)):
        return [clean_for_json(item) for item in obj]
    
    # Handle pandas Index types
    if isinstance(obj, (pd.Index, pd.MultiIndex)):
        return [clean_for_json(item) for item in obj.tolist()]
    
    # Default: return as-is (should be JSON serializable already)
    return obj


class MongoRequest(BaseModel):
    # Optional user identifier to fetch saved credentials from Postgres
    user_id: Optional[str] = None
    # Either provide mongo_uri or provide the individual connection fields
    mongo_uri: Optional[str] = Field(
        None, description="Full MongoDB URI, e.g. mongodb://user:pass@host:port/db"
    )
    username: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    collection: Optional[str] = None
    # Query should be a dict representing the MongoDB filter; allow any JSON object
    query: Optional[Dict[str, Any]] = Field(default_factory=dict)
    # By default we use pymongo; set to True to use Spark instead
    use_pyspark: bool = False


def _build_conn_info_from_body(body: MongoRequest) -> Dict[str, Any]:
    """Build a normalized connection dict for downstream modules from request body."""
    if body.mongo_uri:
        if not body.database or not body.collection:
            raise HTTPException(
                status_code=400, detail={"error": "missing_database_or_collection"}
            )
        return {
            "mongo_uri": body.mongo_uri,
            "database": body.database,
            "collection": body.collection,
            "query": body.query or {},
        }

    # require username/password/host/port/database/collection when no URI provided
    missing = [
        k
        for k in ("username", "password", "host", "port", "database", "collection")
        if getattr(body, k) is None
    ]
    if missing:
        raise HTTPException(
            status_code=400,
            detail={"error": "missing_credentials_or_target", "missing": missing},
        )

    uri = f"mongodb://{body.username}:{body.password}@{body.host}:{body.port}/{body.database}?authSource=admin"
    return {
        "mongo_uri": uri,
        "database": body.database,
        "collection": body.collection,
        "query": body.query or {},
    }


@app.post("/mongo/credentials")
def save_credentials(
    payload: MongoRequest,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Save or update MongoDB credentials and target info for a user in Postgres.

    If `user_id` is not provided in the body, derive it from the authenticated user.
    """
    if not payload.user_id:
        # Use the authenticated user's id when not provided explicitly
        payload.user_id = str(current_user.id)

    # Normalize mongo_uri
    if payload.mongo_uri:
        final_uri = payload.mongo_uri
    else:
        missing = [
            k
            for k in ("username", "password", "host", "port", "database")
            if getattr(payload, k) is None
        ]
        if missing:
            raise HTTPException(
                status_code=400,
                detail={"error": "missing_fields", "missing": missing},
            )
        final_uri = f"mongodb://{payload.username}:{payload.password}@{payload.host}:{payload.port}/{payload.database}?authSource=admin"

    data = {
        "user_id": payload.user_id,
        "mongo_uri": final_uri,
        "username": payload.username,
        "password": payload.password,
        "host": payload.host,
        "port": payload.port,
        "database": payload.database,
        "collection": payload.collection,
        "query": payload.query or {},
    }

    try:
        pg_creds.ensure_table()
        pg_creds.save_credentials(payload.user_id, data)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail={"error": "db_error", "message": str(e)}
        )

    return {"status": "ok", "user_id": payload.user_id}


@app.post("/mongo/read")
def read_from_mongo(payload: MongoRequest):
    """Read data from MongoDB. Supports pymongo (default) or Spark."""
    conn_info = None

    if payload.user_id:
        try:
            pg_row = pg_creds.get_credentials(payload.user_id)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail={"error": "db_error", "message": str(e)}
            )
        if not pg_row:
            raise HTTPException(
                status_code=404, detail={"error": "credentials_not_found"}
            )

        # Prefer stored mongo_uri, else rebuild
        if pg_row.get("mongo_uri"):
            mongo_uri = pg_row["mongo_uri"]
        else:
            mongo_uri = f"mongodb://{pg_row['username']}:{pg_row['password']}@{pg_row['host']}:{pg_row['port']}/{pg_row['database']}?authSource=admin"

        conn_info = {
            "mongo_uri": mongo_uri,
            "database": pg_row["database"],
            "collection": pg_row["collection"],
            "query": pg_row.get("query") or {},
        }
    else:
        conn_info = _build_conn_info_from_body(payload)

    # Use pymongo
    if not payload.use_pyspark:
        try:
            docs = mongo_conn.read_with_pymongo(
                mongo_uri=conn_info["mongo_uri"],
                database=conn_info["database"],
                collection=conn_info["collection"],
                query=conn_info["query"],
                limit=100,
            )
            return {"status": "ok", "data": docs, "count": len(docs)}
        except Exception as e:
            import traceback
            error_details = {
                "error": "read_error",
                "message": str(e),
                "type": type(e).__name__
            }
            raise HTTPException(
                status_code=500, detail=error_details
            )

    # Use Spark
    try:
        spark_summary = spark_reader.read_with_spark(
            mongo_uri=conn_info["mongo_uri"],
            database=conn_info["database"],
            collection=conn_info["collection"],
            query=conn_info["query"],
            limit=100,
        )
        return {"status": "ok", "data": spark_summary.get("preview", [])}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail={"error": "read_error", "message": str(e)}
        )


class SchemaGenerationRequest(BaseModel):
    """Request model for schema generation from DataFrame."""
    user_id: Optional[str] = None
    mongo_uri: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    collection: Optional[str] = None
    query: Optional[Dict[str, Any]] = Field(default_factory=dict)
    sample_size: int = Field(default=1000, description="Number of documents to sample for schema generation")
    include_constraints: bool = Field(default=True, description="Include min/max constraints in schema")


@app.post("/schema/generate")
def generate_schema_from_collection(payload: SchemaGenerationRequest):
    """Generate schema by analyzing a MongoDB collection sample."""
    
    # Get connection info (reuse existing logic)
    conn_info = None
    
    if payload.user_id:
        try:
            pg_row = pg_creds.get_credentials(payload.user_id)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail={"error": "db_error", "message": str(e)}
            )
        if not pg_row:
            raise HTTPException(
                status_code=404, detail={"error": "credentials_not_found"}
            )

        # Build connection info from stored credentials
        if pg_row.get("mongo_uri"):
            mongo_uri = pg_row["mongo_uri"]
        else:
            mongo_uri = f"mongodb://{pg_row['username']}:{pg_row['password']}@{pg_row['host']}:{pg_row['port']}/{pg_row['database']}?authSource=admin"

        conn_info = {
            "mongo_uri": mongo_uri,
            "database": pg_row["database"],
            "collection": pg_row["collection"],
            "query": pg_row.get("query") or {},
        }
    else:
        # Build from request payload
        if payload.mongo_uri:
            if not payload.database or not payload.collection:
                raise HTTPException(
                    status_code=400, detail={"error": "missing_database_or_collection"}
                )
            conn_info = {
                "mongo_uri": payload.mongo_uri,
                "database": payload.database,
                "collection": payload.collection,
                "query": payload.query or {},
            }
        else:
            # require username/password/host/port/database/collection
            missing = [
                k for k in ("username", "password", "host", "port", "database", "collection")
                if getattr(payload, k) is None
            ]
            if missing:
                raise HTTPException(
                    status_code=400,
                    detail={"error": "missing_credentials_or_target", "missing": missing},
                )

            uri = f"mongodb://{payload.username}:{payload.password}@{payload.host}:{payload.port}/{payload.database}?authSource=admin"
            conn_info = {
                "mongo_uri": uri,
                "database": payload.database,
                "collection": payload.collection,
                "query": payload.query or {},
            }
    
    try:
        # Read data using Module 1
        reader = create_reader_from_connection_info(
            mongo_uri=conn_info["mongo_uri"],
            database=conn_info["database"],
            collection=conn_info["collection"]
        )
        df = reader.read_to_pandas(query=conn_info.get("query", {}), limit=payload.sample_size)
        
        if df.empty:
            raise HTTPException(
                status_code=400, detail={"error": "no_data_found", "message": "Collection is empty or query returned no results"}
            )
        
        # Generate schema using Module 2
        transformer = DataTransformer()
        schema = transformer.generate_schema_from_dataframe(
            df, 
            sample_size=payload.sample_size,
            include_constraints=payload.include_constraints
        )
        
        schema_summary = transformer.get_schema_summary()
        
        # Clean all data for JSON serialization
        clean_schema = clean_for_json(schema)
        clean_summary = clean_for_json(schema_summary)
        
        return {
            "status": "ok",
            "schema": clean_schema,
            "summary": clean_summary,
            "sample_info": {
                "rows_analyzed": len(df),
                "columns": len(df.columns),
                "collection": payload.collection or conn_info["collection"],
                "database": payload.database or conn_info["database"]
            }
        }
        
    except HTTPException:
        raise
    except UnicodeDecodeError as e:
        raise HTTPException(
            status_code=500, 
            detail={
                "error": "encoding_error", 
                "message": f"Unable to decode data from MongoDB. The collection may contain binary data that cannot be automatically converted. Error: {str(e)}",
                "suggestion": "Consider filtering out binary fields or using a different data source."
            }
        )
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        raise HTTPException(
            status_code=500, 
            detail={
                "error": "schema_generation_error", 
                "message": str(e),
                "type": type(e).__name__
            }
        )


@app.post("/schema/upload-avro")
async def upload_avro_schema(file: UploadFile = File(...)):
    """Upload and parse an Avro schema file."""
    
    if not file.filename.endswith(('.avsc', '.json')):
        raise HTTPException(
            status_code=400, 
            detail={"error": "invalid_file_type", "message": "File must be .avsc or .json"}
        )
    
    try:
        # Read file content
        content = await file.read()
        
        # Parse Avro schema
        transformer = DataTransformer()
        schema = transformer.load_schema_from_avro(content, replace_existing=True)
        
        schema_summary = transformer.get_schema_summary()
        
        return {
            "status": "ok",
            "schema": schema,
            "summary": schema_summary,
            "file_info": {
                "filename": file.filename,
                "size_bytes": len(content)
            }
        }
        
    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=400, 
            detail={"error": "invalid_json", "message": f"File contains invalid JSON: {str(e)}"}
        )
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_avro_schema", "message": str(e)}
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail={"error": "upload_error", "message": str(e)}
        )


@app.post("/schema/from-json")
def load_schema_from_json(schema_data: Dict[str, Any]):
    """Load schema from JSON data (either our format or Avro format)."""
    
    try:
        transformer = DataTransformer()
        
        # Try to detect if it's an Avro schema
        if schema_data.get('type') == 'record' and 'fields' in schema_data:
            # Looks like Avro schema
            schema = transformer.load_schema_from_avro(schema_data, replace_existing=True)
            schema_type = "avro"
        else:
            # Assume it's our schema format
            transformer.schema = schema_data
            transformer.schema_source = "json_loaded"
            schema = schema_data
            schema_type = "native"
        
        schema_summary = transformer.get_schema_summary()
        
        return {
            "status": "ok",
            "schema": schema,
            "summary": schema_summary,
            "schema_type": schema_type
        }
        
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_schema", "message": str(e)}
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "load_error", "message": str(e)}
        )


class TransformRequest(BaseModel):
    """Request model for data transformation with schema."""
    user_id: Optional[str] = None
    mongo_uri: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    collection: Optional[str] = None
    query: Optional[Dict[str, Any]] = Field(default_factory=dict)
    schema: Optional[Dict[str, Any]] = None
    limit: int = Field(default=100, description="Limit number of documents to transform")
    flatten_data: bool = Field(default=True, description="Whether to flatten nested structures")
    apply_schema: bool = Field(default=True, description="Whether to apply schema validation")
    strict_schema: bool = Field(default=False, description="Strict schema validation")


@app.post("/mongo/transform")
def transform_mongo_data(payload: TransformRequest):
    """Read MongoDB data and apply transformation with optional schema."""
    
    # Get connection info (reuse existing logic)
    conn_info = None
    
    if payload.user_id:
        try:
            pg_row = pg_creds.get_credentials(payload.user_id)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail={"error": "db_error", "message": str(e)}
            )
        if not pg_row:
            raise HTTPException(
                status_code=404, detail={"error": "credentials_not_found"}
            )

        # Build connection info from stored credentials
        if pg_row.get("mongo_uri"):
            mongo_uri = pg_row["mongo_uri"]
        else:
            mongo_uri = f"mongodb://{pg_row['username']}:{pg_row['password']}@{pg_row['host']}:{pg_row['port']}/{pg_row['database']}?authSource=admin"

        conn_info = {
            "mongo_uri": mongo_uri,
            "database": pg_row["database"],
            "collection": pg_row["collection"],
            "query": pg_row.get("query") or {},
        }
    else:
        # Build from request payload
        if payload.mongo_uri:
            if not payload.database or not payload.collection:
                raise HTTPException(
                    status_code=400, detail={"error": "missing_database_or_collection"}
                )
            conn_info = {
                "mongo_uri": payload.mongo_uri,
                "database": payload.database,
                "collection": payload.collection,
                "query": payload.query or {},
            }
        else:
            # require username/password/host/port/database/collection
            missing = [
                k for k in ("username", "password", "host", "port", "database", "collection")
                if getattr(payload, k) is None
            ]
            if missing:
                raise HTTPException(
                    status_code=400,
                    detail={"error": "missing_credentials_or_target", "missing": missing},
                )

            uri = f"mongodb://{payload.username}:{payload.password}@{payload.host}:{payload.port}/{payload.database}?authSource=admin"
            conn_info = {
                "mongo_uri": uri,
                "database": payload.database,
                "collection": payload.collection,
                "query": payload.query or {},
            }
    
    try:
        # Read data using Module 1
        reader = create_reader_from_connection_info(
            mongo_uri=conn_info["mongo_uri"],
            database=conn_info["database"],
            collection=conn_info["collection"]
        )
        df = reader.read_to_pandas(query=conn_info.get("query", {}), limit=payload.limit)
        
        if df.empty:
            return {"status": "ok", "data": [], "message": "No data found"}
        
        # Transform data using Module 2
        transformer = DataTransformer(schema=payload.schema)
        
        if payload.flatten_data:
            df = transformer.flatten_dataframe(df)
        
        if payload.apply_schema and transformer.schema:
            df = transformer.apply_schema(df, strict=payload.strict_schema)
        
        # Convert to records for JSON response and clean numpy types
        data = df.to_dict('records')
        data = clean_for_json(data)
        
        validation_errors = transformer.validation_errors if hasattr(transformer, 'validation_errors') else []
        validation_errors = clean_for_json(validation_errors)
        
        result = {
            "status": "ok",
            "data": data,
            "transform_info": {
                "rows_processed": len(data) if isinstance(data, list) else 0,
                "columns": len(df.columns) if data else 0,
                "flattened": payload.flatten_data,
                "schema_applied": payload.apply_schema and bool(transformer.schema),
                "validation_errors": validation_errors
            }
        }
        
        if transformer.schema:
            schema_summary = transformer.get_schema_summary()
            result["schema_summary"] = clean_for_json(schema_summary)
        
        return result
        
    except HTTPException:
        raise
    except UnicodeDecodeError as e:
        raise HTTPException(
            status_code=500, 
            detail={
                "error": "encoding_error", 
                "message": f"Unable to decode data from MongoDB. The collection may contain binary data that cannot be automatically converted. Error: {str(e)}",
                "suggestion": "Consider filtering out binary fields or using a different data source."
            }
        )
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500, 
            detail={
                "error": "transform_error", 
                "message": str(e),
                "type": type(e).__name__
            }
        )


# Hudi Management Endpoints

class HudiTableRequest(BaseModel):
    """Request model for Hudi table operations."""
    table_name: str = Field(..., description="Hudi table name")
    database: str = Field(default="default", description="Hudi database name")
    base_path: str = Field(..., description="Hudi table base path")
    schema: Dict[str, Any] = Field(..., description="Table schema")
    partition_field: Optional[str] = Field(None, description="Partition field")
    table_type: str = Field(default="COPY_ON_WRITE", description="Hudi table type")


@app.post("/hudi/table/create")
def create_hudi_table(
    request: HudiTableRequest,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Create a new Hudi table."""
    try:
        # Create table configuration
        table_config = HudiTableConfig(
            table_name=request.table_name,
            database=request.database,
            base_path=request.base_path,
            schema=request.schema,
            partition_field=request.partition_field
        )
        
        # Create table manager
        table_manager = HudiTableManager()
        
        # Create table
        success = table_manager.create_table(table_config)
        
        if success:
            return {
                "status": "ok",
                "message": f"Table {request.database}.{request.table_name} created successfully",
                "table_name": request.table_name,
                "database": request.database,
                "base_path": request.base_path
            }
        else:
            raise HTTPException(
                status_code=500,
                detail={"error": "table_creation_failed", "message": "Failed to create Hudi table"}
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "hudi_error", "message": str(e)}
        )
    finally:
        if 'table_manager' in locals():
            table_manager.close()


@app.get("/hudi/table/{table_name}")
def get_hudi_table_info(
    table_name: str,
    database: str = "default",
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Get Hudi table information."""
    try:
        table_manager = HudiTableManager()
        table_info = table_manager.get_table_info(table_name, database)
        
        if not table_info:
            raise HTTPException(
                status_code=404,
                detail={"error": "table_not_found", "message": f"Table {database}.{table_name} not found"}
            )
        
        return {
            "status": "ok",
            "table_info": table_info.dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "hudi_error", "message": str(e)}
        )
    finally:
        if 'table_manager' in locals():
            table_manager.close()


@app.get("/hudi/tables")
def list_hudi_tables(
    database: str = "default",
    current_user: UserResponse = Depends(get_current_active_user)
):
    """List all Hudi tables in a database."""
    try:
        table_manager = HudiTableManager()
        tables = table_manager.list_tables(database)
        
        return {
            "status": "ok",
            "tables": tables,
            "database": database
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "hudi_error", "message": str(e)}
        )
    finally:
        if 'table_manager' in locals():
            table_manager.close()


# Job Management Endpoints

class BatchJobRequest(BaseModel):
    """Request model for creating batch jobs."""
    job_name: str = Field(..., description="Job name")
    mongo_uri: str = Field(..., description="MongoDB connection URI")
    database: str = Field(..., description="MongoDB database name")
    collection: str = Field(..., description="MongoDB collection name")
    query: Dict[str, Any] = Field(default_factory=dict, description="MongoDB query filter")
    date_field: str = Field(..., description="Date field for batch processing")
    hudi_table_name: str = Field(..., description="Target Hudi table name")
    hudi_base_path: str = Field(..., description="Hudi table base path")
    schema: Optional[Dict[str, Any]] = Field(None, description="Data transformation schema")
    cron_expression: Optional[str] = Field(None, description="Cron expression for scheduling")
    batch_size: int = Field(default=10000, description="Batch size for processing")
    description: Optional[str] = Field(None, description="Job description")


@app.post("/jobs/batch/create")
def create_batch_job(
    request: BatchJobRequest,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Create a new batch job."""
    try:
        # Create job configuration
        job_config = BatchJobConfig(
            job_id="",  # Will be generated
            job_name=request.job_name,
            user_id=current_user.id,
            mongo_uri=request.mongo_uri,
            database=request.database,
            collection=request.collection,
            query=request.query,
            date_field=request.date_field,
            hudi_table_name=request.hudi_table_name,
            hudi_base_path=request.hudi_base_path,
            schema=request.schema,
            batch_size=request.batch_size,
            description=request.description,
            created_by=current_user.username
        )
        
        # Create job manager
        with JobManager() as job_manager:
            job_id = job_manager.create_job(job_config)
            
            return {
                "status": "ok",
                "message": "Batch job created successfully",
                "job_id": job_id,
                "job_name": request.job_name
            }
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_creation_failed", "message": str(e)}
        )


class StreamJobRequest(BaseModel):
    """Request model for creating stream jobs."""
    job_name: str = Field(..., description="Job name")
    mongo_uri: str = Field(..., description="MongoDB connection URI")
    database: str = Field(..., description="MongoDB database name")
    collection: str = Field(..., description="MongoDB collection name")
    query: Dict[str, Any] = Field(default_factory=dict, description="MongoDB query filter")
    hudi_table_name: str = Field(..., description="Target Hudi table name")
    hudi_base_path: str = Field(..., description="Hudi table base path")
    schema: Optional[Dict[str, Any]] = Field(None, description="Data transformation schema")
    polling_interval_seconds: int = Field(default=60, description="Polling interval in seconds")
    batch_size: int = Field(default=1000, description="Batch size for stream processing")
    description: Optional[str] = Field(None, description="Job description")


@app.post("/jobs/stream/create")
def create_stream_job(
    request: StreamJobRequest,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Create a new stream job."""
    try:
        # Create job configuration
        job_config = StreamJobConfig(
            job_id="",  # Will be generated
            job_name=request.job_name,
            user_id=current_user.id,
            mongo_uri=request.mongo_uri,
            database=request.database,
            collection=request.collection,
            query=request.query,
            hudi_table_name=request.hudi_table_name,
            hudi_base_path=request.hudi_base_path,
            schema=request.schema,
            polling_interval_seconds=request.polling_interval_seconds,
            batch_size=request.batch_size,
            description=request.description,
            created_by=current_user.username
        )
        
        # Create job manager
        with JobManager() as job_manager:
            job_id = job_manager.create_job(job_config)
            
            return {
                "status": "ok",
                "message": "Stream job created successfully",
                "job_id": job_id,
                "job_name": request.job_name
            }
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_creation_failed", "message": str(e)}
        )


@app.get("/jobs")
def list_jobs(
    current_user: UserResponse = Depends(get_current_active_user)
):
    """List all jobs for the current user."""
    try:
        with JobManager() as job_manager:
            jobs = job_manager.list_jobs(user_id=current_user.id)
            
            job_list = []
            for job in jobs:
                job_list.append({
                    "job_id": job.job_id,
                    "job_name": job.job_name,
                    "job_type": job.job_type,
                    "enabled": job.enabled,
                    "created_at": job.created_at,
                    "last_run": job.last_run,
                    "description": job.description
                })
            
            return {
                "status": "ok",
                "jobs": job_list
            }
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_listing_failed", "message": str(e)}
        )


@app.post("/jobs/{job_id}/start")
def start_job(
    job_id: str,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Start a job execution."""
    try:
        with JobManager() as job_manager:
            # Check if user owns the job
            job = job_manager.get_job(job_id)
            if not job or job.user_id != current_user.id:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "job_not_found", "message": "Job not found or access denied"}
                )
            
            # Start job
            execution_id = job_manager.start_job(job_id, "manual")
            
            if execution_id:
                return {
                    "status": "ok",
                    "message": "Job started successfully",
                    "execution_id": execution_id
                }
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"error": "job_start_failed", "message": "Failed to start job"}
                )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_start_failed", "message": str(e)}
        )


@app.get("/jobs/{job_id}/status")
def get_job_status(
    job_id: str,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Get job status and execution history."""
    try:
        with JobManager() as job_manager:
            # Check if user owns the job
            job = job_manager.get_job(job_id)
            if not job or job.user_id != current_user.id:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "job_not_found", "message": "Job not found or access denied"}
                )
            
            # Get job executions
            executions = job_manager.get_job_executions(job_id, limit=10)
            
            # Get job metrics
            metrics = job_manager.get_job_metrics(job_id)
            
            return {
                "status": "ok",
                "job": {
                    "job_id": job.job_id,
                    "job_name": job.job_name,
                    "job_type": job.job_type,
                    "enabled": job.enabled,
                    "created_at": job.created_at,
                    "last_run": job.last_run
                },
                "executions": [execution.dict() for execution in executions],
                "metrics": metrics.dict() if metrics else None
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_status_failed", "message": str(e)}
        )


class JobUpdateRequest(BaseModel):
    """Request model for updating jobs (supports both batch and stream)."""
    job_name: Optional[str] = Field(None, description="Job name")
    mongo_uri: Optional[str] = Field(None, description="MongoDB connection URI")
    database: Optional[str] = Field(None, description="MongoDB database name")
    collection: Optional[str] = Field(None, description="MongoDB collection name")
    query: Optional[Dict[str, Any]] = Field(default_factory=dict, description="MongoDB query filter")
    hudi_table_name: Optional[str] = Field(None, description="Target Hudi table name")
    hudi_base_path: Optional[str] = Field(None, description="Hudi table base path")
    schema: Optional[Dict[str, Any]] = Field(None, description="Data transformation schema")
    description: Optional[str] = Field(None, description="Job description")
    # Batch-specific
    date_field: Optional[str] = Field(None, description="Date field for batch processing")
    batch_size: Optional[int] = Field(None, description="Batch size for processing")
    # Stream-specific
    polling_interval_seconds: Optional[int] = Field(None, description="Polling interval for stream jobs")


@app.put("/jobs/{job_id}")
def update_job(
    job_id: str,
    request: JobUpdateRequest,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Update an existing job. Only provided fields will be updated."""
    try:
        with JobManager() as job_manager:
            # Check if user owns the job
            existing_job = job_manager.get_job(job_id)
            if not existing_job or existing_job.user_id != current_user.id:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "job_not_found", "message": "Job not found or access denied"}
                )
            
            # Get current job config - existing_job is already a JobConfig object
            if existing_job.job_type == JobType.BATCH:
                # Update batch job config - merge existing values with new ones
                if isinstance(existing_job, BatchJobConfig):
                    job_config = BatchJobConfig(
                        job_id=job_id,
                        job_name=request.job_name if request.job_name else existing_job.job_name,
                        user_id=current_user.id,
                        mongo_uri=request.mongo_uri if request.mongo_uri else existing_job.mongo_uri,
                        database=request.database if request.database else existing_job.database,
                        collection=request.collection if request.collection else existing_job.collection,
                        query=request.query if request.query else existing_job.query,
                        date_field=request.date_field if request.date_field else existing_job.date_field,
                        hudi_table_name=request.hudi_table_name if request.hudi_table_name else existing_job.hudi_table_name,
                        hudi_base_path=request.hudi_base_path if request.hudi_base_path else existing_job.hudi_base_path,
                        schema=request.schema if request.schema else existing_job.schema,
                        batch_size=request.batch_size if request.batch_size else existing_job.batch_size,
                        description=request.description if request.description else existing_job.description,
                        created_by=existing_job.created_by,
                        enabled=existing_job.enabled,
                        schedule=existing_job.schedule  # Preserve schedule
                    )
                else:
                    raise HTTPException(status_code=400, detail={"error": "invalid_job_type", "message": "Job is not a batch job"})
            else:
                # Update stream job config
                if isinstance(existing_job, StreamJobConfig):
                    job_config = StreamJobConfig(
                        job_id=job_id,
                        job_name=request.job_name if request.job_name else existing_job.job_name,
                        user_id=current_user.id,
                        mongo_uri=request.mongo_uri if request.mongo_uri else existing_job.mongo_uri,
                        database=request.database if request.database else existing_job.database,
                        collection=request.collection if request.collection else existing_job.collection,
                        query=request.query if request.query else existing_job.query,
                        hudi_table_name=request.hudi_table_name if request.hudi_table_name else existing_job.hudi_table_name,
                        hudi_base_path=request.hudi_base_path if request.hudi_base_path else existing_job.hudi_base_path,
                        schema=request.schema if request.schema else existing_job.schema,
                        polling_interval_seconds=request.polling_interval_seconds if request.polling_interval_seconds else existing_job.polling_interval_seconds,
                        batch_size=request.batch_size if request.batch_size else existing_job.batch_size,
                        description=request.description if request.description else existing_job.description,
                        created_by=existing_job.created_by,
                        enabled=existing_job.enabled,
                        schedule=existing_job.schedule  # Preserve schedule
                    )
                else:
                    raise HTTPException(status_code=400, detail={"error": "invalid_job_type", "message": "Job is not a stream job"})
            
            success = job_manager.update_job(job_id, job_config)
            
            if success:
                return {
                    "status": "ok",
                    "message": "Job updated successfully",
                    "job_id": job_id
                }
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"error": "job_update_failed", "message": "Failed to update job"}
                )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_update_failed", "message": str(e)}
        )


@app.delete("/jobs/{job_id}")
def delete_job(
    job_id: str,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Delete a job."""
    try:
        with JobManager() as job_manager:
            # Check if user owns the job
            job = job_manager.get_job(job_id)
            if not job or job.user_id != current_user.id:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "job_not_found", "message": "Job not found or access denied"}
                )
            
            # Stop job if running (for stream jobs)
            if hasattr(app.state, 'job_scheduler') and app.state.job_scheduler:
                # Unschedule if scheduled
                app.state.job_scheduler.unschedule_job(job_id)
            
            success = job_manager.delete_job(job_id)
            
            if success:
                return {
                    "status": "ok",
                    "message": "Job deleted successfully",
                    "job_id": job_id
                }
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"error": "job_deletion_failed", "message": "Failed to delete job"}
                )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_deletion_failed", "message": str(e)}
        )


@app.post("/jobs/{job_id}/stop")
def stop_job(
    job_id: str,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Stop a running job."""
    try:
        with JobManager() as job_manager:
            # Check if user owns the job
            job = job_manager.get_job(job_id)
            if not job or job.user_id != current_user.id:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "job_not_found", "message": "Job not found or access denied"}
                )
            
            # Get latest execution
            executions = job_manager.get_job_executions(job_id, limit=1)
            if not executions or executions[0].status != JobStatus.RUNNING:
                raise HTTPException(
                    status_code=400,
                    detail={"error": "job_not_running", "message": "Job is not currently running"}
                )
            
            execution = executions[0]
            execution_id = execution.execution_id
            
            # Stop based on job type
            if job.job_type == JobType.STREAM:
                # Stop stream job
                if hasattr(app.state, 'job_scheduler') and app.state.job_scheduler:
                    stream_processor = app.state.job_scheduler.stream_processor
                    stopped = stream_processor.stop_stream_job(execution_id)
                    if stopped:
                        from ..jobs.models import JobStatus as JS
                        job_manager.complete_job(
                            execution_id,
                            JobResult(
                                job_id=job_id,
                                execution_id=execution_id,
                                status=JS.CANCELLED,
                                started_at=execution.started_at,
                                completed_at=datetime.utcnow()
                            )
                        )
                        return {
                            "status": "ok",
                            "message": "Job stopped successfully",
                            "execution_id": execution_id
                        }
            
            # For batch jobs or if stream stop failed
            from ..jobs.models import JobStatus as JS
            job_manager.complete_job(
                execution_id,
                JobResult(
                    job_id=job_id,
                    execution_id=execution_id,
                    status=JS.CANCELLED,
                    started_at=execution.started_at,
                    completed_at=datetime.utcnow()
                )
            )
            
            return {
                "status": "ok",
                "message": "Job stopped successfully",
                "execution_id": execution_id
            }
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_stop_failed", "message": str(e)}
        )


@app.put("/jobs/{job_id}/enable")
def enable_job(
    job_id: str,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Enable a job."""
    try:
        with JobManager() as job_manager:
            # Check if user owns the job
            job = job_manager.get_job(job_id)
            if not job or job.user_id != current_user.id:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "job_not_found", "message": "Job not found or access denied"}
                )
            
            success = job_manager.enable_job(job_id)
            
            if success:
                return {
                    "status": "ok",
                    "message": "Job enabled successfully",
                    "job_id": job_id
                }
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"error": "job_enable_failed", "message": "Failed to enable job"}
                )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_enable_failed", "message": str(e)}
        )


@app.put("/jobs/{job_id}/disable")
def disable_job(
    job_id: str,
    current_user: UserResponse = Depends(get_current_active_user)
):
    """Disable a job."""
    try:
        with JobManager() as job_manager:
            # Check if user owns the job
            job = job_manager.get_job(job_id)
            if not job or job.user_id != current_user.id:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "job_not_found", "message": "Job not found or access denied"}
                )
            
            success = job_manager.disable_job(job_id)
            
            if success:
                return {
                    "status": "ok",
                    "message": "Job disabled successfully",
                    "job_id": job_id
                }
            else:
                raise HTTPException(
                    status_code=500,
                    detail={"error": "job_disable_failed", "message": "Failed to disable job"}
                )
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "job_disable_failed", "message": str(e)}
        )


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "morphix-etl-platform",
        "version": "1.0.0",
        "modules": ["auth", "etl", "hudi", "jobs"]
    }
