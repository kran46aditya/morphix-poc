"""
Centralized configuration management for Morphix ETL Platform.

Uses Pydantic Settings for validation and environment variable loading.
Loads from .env file if present, falls back to environment variables, then defaults.
"""
import os
from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """PostgreSQL database configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="DB_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    # Connection URL (preferred) or individual components
    url: Optional[str] = Field(
        default=None,
        description="Full PostgreSQL connection URL. Overrides individual fields if set."
    )
    
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    user: str = Field(default="user", description="PostgreSQL username")
    password: str = Field(default="pass", description="PostgreSQL password")
    database: str = Field(default="morphix", description="PostgreSQL database name")
    
    # Connection pool settings
    pool_size: int = Field(default=5, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")
    
    @property
    def connection_url(self) -> str:
        """Get the database connection URL."""
        if self.url:
            return self.url
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class MongoSettings(BaseSettings):
    """MongoDB default configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="MONGO_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    # Default connection (used when not provided via API)
    default_host: str = Field(default="localhost", description="Default MongoDB host")
    default_port: int = Field(default=27017, description="Default MongoDB port")
    default_database: str = Field(default="testdb", description="Default MongoDB database")
    
    # Connection settings
    connect_timeout: int = Field(default=10, description="Connection timeout in seconds")
    server_selection_timeout: int = Field(default=10, description="Server selection timeout in seconds")
    max_pool_size: int = Field(default=100, description="Max connection pool size")


class SparkSettings(BaseSettings):
    """Apache Spark configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="SPARK_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    app_name: str = Field(default="MorphixETL", description="Spark application name")
    master: str = Field(default="local[*]", description="Spark master URL")
    
    # Memory settings
    driver_memory: str = Field(default="2g", description="Spark driver memory")
    executor_memory: str = Field(default="2g", description="Spark executor memory")
    
    # Hudi settings
    hudi_jar_path: Optional[str] = Field(
        default=None,
        description="Path to Hudi JAR files (optional, uses packages if not set)"
    )
    
    # MongoDB connector
    mongo_connector_package: str = Field(
        default="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        description="MongoDB Spark connector package"
    )


class HudiSettings(BaseSettings):
    """Apache Hudi configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="HUDI_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    base_path: str = Field(
        default="/tmp/hudi",
        description="Base path for Hudi tables"
    )
    
    # Write settings
    table_type: str = Field(default="COPY_ON_WRITE", description="Default Hudi table type")
    operation_type: str = Field(default="UPSERT", description="Default write operation")
    
    # Performance settings
    hoodie_parquet_max_file_size: str = Field(
        default="134217728",
        description="Max file size for Hudi parquet files (128MB)"
    )
    hoodie_parquet_block_size: str = Field(
        default="134217728",
        description="Block size for Hudi parquet files (128MB)"
    )


class JobSettings(BaseSettings):
    """Job management configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="JOB_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    # Scheduler settings
    scheduler_enabled: bool = Field(default=True, description="Enable job scheduler")
    scheduler_check_interval: int = Field(
        default=60,
        description="Scheduler check interval in seconds"
    )
    
    # Execution settings
    default_batch_size: int = Field(default=10000, description="Default batch size for jobs")
    default_polling_interval: int = Field(
        default=60,
        description="Default polling interval for stream jobs (seconds)"
    )
    
    # Retry settings
    max_retries: int = Field(default=3, description="Maximum job retry attempts")
    retry_backoff_factor: float = Field(default=2.0, description="Retry backoff multiplier")


class AuthSettings(BaseSettings):
    """Authentication and security configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="AUTH_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    secret_key: str = Field(
        default="your-secret-key-change-in-production",
        description="JWT secret key (MUST be changed in production)"
    )
    
    algorithm: str = Field(default="HS256", description="JWT algorithm")
    access_token_expire_minutes: int = Field(
        default=30,
        description="Access token expiration time in minutes"
    )
    
    # Password settings
    password_min_length: int = Field(default=8, description="Minimum password length")
    password_require_special: bool = Field(
        default=True,
        description="Require special characters in password"
    )


class TrinoSettings(BaseSettings):
    """Trino server configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="TRINO_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8083, description="Trino coordinator port (mapped from container port 8080)")
    user: str = Field(default="admin", description="Trino user")
    catalog: str = Field(default="hive", description="Default catalog name")
    schema: str = Field(default="default", description="Default schema name")
    http_scheme: str = Field(default="http", description="HTTP scheme (http/https)")
    request_timeout: int = Field(default=300, description="Request timeout in seconds")
    
    # Authentication (optional)
    password: Optional[str] = Field(None, description="Trino password (if required)")
    ssl_cert_path: Optional[str] = Field(None, description="SSL certificate path")


class MinIOSettings(BaseSettings):
    """MinIO (S3-compatible) storage configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="MINIO_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    endpoint: str = Field(default="localhost:9000", description="MinIO endpoint (host:port)")
    access_key: str = Field(default="minioadmin", description="MinIO access key")
    secret_key: str = Field(default="minioadmin", description="MinIO secret key")
    secure: bool = Field(default=False, description="Use HTTPS (secure)")
    region: str = Field(default="us-east-1", description="MinIO region")
    
    @property
    def s3_endpoint(self) -> str:
        """Get S3-compatible endpoint URL."""
        protocol = "https" if self.secure else "http"
        return f"{protocol}://{self.endpoint}"


class APISettings(BaseSettings):
    """API server configuration."""
    
    model_config = SettingsConfigDict(
        env_prefix="API_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8"
    )
    
    host: str = Field(default="0.0.0.0", description="API server host")
    port: int = Field(default=8000, description="API server port")
    reload: bool = Field(default=False, description="Enable auto-reload in development")
    
    # CORS settings
    cors_origins: list[str] = Field(
        default=["*"],
        description="Allowed CORS origins"
    )
    cors_allow_credentials: bool = Field(default=True, description="Allow CORS credentials")
    
    # Rate limiting
    rate_limit_enabled: bool = Field(default=False, description="Enable rate limiting")
    rate_limit_requests: int = Field(default=100, description="Requests per window")
    rate_limit_window: int = Field(default=60, description="Rate limit window in seconds")


class Settings(BaseSettings):
    """Main application settings combining all sub-settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Environment
    environment: str = Field(default="development", description="Application environment")
    debug: bool = Field(default=False, description="Debug mode")
    
    # Sub-configurations
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    mongo: MongoSettings = Field(default_factory=MongoSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    hudi: HudiSettings = Field(default_factory=HudiSettings)
    job: JobSettings = Field(default_factory=JobSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    api: APISettings = Field(default_factory=APISettings)
    trino: TrinoSettings = Field(default_factory=TrinoSettings)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    
    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment value."""
        allowed = {"development", "staging", "production"}
        if v.lower() not in allowed:
            raise ValueError(f"Environment must be one of: {allowed}")
        return v.lower()
    
    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == "development"


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance (singleton pattern)."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """Reload settings from environment (useful for testing)."""
    global _settings
    _settings = Settings()
    return _settings

