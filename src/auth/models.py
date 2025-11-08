"""
Authentication models and schemas for the ETL platform.
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, EmailStr, Field
from enum import Enum


class UserRole(str, Enum):
    """User roles in the system."""
    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"


class UserStatus(str, Enum):
    """User account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"


class UserBase(BaseModel):
    """Base user model with common fields."""
    username: str = Field(..., min_length=3, max_length=50, description="Unique username")
    email: EmailStr = Field(..., description="User email address")
    full_name: str = Field(..., min_length=1, max_length=100, description="Full name")
    role: UserRole = Field(default=UserRole.USER, description="User role")
    status: UserStatus = Field(default=UserStatus.ACTIVE, description="Account status")


class UserCreate(UserBase):
    """Model for creating a new user."""
    password: str = Field(..., min_length=8, description="Password (min 8 characters)")


class UserLogin(BaseModel):
    """Model for user login."""
    username: str = Field(..., description="Username or email")
    password: str = Field(..., description="Password")


class UserUpdate(BaseModel):
    """Model for updating user information."""
    full_name: Optional[str] = Field(None, min_length=1, max_length=100)
    email: Optional[EmailStr] = None
    role: Optional[UserRole] = None
    status: Optional[UserStatus] = None


class PasswordChange(BaseModel):
    """Model for password change."""
    current_password: str = Field(..., description="Current password")
    new_password: str = Field(..., min_length=8, description="New password")


class User(UserBase):
    """Complete user model with database fields."""
    id: int = Field(..., description="User ID")
    created_at: datetime = Field(..., description="Account creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    last_login: Optional[datetime] = Field(None, description="Last login timestamp")
    is_active: bool = Field(default=True, description="Account active status")
    
    class Config:
        from_attributes = True


class UserResponse(UserBase):
    """User response model (excludes sensitive data)."""
    id: int = Field(..., description="User ID")
    created_at: datetime = Field(..., description="Account creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    last_login: Optional[datetime] = Field(None, description="Last login timestamp")
    is_active: bool = Field(default=True, description="Account active status")
    
    class Config:
        from_attributes = True


class Token(BaseModel):
    """Authentication token model."""
    access_token: str = Field(..., description="JWT access token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Token expiration time in seconds")
    user: UserResponse = Field(..., description="User information")


class TokenData(BaseModel):
    """Token payload data."""
    user_id: int = Field(..., description="User ID")
    username: str = Field(..., description="Username")
    role: UserRole = Field(..., description="User role")
    exp: datetime = Field(..., description="Token expiration")


class UserPermissions(BaseModel):
    """User permissions model."""
    can_create_jobs: bool = Field(default=True, description="Can create ETL jobs")
    can_manage_users: bool = Field(default=False, description="Can manage other users")
    can_access_admin: bool = Field(default=False, description="Can access admin features")
    can_manage_schemas: bool = Field(default=True, description="Can manage schemas")
    can_view_logs: bool = Field(default=True, description="Can view system logs")
    max_concurrent_jobs: int = Field(default=5, description="Maximum concurrent jobs")
    data_retention_days: int = Field(default=30, description="Data retention period")


class SessionInfo(BaseModel):
    """User session information."""
    user_id: int = Field(..., description="User ID")
    username: str = Field(..., description="Username")
    role: UserRole = Field(..., description="User role")
    permissions: UserPermissions = Field(..., description="User permissions")
    session_id: str = Field(..., description="Session ID")
    created_at: datetime = Field(..., description="Session creation time")
    expires_at: datetime = Field(..., description="Session expiration time")
    is_active: bool = Field(default=True, description="Session active status")
