"""
Authentication API endpoints for user management and login.
"""

from datetime import timedelta, datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from .models import (
    UserCreate, UserLogin, UserUpdate, PasswordChange, 
    Token, UserResponse, UserPermissions
)
from .auth_manager import AuthManager, UserDB

# Router setup
auth_router = APIRouter(prefix="/auth", tags=["authentication"])

# Security
security = HTTPBearer()


def get_auth_manager() -> AuthManager:
    """Dependency to get AuthManager instance."""
    return AuthManager()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    auth_manager: AuthManager = Depends(get_auth_manager)
) -> UserResponse:
    """Get current authenticated user."""
    token = credentials.credentials
    token_data = auth_manager.verify_token(token)
    
    if token_data is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = auth_manager.get_user_by_id(token_data.user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return auth_manager.get_user_response(user)


def get_current_active_user(current_user: UserResponse = Depends(get_current_user)) -> UserResponse:
    """Get current active user."""
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    return current_user


def get_admin_user(current_user: UserResponse = Depends(get_current_active_user)) -> UserResponse:
    """Get current admin user."""
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )
    return current_user


@auth_router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register_user(
    user_create: UserCreate,
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Register a new user."""
    try:
        user = auth_manager.create_user(user_create)
        return auth_manager.get_user_response(user)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@auth_router.post("/login", response_model=Token)
def login_user(
    user_login: UserLogin,
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Authenticate user and return access token."""
    user = auth_manager.authenticate_user(user_login.username, user_login.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Update last login
    auth_manager.db.query(UserDB).filter(
        UserDB.id == user.id
    ).update({"last_login": datetime.utcnow()})
    auth_manager.db.commit()
    
    # Create access token
    access_token_expires = timedelta(minutes=30)
    access_token = auth_manager.create_access_token(
        user, expires_delta=access_token_expires
    )
    
    # Create session
    session_info = auth_manager.create_session(user)
    
    return Token(
        access_token=access_token,
        token_type="bearer",
        expires_in=1800,  # 30 minutes
        user=auth_manager.get_user_response(user)
    )


@auth_router.post("/logout")
def logout_user(
    current_user: UserResponse = Depends(get_current_active_user),
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Logout user and invalidate all sessions."""
    sessions_invalidated = auth_manager.invalidate_user_sessions(current_user.id)
    return {
        "message": "Successfully logged out",
        "sessions_invalidated": sessions_invalidated
    }


@auth_router.get("/me", response_model=UserResponse)
def get_current_user_info(current_user: UserResponse = Depends(get_current_active_user)):
    """Get current user information."""
    return current_user


@auth_router.get("/permissions", response_model=UserPermissions)
def get_user_permissions(
    current_user: UserResponse = Depends(get_current_active_user),
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Get current user permissions."""
    user = auth_manager.get_user_by_id(current_user.id)
    return auth_manager._get_user_permissions(user)


@auth_router.put("/me", response_model=UserResponse)
def update_current_user(
    user_update: UserUpdate,
    current_user: UserResponse = Depends(get_current_active_user),
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Update current user information."""
    user = auth_manager.update_user(current_user.id, user_update)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return auth_manager.get_user_response(user)


@auth_router.post("/change-password")
def change_password(
    password_change: PasswordChange,
    current_user: UserResponse = Depends(get_current_active_user),
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Change user password."""
    success = auth_manager.change_password(current_user.id, password_change)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )
    return {"message": "Password changed successfully"}


@auth_router.get("/users", response_model=list[UserResponse])
def list_users(
    skip: int = 0,
    limit: int = 100,
    current_user: UserResponse = Depends(get_admin_user),
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """List all users (admin only)."""
    users = auth_manager.db.query(UserDB).offset(skip).limit(limit).all()
    return [auth_manager.get_user_response(auth_manager._db_user_to_user(user)) for user in users]


@auth_router.get("/users/{user_id}", response_model=UserResponse)
def get_user(
    user_id: int,
    current_user: UserResponse = Depends(get_admin_user),
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Get user by ID (admin only)."""
    user = auth_manager.get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return auth_manager.get_user_response(user)


@auth_router.put("/users/{user_id}", response_model=UserResponse)
def update_user(
    user_id: int,
    user_update: UserUpdate,
    current_user: UserResponse = Depends(get_admin_user),
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Update user by ID (admin only)."""
    user = auth_manager.update_user(user_id, user_update)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    return auth_manager.get_user_response(user)


@auth_router.delete("/users/{user_id}")
def delete_user(
    user_id: int,
    current_user: UserResponse = Depends(get_admin_user),
    auth_manager: AuthManager = Depends(get_auth_manager)
):
    """Delete user by ID (admin only)."""
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )
    
    user = auth_manager.get_user_by_id(user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Soft delete by setting status to inactive
    auth_manager.db.query(UserDB).filter(
        UserDB.id == user_id
    ).update({
        "status": "inactive",
        "is_active": False
    })
    auth_manager.db.commit()
    
    return {"message": "User deleted successfully"}


@auth_router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "authentication"}
