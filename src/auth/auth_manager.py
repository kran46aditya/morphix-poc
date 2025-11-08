"""
Authentication manager for user management, JWT tokens, and session handling.
"""

import os
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError

from .models import (
    User, UserCreate, UserLogin, UserUpdate, PasswordChange, 
    Token, TokenData, UserResponse, UserRole, UserStatus,
    UserPermissions, SessionInfo
)

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/morphix")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT settings
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_urlsafe(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))


class UserDB(Base):
    """User database model."""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    full_name = Column(String(100), nullable=False)
    hashed_password = Column(String(255), nullable=False)
    role = Column(SQLEnum(UserRole), default=UserRole.USER, nullable=False)
    status = Column(SQLEnum(UserStatus), default=UserStatus.ACTIVE, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_login = Column(DateTime, nullable=True)


class SessionDB(Base):
    """User session database model."""
    __tablename__ = "user_sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    session_id = Column(String(255), unique=True, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)


# Create tables (lazy initialization)
def _create_tables():
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f"Warning: Could not create database tables: {e}")
        print("Tables will be created when database is available")


class AuthManager:
    """Authentication manager for user operations and JWT handling."""
    
    def __init__(self):
        self.db = SessionLocal()
        # Try to create tables on first use
        try:
            _create_tables()
        except Exception:
            pass  # Tables will be created when database is available
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()
    
    def get_db(self) -> Session:
        """Get database session."""
        return self.db
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        return pwd_context.verify(plain_password, hashed_password)
    
    def get_password_hash(self, password: str) -> str:
        """Hash a password."""
        return pwd_context.hash(password)
    
    def create_user(self, user_create: UserCreate) -> User:
        """Create a new user."""
        # Check if user already exists
        if self.get_user_by_username(user_create.username):
            raise ValueError("Username already exists")
        if self.get_user_by_email(user_create.email):
            raise ValueError("Email already exists")
        
        # Create user
        hashed_password = self.get_password_hash(user_create.password)
        db_user = UserDB(
            username=user_create.username,
            email=user_create.email,
            full_name=user_create.full_name,
            hashed_password=hashed_password,
            role=user_create.role,
            status=user_create.status
        )
        
        try:
            self.db.add(db_user)
            self.db.commit()
            self.db.refresh(db_user)
            return self._db_user_to_user(db_user)
        except IntegrityError:
            self.db.rollback()
            raise ValueError("User creation failed due to constraint violation")
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        db_user = self.db.query(UserDB).filter(UserDB.username == username).first()
        return self._db_user_to_user(db_user) if db_user else None
    
    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        db_user = self.db.query(UserDB).filter(UserDB.email == email).first()
        return self._db_user_to_user(db_user) if db_user else None
    
    def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        db_user = self.db.query(UserDB).filter(UserDB.id == user_id).first()
        return self._db_user_to_user(db_user) if db_user else None
    
    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Authenticate user with username/email and password."""
        # Fetch DB user so we can access hashed_password safely
        db_user = (
            self.db.query(UserDB)
            .filter((UserDB.username == username) | (UserDB.email == username))
            .first()
        )
        if not db_user:
            return None
        if not self.verify_password(password, db_user.hashed_password):
            return None
        if db_user.status != UserStatus.ACTIVE or not db_user.is_active:
            return None
        return self._db_user_to_user(db_user)
    
    def update_user(self, user_id: int, user_update: UserUpdate) -> Optional[User]:
        """Update user information."""
        db_user = self.db.query(UserDB).filter(UserDB.id == user_id).first()
        if not db_user:
            return None
        
        # Update fields
        if user_update.full_name is not None:
            db_user.full_name = user_update.full_name
        if user_update.email is not None:
            db_user.email = user_update.email
        if user_update.role is not None:
            db_user.role = user_update.role
        if user_update.status is not None:
            db_user.status = user_update.status
        
        db_user.updated_at = datetime.utcnow()
        
        try:
            self.db.commit()
            self.db.refresh(db_user)
            return self._db_user_to_user(db_user)
        except IntegrityError:
            self.db.rollback()
            raise ValueError("User update failed due to constraint violation")
    
    def change_password(self, user_id: int, password_change: PasswordChange) -> bool:
        """Change user password."""
        db_user = self.db.query(UserDB).filter(UserDB.id == user_id).first()
        if not db_user:
            return False
        
        # Verify current password
        if not self.verify_password(password_change.current_password, db_user.hashed_password):
            return False
        
        # Update password
        db_user.hashed_password = self.get_password_hash(password_change.new_password)
        db_user.updated_at = datetime.utcnow()
        
        self.db.commit()
        return True
    
    def create_access_token(self, user: User, expires_delta: Optional[timedelta] = None) -> str:
        """Create JWT access token."""
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        
        to_encode = {
            "user_id": user.id,
            "username": user.username,
            "role": user.role.value,
            "exp": expire
        }
        
        return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
    def verify_token(self, token: str) -> Optional[TokenData]:
        """Verify and decode JWT token."""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            user_id: int = payload.get("user_id")
            username: str = payload.get("username")
            role: str = payload.get("role")
            exp: datetime = payload.get("exp")
            
            if user_id is None or username is None or exp is None:
                return None
            
            # Check if token is expired
            if datetime.utcnow() > datetime.fromtimestamp(exp):
                return None
            
            return TokenData(
                user_id=user_id,
                username=username,
                role=UserRole(role),
                exp=datetime.fromtimestamp(exp)
            )
        except JWTError:
            return None
    
    def create_session(self, user: User) -> SessionInfo:
        """Create a user session."""
        session_id = secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
        
        db_session = SessionDB(
            user_id=user.id,
            session_id=session_id,
            expires_at=expires_at
        )
        
        self.db.add(db_session)
        self.db.commit()
        
        return SessionInfo(
            user_id=user.id,
            username=user.username,
            role=user.role,
            permissions=self._get_user_permissions(user),
            session_id=session_id,
            created_at=datetime.utcnow(),
            expires_at=expires_at
        )
    
    def get_session(self, session_id: str) -> Optional[SessionInfo]:
        """Get session information."""
        db_session = self.db.query(SessionDB).filter(
            SessionDB.session_id == session_id,
            SessionDB.is_active == True,
            SessionDB.expires_at > datetime.utcnow()
        ).first()
        
        if not db_session:
            return None
        
        user = self.get_user_by_id(db_session.user_id)
        if not user:
            return None
        
        return SessionInfo(
            user_id=user.id,
            username=user.username,
            role=user.role,
            permissions=self._get_user_permissions(user),
            session_id=session_id,
            created_at=db_session.created_at,
            expires_at=db_session.expires_at
        )
    
    def invalidate_session(self, session_id: str) -> bool:
        """Invalidate a user session."""
        db_session = self.db.query(SessionDB).filter(SessionDB.session_id == session_id).first()
        if not db_session:
            return False
        
        db_session.is_active = False
        self.db.commit()
        return True
    
    def invalidate_user_sessions(self, user_id: int) -> int:
        """Invalidate all sessions for a user."""
        sessions = self.db.query(SessionDB).filter(
            SessionDB.user_id == user_id,
            SessionDB.is_active == True
        ).all()
        
        for session in sessions:
            session.is_active = False
        
        self.db.commit()
        return len(sessions)
    
    def _get_user_permissions(self, user: User) -> UserPermissions:
        """Get user permissions based on role."""
        if user.role == UserRole.ADMIN:
            return UserPermissions(
                can_create_jobs=True,
                can_manage_users=True,
                can_access_admin=True,
                can_manage_schemas=True,
                can_view_logs=True,
                max_concurrent_jobs=50,
                data_retention_days=365
            )
        elif user.role == UserRole.USER:
            return UserPermissions(
                can_create_jobs=True,
                can_manage_users=False,
                can_access_admin=False,
                can_manage_schemas=True,
                can_view_logs=True,
                max_concurrent_jobs=10,
                data_retention_days=90
            )
        else:  # VIEWER
            return UserPermissions(
                can_create_jobs=False,
                can_manage_users=False,
                can_access_admin=False,
                can_manage_schemas=False,
                can_view_logs=True,
                max_concurrent_jobs=0,
                data_retention_days=30
            )
    
    def _db_user_to_user(self, db_user: UserDB) -> User:
        """Convert database user to User model."""
        return User(
            id=db_user.id,
            username=db_user.username,
            email=db_user.email,
            full_name=db_user.full_name,
            role=db_user.role,
            status=db_user.status,
            created_at=db_user.created_at,
            updated_at=db_user.updated_at,
            last_login=db_user.last_login,
            is_active=db_user.is_active
        )
    
    def get_user_response(self, user: User) -> UserResponse:
        """Convert User to UserResponse (excludes sensitive data)."""
        return UserResponse(
            id=user.id,
            username=user.username,
            email=user.email,
            full_name=user.full_name,
            role=user.role,
            status=user.status,
            created_at=user.created_at,
            updated_at=user.updated_at,
            last_login=user.last_login,
            is_active=user.is_active
        )
