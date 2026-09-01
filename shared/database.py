"""
Shared database configuration for all services
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
import os

# Database URL
DB_URL = os.getenv(
    'DATABASE_URL',
    'mysql+pymysql://fhir_user:fhir_password@mysql:3306/fhir_db'
)

# Create engine with connection pooling
engine = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
    echo=False
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db() -> Session:
    """Dependency for FastAPI to get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
