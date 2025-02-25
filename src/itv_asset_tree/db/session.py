from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# ✅ Define a database URL (use SQLite for local testing)
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

# ✅ Create database engine
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})

# ✅ SessionLocal is the DB session for FastAPI dependencies
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ✅ Base class for models
Base = declarative_base()