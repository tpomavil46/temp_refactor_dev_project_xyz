# src/itv_asset_tree/api/dependencies.py
from fastapi import Depends
from sqlalchemy.orm import Session
from itv_asset_tree.db.session import SessionLocal

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()