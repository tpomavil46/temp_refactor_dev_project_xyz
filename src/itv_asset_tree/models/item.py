# src/itv_asset_tree/models/item.py
from sqlalchemy import Column, Integer, String
from itv_asset_tree.db.base_class import Base

class Item(Base):
    __tablename__ = "items"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)