# src/itv_asset_tree/models/item.py
from itv_asset_tree.db.base_class import Base  # ✅ Ensure this is correct
from sqlalchemy import Column, Integer, String

class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    description = Column(String, index=True, nullable=True)