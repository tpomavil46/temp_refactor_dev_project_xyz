# src/itv_asset_tree/crud/item.py
from sqlalchemy.orm import Session
from itv_asset_tree.models.item import Item
from itv_asset_tree.schemas.item import ItemCreate

def get_item(db: Session, item_id: int):
    return db.query(Item).filter(Item.id == item_id).first()

def create_item(db: Session, item: ItemCreate):
    db_item = Item(name=item.name)
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item