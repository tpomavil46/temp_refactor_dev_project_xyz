# src/itv_asset_tree/api/routes/item.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from itv_asset_tree.crud import item as crud_item
from itv_asset_tree.schemas.item import Item, ItemCreate
from itv_asset_tree.api.dependencies import get_db

router = APIRouter()

@router.post("/items/", response_model=Item)
def create_item(item: ItemCreate, db: Session = Depends(get_db)):
    return crud_item.create_item(db=db, item=item)

@router.get("/items/{item_id}", response_model=Item)
def read_item(item_id: int, db: Session = Depends(get_db)):
    db_item = crud_item.get_item(db=db, item_id=item_id)
    if db_item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return db_item