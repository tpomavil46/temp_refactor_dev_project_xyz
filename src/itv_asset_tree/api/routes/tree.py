from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from itv_asset_tree.api.dependencies import get_db
from pydantic import BaseModel
from itv_asset_tree.core.tree_builder import TreeBuilder
from itv_asset_tree.core.tree_modifier import TreeModifier
from itv_asset_tree.schemas.tree import TreeCreateRequest
from itv_asset_tree.utils.logger import log_info

router = APIRouter()

class CreateTreeRequest(BaseModel):
    workbook_name: str
    tree_name: str

@router.post("/build-tree")
def build_tree(request: TreeCreateRequest, db: Session = Depends(get_db)):
    """API Endpoint to build an asset tree."""
    log_info(f"🌳 Building tree for {request.workbook_name}")
    tree_builder = TreeBuilder(request.workbook_name, request.csv_file_path)
    tree_builder.load_csv()
    tree_builder.build_tree()
    return {"message": f"✅ Tree '{request.workbook_name}' built successfully"}

@router.post("/modify-tree")
def modify_tree(tree_name: str, operation: str, db: Session = Depends(get_db)):
    """API Endpoint to modify an existing asset tree."""
    log_info(f"🔄 Modifying tree '{tree_name}' with operation '{operation}'")
    tree_modifier = TreeModifier(tree_name)

    if operation == "insert":
        tree_modifier.insert("ParentPath", {"Name": "New Node"})
    elif operation == "delete":
        tree_modifier.delete("PathToDelete")

    return {"message": f"✅ Tree '{tree_name}' modified successfully"}

class CreateTreeRequest(BaseModel):
    workbook_name: str
    tree_name: str

@router.post("/create-empty-tree")
async def create_empty_tree_endpoint(request: CreateTreeRequest):
    """API endpoint to create an empty tree."""
    log_info(f"API: Creating empty tree '{request.tree_name}' in workbook '{request.workbook_name}'")
    
    try:
        builder = TreeBuilder(request.workbook_name)
        builder.build_empty_tree(request.tree_name)
        return {"message": f"✅ Empty tree '{request.tree_name}' created successfully."}
    
    except Exception as e:
        log_error(f"❌ Error creating empty tree: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create empty tree: {e}")