from pydantic import BaseModel

class TreeCreateRequest(BaseModel):
    workbook_name: str
    csv_file_path: str