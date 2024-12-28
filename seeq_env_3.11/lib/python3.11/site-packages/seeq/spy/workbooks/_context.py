from dataclasses import dataclass
from typing import Any, List, Optional

from seeq.spy._session import Session
from seeq.spy._status import Status


@dataclass
class WorkbookPushContext:
    access_control: Optional[str]
    datasource: Optional[str]
    dummy_items_workbook_context: Optional[Any]
    include_annotations: Optional[bool]
    override_max_interp: Optional[bool]
    owner: Optional[str]
    reconcile_inventory_by: str
    global_inventory: str
    session: Session
    specific_worksheet_ids: Optional[List[str]]
    status: Status
