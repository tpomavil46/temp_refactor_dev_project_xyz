from __future__ import annotations

from seeq.spy.workbooks import job
from seeq.spy.workbooks._annotation import Annotation, Report, Journal
from seeq.spy.workbooks._content import DateRange, Content, AssetSelection
from seeq.spy.workbooks._data import CalculatedSignal, CalculatedCondition, CalculatedScalar, Chart, Datasource, \
    StoredSignal, StoredCondition, LiteralScalar, ThresholdMetric, Asset
from seeq.spy.workbooks._folder import Folder, SHARED, CORPORATE, ALL, USERS, MY_FOLDER, ORIGINAL_FOLDER, \
    SYNTHETIC_FOLDERS, PUBLIC
from seeq.spy.workbooks._item import Item, ItemList
from seeq.spy.workbooks._item_map import ItemMap
from seeq.spy.workbooks._load import load
from seeq.spy.workbooks._pull import pull
from seeq.spy.workbooks._push import push
from seeq.spy.workbooks._save import save
from seeq.spy.workbooks._search import search
from seeq.spy.workbooks._template import ItemTemplate, WorkbookTemplate, AnalysisTemplate, AnalysisWorksheetTemplate, \
    TopicTemplate, TopicDocumentTemplate, AnalysisWorkstepTemplate
from seeq.spy.workbooks._user import User, UserGroup, ORIGINAL_OWNER, FORCE_ME_AS_OWNER
from seeq.spy.workbooks._workbook import Workbook, Analysis, Topic, WorkbookList
from seeq.spy.workbooks._worksheet import Worksheet, AnalysisWorksheet, TopicDocument, WorksheetList
from seeq.spy.workbooks._workstep import AnalysisWorkstep

__all__ = ['search',
           'pull',
           'push',
           'load',
           'save',
           'job',
           'Workbook',
           'Analysis',
           'Topic',
           'DateRange',
           'Content',
           'AssetSelection',
           'Annotation',
           'Report',
           'Journal',
           'Worksheet',
           'AnalysisWorksheet',
           'AnalysisWorkstep',
           'TopicDocument',
           'Item',
           'ItemList',
           'ItemMap',
           'WorkbookList',
           'WorksheetList',
           'ItemTemplate',
           'WorkbookTemplate',
           'AnalysisTemplate',
           'TopicTemplate',
           'AnalysisWorksheetTemplate',
           'TopicDocumentTemplate',
           'AnalysisWorkstepTemplate',
           'ORIGINAL_OWNER', 'FORCE_ME_AS_OWNER',
           'SHARED', 'CORPORATE', 'ALL', 'USERS', 'MY_FOLDER', 'ORIGINAL_OWNER', 'SYNTHETIC_FOLDERS', 'PUBLIC']

Item.available_types = {
    'Annotation': Annotation,
    'Asset': Asset,
    'AssetSelection': AssetSelection,
    'CalculatedCondition': CalculatedCondition,
    'CalculatedScalar': CalculatedScalar,
    'CalculatedSignal': CalculatedSignal,
    'Chart': Chart,
    'Content': Content,
    'Datasource': Datasource,
    'DateRange': DateRange,
    'Folder': Folder,
    'Journal': Journal,
    'Report': Report,
    'StoredCondition': StoredCondition,
    'LiteralScalar': LiteralScalar,
    'StoredSignal': StoredSignal,
    'ThresholdMetric': ThresholdMetric,
    'Workbook': Workbook,
    'Worksheet': Worksheet,
    'Workstep': AnalysisWorkstep,
    'User': User,
    'UserGroup': UserGroup,
}
