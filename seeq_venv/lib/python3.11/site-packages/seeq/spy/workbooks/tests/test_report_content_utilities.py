import json
import re
import types

import pytest

from seeq.sdk import *
from seeq.spy.workbooks import _report_content_utilities, _content


def _deserialize(data):
    return ApiClient().deserialize(types.SimpleNamespace(data=json.dumps(data)), 'DateRangeOutputV1')


@pytest.mark.unit
def test_daterange_fixed():
    _assert_round_trip({
        'condition': None,
        'content': [{'id': '95A0C56B-A139-465F-9BD7-9FD761CB3E8B',
                     'isArchived': False,
                     'isRedacted': False,
                     'name': 'content_DD3B2965-F10E-4BC5-B822-9AE278FD9147_3DEA1A9E-9CB0-4405-B175-E69095D3B38F',
                     'translationKey': None,
                     'type': 'ImageContent'}],
        'cronSchedule': [],
        'dateRange': {'cursorKey': None,
                      'end': '2023-04-12T17:04:23.012Z',
                      'id': '1681232663012000000L:-773801656',
                      'isUncertain': None,
                      'properties': [],
                      'start': '2023-04-11T17:04:23.012Z'},
        'description': None,
        'effectivePermissions': {'manage': True, 'read': True, 'write': True},
        'formula': 'capsule(1681232663012ms, 1681319063012ms)',
        'id': '3774DDC2-C474-4C61-8AE1-FD0F9AF56C70',
        'isArchived': False,
        'isAutoUpdating': False,
        'isNackground': True,
        'isEnabled': True,
        'isRedacted': False,
        'name': 'Date Range: Fixed',
        'report': {'id': '5957BAC5-28D6-4DCD-99D4-5EDD735F4DCC',
                   'isArchived': False,
                   'isRedacted': False,
                   'name': '"WhatsApp is a communication app." That\'s what someone '
                           'told me, and I believe them.',
                   'translationKey': None,
                   'type': 'Report'},
        'status_message': None,
        'translationKey': None,
        'type': 'DateRange'})


@pytest.mark.unit
def test_daterange_fixed_condition():
    _assert_round_trip({
        "dateRange": {
            "id": "1681225200000000000L:299932948",
            "start": "2023-04-11T15:00:00Z",
            "end": "2023-04-11T23:00:00Z",
            "properties": []
        },
        "id": "BB9723DE-EED4-42DB-B6AE-AB9BD769B71E",
        "name": "Date Range: Fixed: Condition",
        "type": "DateRange",
        "effectivePermissions": {
            "read": True,
            "write": True,
            "manage": True
        },
        "formula": "// searchStart=1681232747861ms\n        // searchEnd=1681319147861ms\n        "
                   "// columns=id,start,end,duration\n        // sortBy=start\n        "
                   "// sortAsc=true\n        capsule(1681225200000ms, 1681254000000ms)",
        "condition": {
            "id": "31BBE00E-E0E2-4AB0-BDED-B47AF013B39E",
            "name": "Shifts",
            "type": "CalculatedCondition",
            "isArchived": False,
            "isRedacted": False
        },
        "cronSchedule": [],
        "content": [
            {
                "id": "88E6B87E-33DB-4F7E-A0CC-0B1B4D44A57F",
                "name": "content_964FB305-FCF1-4D6A-B999-6E9C68C07119_BA4774B2-1FAC-4FC6-AE35-B0458BA8CE1A",
                "type": "ImageContent",
                "isArchived": False,
                "isRedacted": False
            }
        ],
        "report": {
            "id": "5957BAC5-28D6-4DCD-99D4-5EDD735F4DCC",
            "name": "\"WhatsApp is a communication app.\" That's what someone told me, and I believe them.",
            "type": "Report",
            "isArchived": False,
            "isRedacted": False
        },
        "isArchived": False,
        "isRedacted": False,
        "isBackground": True,
        "isEnabled": True,
        "isAutoUpdating": False
    })


@pytest.mark.unit
def test_daterange_autoupdate_condition():
    _assert_round_trip({
        "dateRange": {
            "id": "1681138800000000000L:1535585417",
            "start": "2023-04-10T15:00:00Z",
            "end": "2023-04-10T23:00:00Z",
            "properties": []
        },
        "id": "FA0B7607-DE3E-489C-9C25-0C87800D7ABA",
        "name": "Date Range: Auto Update: Condition",
        "type": "DateRange",
        "effectivePermissions": {
            "read": True,
            "write": True,
            "manage": True
        },
        "formula": "$condition.removeLongerThan(9h).setCertain().toGroup(capsule($now + 12min - 604800000ms, $now + 12min)).pick(-4)",
        "condition": {
            "id": "31BBE00E-E0E2-4AB0-BDED-B47AF013B39E",
            "name": "Shifts",
            "type": "CalculatedCondition",
            "isArchived": False,
            "isRedacted": False
        },
        "cronSchedule": [],
        "content": [
            {
                "id": "20B244D2-9CEF-41AC-B3EF-FDD8DF527F95",
                "name": "content_C9BAE059-F996-46B8-AEC2-6FB62A45400A_68572CD6-F53A-4E60-94EB-EA5EA01B20E1",
                "type": "ImageContent",
                "isArchived": False,
                "isRedacted": False
            }
        ],
        "report": {
            "id": "5957BAC5-28D6-4DCD-99D4-5EDD735F4DCC",
            "name": "\"WhatsApp is a communication app.\" That's what someone told me, and I believe them.",
            "type": "Report",
            "isArchived": False,
            "isRedacted": False
        },
        "isArchived": False,
        "isRedacted": False,
        "isBackground": True,
        "isEnabled": True,
        "isAutoUpdating": True
    })


@pytest.mark.unit
def test_daterange_autoupdate_daily_schedule():
    _assert_round_trip({
        "dateRange": {
            "id": "1620845254741752400L:-370267264",
            "start": "2021-05-12T18:47:34.741752400Z",
            "end": "2021-05-13T18:47:34.741752400Z",
            "properties": []
        },
        "id": "4771EFE2-6ABA-4D3B-AF16-E1A151A0CBAA",
        "name": "Date Range: Auto Update: Daily Schedule",
        "type": "DateRange",
        "effectivePermissions": {
            "read": True,
            "write": True,
            "manage": True
        },
        "formula": "capsule($now - 23month - 86400000ms, $now - 23month)",
        "cronSchedule": [],
        "content": [
            {
                "id": "52EEC29C-6168-492C-8325-A95180CCF6BE",
                "name": "content_6481C68A-DE78-47FC-A908-E79410B37FB9_CF25B5AD-D526-4499-AE0E-B42B7885BE09",
                "type": "ImageContent",
                "isArchived": False,
                "isRedacted": False
            }
        ],
        "report": {
            "id": "5957BAC5-28D6-4DCD-99D4-5EDD735F4DCC",
            "name": "\"WhatsApp is a communication app.\" That's what someone told me, and I believe them.",
            "type": "Report",
            "isArchived": False,
            "isRedacted": False
        },
        "isArchived": False,
        "isRedacted": False,
        "isBackground": True,
        "isEnabled": True,
        "isAutoUpdating": True
    })


@pytest.mark.unit
def test_daterange_autoupdate_live():
    _assert_round_trip({
        "dateRange": {
            "id": "1681243212557823000L:1129812965",
            "start": "2023-04-11T20:00:12.557823Z",
            "end": "2023-04-15T20:00:12.557823Z",
            "properties": []
        },
        "id": "0EDDBC54-3943-64E0-B1B1-BA2BB51DEA7A",
        "name": "Live Date Range",
        "type": "DateRange",
        "effectivePermissions": {
            "read": True,
            "write": True,
            "manage": True
        },
        "formula": "capsule($now + 20min - 345600000ms, $now + 20min)",
        "cronSchedule": [],
        "content": [
            {
                "id": "0EDDBBD0-9B8C-77C0-BA60-35EE6CD77544",
                "name": "content_0EDDBBC0-C59C-FDE0-8220-32C68D369F39_0EDDBBD0-0760-75B0-AF4B-CDD0A43A5BEF",
                "type": "ReactJsonContent",
                "isArchived": False,
                "isRedacted": False
            }
        ],
        "report": {
            "id": "0EDDBBC0-675C-7730-91B6-F962A297B7DA",
            "name": "Unnamed",
            "type": "Report",
            "isArchived": False,
            "isRedacted": False
        },
        "isArchived": False,
        "isRedacted": False,
        "isBackground": False,
        "isEnabled": True,
        "isAutoUpdating": True
    })


def _assert_round_trip(data):
    date_range_output: DateRangeOutputV1 = _deserialize(data)
    _content.DateRange.fix_up_date_range_object_for_compatibility(date_range_output)
    formula_1 = re.sub(r'\n\s+', '\n                ', date_range_output.formula)
    date_range_dict = _report_content_utilities.format_date_range_from_api_output(date_range_output)
    assert 'irregular_formula' not in date_range_dict
    formula_2 = _report_content_utilities.create_date_range_formula(date_range_dict)
    assert formula_1 == formula_2
