import locale

import pytest

from seeq.base.seeq_names import SeeqNames
from seeq.spy._usage import RequestTimings


def is_within_one(value, expected):
    assert abs(expected - value) < 2, f"Value {value} is not within one of {expected}"


@pytest.mark.unit
def test_from_api_headers():
    parse_and_verify_results()


@pytest.mark.unit
def test_meter_parsing_works_in_non_english_locales():
    current_locale = locale.getlocale()

    # verify parsing behaviour in Germany
    locale.setlocale(locale.LC_ALL, 'de_DE.UTF8')
    parse_and_verify_results()

    # verify parsing behaviour in Estonia
    locale.setlocale(locale.LC_ALL, 'et_EE.UTF8')
    parse_and_verify_results()

    # verify parsing behaviour in Brazil
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF8')
    parse_and_verify_results()

    # verify parsing behaviour in Indonesia
    locale.setlocale(locale.LC_ALL, 'id_ID.UTF8')
    parse_and_verify_results()

    locale.setlocale(locale.LC_ALL, current_locale)


def parse_and_verify_results():
    server_timings = (
        "0;dur=0.00;desc=Datasource,"
        "1;dur=1.02;desc=Cache,"
        "2;dur=2.53;desc=Processing,"
        "3;dur=3.00;desc=GC,"
        "4;dur=0.05;desc=Request Queue,"
        "5;dur=14.00;desc=Calc Engine Queue,"
        "6;dur=0.61;desc=Seeq Database,"
        "7;dur=4.22;desc=Total"
    )

    server_meters = (
        "0;dur=8;desc=Datasource Samples Read,"
        "1;dur=6;desc=Datasource Capsules Read,"
        "2;dur=39;desc=Cache Samples Read,"
        "3;dur=9;desc=Cache Capsules Read,"
        "4;dur=36;desc=Cache In-Memory Samples Read,"
        "5;dur=4;desc=Cache In-Memory Capsules Read,"
        "6;dur=3;desc=Database Items Read,"
        "7;dur=1;desc=Database Relationships Read"
    )

    headers = {
        SeeqNames.API.Headers.server_timing: server_timings,
        SeeqNames.API.Headers.server_meters: server_meters
    }

    request_timings = RequestTimings.from_api_response_headers(headers)

    is_within_one(request_timings.datasource_duration.total_seconds(), 0)
    is_within_one(request_timings.cache_duration.total_seconds(), 0.0010200)
    is_within_one(request_timings.calc_engine_processing_duration.total_seconds(), 0.0025300)
    is_within_one(request_timings.garbage_collection_duration.total_seconds(), 0.0030000)
    is_within_one(request_timings.request_queue_duration.total_seconds(), 0.0000500)
    is_within_one(request_timings.calc_engine_queue_duration.total_seconds(), 0.0140000)
    is_within_one(request_timings.metadata_duration.total_seconds(), 0.0006100)

    assert request_timings.datasource_samples_count == 8
    assert request_timings.datasource_capsules_count == 6
    assert request_timings.cache_persisted_samples_count == 39
    assert request_timings.cache_persisted_capsules_count == 9
    assert request_timings.cache_in_memory_samples_count == 36
    assert request_timings.cache_in_memory_capsules_count == 4
    assert request_timings.metadata_items_count == 3
    assert request_timings.metadata_relationships_count == 1


@pytest.mark.unit
def test_invalid_headers():
    server_timings = (
        "0;dur=0.00;desc=Datasource,"
        "1;dur=1.02;desc=Cache,"
        # Processing is missing. Default to 0.
        "3;dur=3.00;desc=GC,"
        "4;dur=0.05;desc=Request Queue,"
        "5;dur=14.00;desc=Calc Engine Queue,"
        "6;dur=0.61;desc=Seeq Database,"
        # Total is missing
        'dtSInfo; desc="0"'  # Extra entries seen in CRAB-40649
        'dtRpid; desc="1639984490"'
    )

    server_meters = (
        "0;dur=8;desc=Datasource Samples Read,"
        "1;dur=6;desc=Datasource Capsules Read,"
        # Cache Samples Read is missing
        "3;dur=9;desc=Cache Capsules Read,"
        "4;dur=36;desc=Cache In-Memory Samples Read,"
        "5;dur=4;desc=Cache In-Memory Capsules Read,"
        # Database Items Read is missing
        # DB Relationships will also be 0. No trailing comma means the invalid entries will confuse its parsing.
        "7;dur=1;desc=Database Relationships Read"
        'dtSInfo; desc="0"'
        'dtRpid; desc="1639984490"'
    )

    headers = {
        SeeqNames.API.Headers.server_timing: server_timings,
        SeeqNames.API.Headers.server_meters: server_meters
    }

    request_timings = RequestTimings.from_api_response_headers(headers)

    is_within_one(request_timings.datasource_duration.total_seconds(), 0)
    is_within_one(request_timings.cache_duration.total_seconds(), 0.0010200)
    is_within_one(request_timings.calc_engine_processing_duration.total_seconds(), 0)
    is_within_one(request_timings.garbage_collection_duration.total_seconds(), 0.0030000)
    is_within_one(request_timings.request_queue_duration.total_seconds(), 0.0000500)
    is_within_one(request_timings.calc_engine_queue_duration.total_seconds(), 0.0140000)
    is_within_one(request_timings.metadata_duration.total_seconds(), 0)

    assert request_timings.datasource_samples_count == 8
    assert request_timings.datasource_capsules_count == 6
    assert request_timings.cache_persisted_samples_count == 0
    assert request_timings.cache_persisted_capsules_count == 9
    assert request_timings.cache_in_memory_samples_count == 36
    assert request_timings.cache_in_memory_capsules_count == 4
    assert request_timings.metadata_items_count == 0
    assert request_timings.metadata_relationships_count == 0
