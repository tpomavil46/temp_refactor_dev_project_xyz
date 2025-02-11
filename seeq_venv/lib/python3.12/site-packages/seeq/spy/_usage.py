from __future__ import annotations

import math
import textwrap
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict

from seeq.base.seeq_names import SeeqNames


@dataclass
class RequestTimings:
    """
    Direct port of net-link/sdk/Seeq.Link.SDK/Utilities/RequestTimings.cs
    """
    datasource_duration: timedelta
    datasource_samples_count: int
    datasource_capsules_count: int
    cache_duration: timedelta
    cache_persisted_samples_count: int
    cache_persisted_capsules_count: int
    cache_in_memory_samples_count: int
    cache_in_memory_capsules_count: int
    calc_engine_processing_duration: timedelta
    calc_engine_queue_duration: timedelta
    request_queue_duration: timedelta
    garbage_collection_duration: timedelta
    metadata_duration: timedelta
    metadata_items_count: int
    metadata_relationships_count: int

    # NOTE: since appserver always sends numeric data formatted in the US locale without thousands separators, there
    # is no need for a locale-specific parsing logic
    @staticmethod
    def from_milliseconds_string(milliseconds: str) -> timedelta:
        return timedelta(milliseconds=float(milliseconds))

    @staticmethod
    def parse_meter(header_value: str) -> int:
        return int(float(header_value))

    @staticmethod
    def from_api_response_headers(headers: Dict[str, str]) -> RequestTimings:
        pieces = dict()

        for header in [headers[SeeqNames.API.Headers.server_timing], headers[SeeqNames.API.Headers.server_meters]]:
            for _tuple in header.split(','):
                try:
                    parts = dict()
                    for part in _tuple.split(';'):
                        if part.find('=') == -1:
                            continue

                        key = part[:part.find('=')]
                        value = part[part.find('=') + 1:]
                        parts[key] = value

                    if 'dur' not in parts or 'desc' not in parts:
                        continue

                    pieces[parts['desc'].replace('"', '')] = parts['dur']
                except KeyError:
                    # CRAB-40649 There may be non-Seeq headers in the Server-Timings. Allow the rest of the pieces
                    # to complete even if this segment was invalid.
                    pass

        def _safely_get_timedelta_for_header(_key: str):
            if _key in pieces:
                try:
                    return RequestTimings.from_milliseconds_string(pieces[_key])
                except ValueError:
                    pass
            return timedelta()

        def _safely_get_meter_for_header(_key: str):
            if _key in pieces:
                try:
                    return RequestTimings.parse_meter(pieces[_key])
                except ValueError:
                    pass
            return 0

        return RequestTimings(
            _safely_get_timedelta_for_header(SeeqNames.API.Headers.Timings.datasource),
            _safely_get_meter_for_header(SeeqNames.API.Headers.Meters.datasource_samples_read),
            _safely_get_meter_for_header(SeeqNames.API.Headers.Meters.datasource_capsules_read),
            _safely_get_timedelta_for_header(SeeqNames.API.Headers.Timings.cache),
            _safely_get_meter_for_header(SeeqNames.API.Headers.Meters.cache_samples_read),
            _safely_get_meter_for_header(SeeqNames.API.Headers.Meters.cache_capsules_read),
            _safely_get_meter_for_header(SeeqNames.API.Headers.Meters.cache_in_memory_samples_read),
            _safely_get_meter_for_header(SeeqNames.API.Headers.Meters.cache_in_memory_capsules_read),
            _safely_get_timedelta_for_header(SeeqNames.API.Headers.Timings.processing),
            _safely_get_timedelta_for_header(SeeqNames.API.Headers.Timings.calc_engine_queue),
            _safely_get_timedelta_for_header(SeeqNames.API.Headers.Timings.request_queue),
            _safely_get_timedelta_for_header(SeeqNames.API.Headers.Timings.g_c),
            _safely_get_timedelta_for_header(SeeqNames.API.Headers.Timings.seeq_database),
            _safely_get_meter_for_header(SeeqNames.API.Headers.Meters.database_items_read),
            _safely_get_meter_for_header(SeeqNames.API.Headers.Meters.database_relationships_read))

    def __add__(self, other):
        return RequestTimings(
            self.datasource_duration + other.datasource_duration,
            self.datasource_samples_count + other.datasource_samples_count,
            self.datasource_capsules_count + other.datasource_capsules_count,
            self.cache_duration + other.cache_duration,
            self.cache_persisted_samples_count + other.cache_persisted_samples_count,
            self.cache_persisted_capsules_count + other.cache_persisted_capsules_count,
            self.cache_in_memory_samples_count + other.cache_in_memory_samples_count,
            self.cache_in_memory_capsules_count + other.cache_in_memory_capsules_count,
            self.calc_engine_processing_duration + other.calc_engine_processing_duration,
            self.calc_engine_queue_duration + other.calc_engine_queue_duration,
            self.request_queue_duration + other.request_queue_duration,
            self.garbage_collection_duration + other.garbage_collection_duration,
            self.metadata_duration + other.metadata_duration,
            self.metadata_items_count + other.metadata_items_count,
            self.metadata_relationships_count + other.metadata_relationships_count)


class Usage:
    """
    Used during spy.pull() to track data processing and timing information that is seen in the "rocket" in Seeq
    Workbench. This class is returned in the "Data Processed" column of the Status DataFrame of spy.pull calls.
    """

    request_timings: RequestTimings

    def __init__(self, _bytes: int = 0):
        self.request_timings = RequestTimings(
            timedelta(),
            0,
            0,
            timedelta(),
            0,
            0,
            0,
            0,
            timedelta(),
            timedelta(),
            timedelta(),
            timedelta(),
            timedelta(),
            0,
            0
        )

    def __str__(self):
        return self._humanized()

    def __repr__(self):
        return (textwrap.dedent(f"""
            Time spent in Request Queue:           {self.request_timings.request_queue_duration}
            Time spent reading Metadata:           {self.request_timings.metadata_duration}
            Time spent waiting for Datasource(s):  {self.request_timings.datasource_duration}
            Time spent reading from Seeq Cache:    {self.request_timings.cache_duration}
            Time spent in Calc Engine Queue:       {self.request_timings.calc_engine_queue_duration}
            Time spent in Calc Engine:             {self.request_timings.calc_engine_processing_duration}
            Time spent reclaiming Memory:          {self.request_timings.garbage_collection_duration}

            Metadata items read:                {self.request_timings.metadata_items_count:>17}
            Metadata relationships read:        {self.request_timings.metadata_relationships_count:>17}
            Samples read from Datasource(s):    {self.request_timings.datasource_samples_count:>17}
            Samples read from Persistent Cache: {self.request_timings.cache_persisted_samples_count:>17}
            Samples read from In-Memory Cache:  {self.request_timings.cache_in_memory_samples_count:>17}
            Capsules read from Datasource(s):   {self.request_timings.datasource_capsules_count:>17}
            Capsules read from Persistent Cache:{self.request_timings.cache_persisted_capsules_count:>17}
            Capsules read from In-Memory Cache: {self.request_timings.cache_in_memory_capsules_count:>17}
            Total bytes processed:              {self.bytes_processed:>17}
        """).strip())

    @property
    def bytes_processed(self) -> int:
        # The multiplier matches
        # appserver/drivers/graph/src/main/java/com/seeq/appserver/driver/graph/service/DataConsumptionReportService.kt
        return (16 * (self.request_timings.cache_in_memory_samples_count +
                      self.request_timings.cache_persisted_samples_count +
                      self.request_timings.datasource_samples_count)

                + 64 * (self.request_timings.cache_in_memory_capsules_count +
                        self.request_timings.cache_persisted_capsules_count +
                        self.request_timings.datasource_capsules_count))

    def _humanized(self) -> str:
        if self.bytes_processed == 0:
            return '0 B'

        suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        i = int(math.floor(math.log(self.bytes_processed, 1000)))
        p = math.pow(1000, i)
        s = int(self.bytes_processed / p)
        return '%s %s' % (s, suffixes[i])

    def add(self, http_headers: Dict[str, str]):
        request_timings = RequestTimings.from_api_response_headers(http_headers)
        self.request_timings += request_timings
