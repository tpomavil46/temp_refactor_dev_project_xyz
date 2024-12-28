from __future__ import annotations

import copy
import datetime
import re
import types
from typing import Optional, Tuple, Dict

from seeq.sdk import *
from seeq.spy import _login

# *********************************************************************************************************************
#
# This file is a direct port of several TypeScript functions from
# client/packages/webserver/app/src/annotation/reportContent.utilities.ts
# There should be NO CHANGES IN LOGIC in this file as compared to that TypeScript file, down to the slightest detail.
# Otherwise, the frontend may not correctly process the Date Ranges that are pushed by SPy and likewise SPy may not
# process the Date Ranges that are pulled from the frontend.
#
# *********************************************************************************************************************


QUARTZ_CRON_PENDING_INPUT = '59 59 23 31 12 ? 2099'

DURATION_SCALAR_UNITS = [
    's', 'second', 'seconds', 'min', 'minute', 'minutes', 'h', 'hr', 'hrs', 'hour', 'hours', 'day', 'd', 'days',
    'week', 'wk', 'weeks', 'month', 'mo', 'months', 'year', 'y', 'yr', 'years',
    'ns', 'ms'
]

OFFSET_DIRECTION = types.SimpleNamespace(
    PAST='past',
    FUTURE='future'
)

CAPSULE_SELECTION = types.SimpleNamespace(
    STRATEGY=types.SimpleNamespace(
        CLOSEST_TO='closestTo',
        OFFSET_BY='offsetBy',
    ),
    REFERENCE=types.SimpleNamespace(
        START='start',
        END='end',
    ),
)

DEFAULT_DATE_RANGE = {
    'auto': {
        'enabled': False,
        'duration': datetime.timedelta(days=1).total_seconds() * 1000,
        'offset': {'value': 0, 'units': 'min'},
        'offsetDirection': OFFSET_DIRECTION.PAST,
        'cronSchedule': [QUARTZ_CRON_PENDING_INPUT],
        'noCapsuleFound': False,
    },
    'condition': {
        'strategy': CAPSULE_SELECTION.STRATEGY.CLOSEST_TO,
        'reference': CAPSULE_SELECTION.REFERENCE.END,
        'offset': 1,
        'range': {},
    },
    'range': {},
    'enabled': True,
}


# export const formatDateRangeFromApiOutput = (dateRangeOutput: DateRangeOutputV1): DateRange => {
def format_date_range_from_api_output(date_range_output: DateRangeOutputV1) -> dict:
    # let formulaFormValid = false;
    formula_form_valid = False
    # const dateRange: any = {};
    # _.defaultsDeep(dateRange, DEFAULT_DATE_RANGE);
    date_range: dict = copy.deepcopy(DEFAULT_DATE_RANGE)
    # _.assign(dateRange, _.pick(dateRangeOutput, ['name', 'id', 'description']));
    date_range.update({'name': date_range_output.name,
                       'id': date_range_output.id,
                       'description': date_range_output.description})
    # noinspection PyUnresolvedReferences
    # _.assign(dateRange.condition, _.pick(dateRangeOutput.condition, ['name', 'id', 'isRedacted']));
    if date_range_output.condition is not None:
        date_range['condition'].update({'name': date_range_output.condition.name,
                                        'id': date_range_output.condition.id,
                                        'isRedacted': date_range_output.condition.is_redacted})
    # noinspection PyUnresolvedReferences
    # _.assign(dateRange.auto, {
    #   enabled: dateRangeOutput.isAutoUpdating,
    # });
    date_range['auto'].update({'enabled': date_range_output.is_auto_updating})
    # dateRange.enabled = dateRangeOutput.isEnabled;
    date_range['enabled'] = date_range_output.is_enabled
    # dateRange.reportId = dateRangeOutput.report?.id;
    date_range['reportId'] = date_range_output.report.id if date_range_output.report is not None else None
    # dateRange.isArchived = dateRangeOutput.isArchived;
    date_range['isArchived'] = date_range_output.is_archived

    #
    # // The backend gives us back ISO8601 timestamps, but expects milliseconds back.
    # if (dateRangeOutput.dateRange?.start) {
    #   dateRange.range.start = moment.utc(dateRangeOutput.dateRange.start).valueOf();
    # }
    if date_range_output.date_range is not None and date_range_output.date_range.start is not None:
        date_range['range']['start'] = int(_login.parse_content_datetime_with_timezone(
            session=None, dt=date_range_output.date_range.start).value / 1_000_000)
    #
    # if (dateRangeOutput.dateRange?.end) {
    #   dateRange.range.end = moment.utc(dateRangeOutput.dateRange.end).valueOf();
    # }
    if date_range_output.date_range is not None and date_range_output.date_range.end is not None:
        date_range['range']['end'] = int(_login.parse_content_datetime_with_timezone(
            session=None, dt=date_range_output.date_range.end).value / 1_000_000)
    #
    # try {
    try:
        # // See .createDateRangeFormula() for expected formula formats
        #
        # // Fixed, no condition
        # if (!dateRangeOutput.condition?.id && dateRangeOutput.formula?.match(/^capsule\(.*\)$/)) {
        if ((date_range_output.condition is None or date_range_output.condition.id is None) and
                (date_range_output.formula is not None and re.search(r'^capsule\(.*\)$', date_range_output.formula))):
            # // Nothing additional needs to be extracted from the formula
            # formulaFormValid = true;
            formula_form_valid = True

        # }
        #
        # const setConditionProperties = (
        #   searchStart?: string,
        #   searchEnd?: string,
        #   maxDuration?: string,
        #   columns?: string,
        #   sortBy?: string,
        #   sortAsc?: string,
        # ) => {
        def set_condition_properties(search_start: Optional[str] = None,
                                     search_end: Optional[str] = None,
                                     max_duration: Optional[str] = None,
                                     columns: Optional[str] = None,
                                     sort_by: Optional[str] = None,
                                     sort_asc: Optional[str] = None):
            # dateRange.condition.range = {
            #   start: toNumber(searchStart),
            #   end: toNumber(searchEnd),
            # };
            date_range['condition']['range'] = {
                'start': int(search_start) if search_start is not None else None,
                'end': int(search_end) if search_end is not None else None,
            }
            #
            # if (columns) {
            if columns is not None:
                # dateRange.condition.columns = columns.split(',');
                date_range['condition']['columns'] = columns.split(',')
                # dateRange.condition.sortBy = sortBy;
                date_range['condition']['sortBy'] = sort_by
                # dateRange.condition.sortAsc = sortAsc === 'true';
                date_range['condition']['sortAsc'] = sort_asc.lower() == 'true'
                # dateRange.condition.isCapsuleFromTable = true;
                date_range['condition']['isCapsuleFromTable'] = True
            # } else {
            else:
                # dateRange.condition.isCapsuleFromTable = false;
                date_range['condition']['isCapsuleFromTable'] = False
            # }
            #
            # if (maxDuration) {
            if max_duration is not None:
                # const maximumDuration = splitDuration(maxDuration);
                maximum_duration = _split_duration(max_duration)
                # if (!maximumDuration) {
                if maximum_duration is None:
                    # throw new Error(`Could not parse ${maxDuration} as a maximum duration`);
                    raise ValueError(f'Could not parse {max_duration} as a maximum duration')
                # }
                #
                # if (!_.includes(DURATION_SCALAR_UNITS, maximumDuration.units)) {
                if maximum_duration['units'] not in DURATION_SCALAR_UNITS:
                    # throw new Error(
                    # `Invalid maximum duration unit ${maximumDuration.units} in ${maxDuration} as a maximum duration`,
                    # );
                    raise ValueError(
                        f'Invalid maximum duration unit {maximum_duration["units"]} in {max_duration} as a maximum '
                        f'duration')
                # }
                #
                # dateRange.condition.maximumDuration = maximumDuration;
                date_range['condition']['maximumDuration'] = maximum_duration

            # }

        # };
        #
        # // auto enabled
        # const setOffsetProperties = (offset = '1') => {
        def set_offset_properties(offset='1'):
            # const pick = toNumber(offset) ?? 1;
            pick = int(offset) if offset is not None else 1
            # if (pick === 1) {
            if pick == 1:
                # dateRange.condition.strategy = CAPSULE_SELECTION.STRATEGY.CLOSEST_TO;
                date_range['condition']['strategy'] = CAPSULE_SELECTION.STRATEGY.CLOSEST_TO
                # dateRange.condition.reference = CAPSULE_SELECTION.REFERENCE.START;
                date_range['condition']['reference'] = CAPSULE_SELECTION.REFERENCE.START
                # dateRange.condition.offset = 1;
                date_range['condition']['offset'] = 1
            # } else if (pick > 1) {
            elif pick > 1:
                # dateRange.condition.strategy = CAPSULE_SELECTION.STRATEGY.OFFSET_BY;
                date_range['condition']['strategy'] = CAPSULE_SELECTION.STRATEGY.OFFSET_BY
                # dateRange.condition.reference = CAPSULE_SELECTION.REFERENCE.START;
                date_range['condition']['reference'] = CAPSULE_SELECTION.REFERENCE.START
                # dateRange.condition.offset = pick - 1;
                date_range['condition']['offset'] = pick - 1
            # } else if (pick === -1) {
            elif pick == -1:
                # dateRange.condition.strategy = CAPSULE_SELECTION.STRATEGY.CLOSEST_TO;
                date_range['condition']['strategy'] = CAPSULE_SELECTION.STRATEGY.CLOSEST_TO
                # dateRange.condition.reference = CAPSULE_SELECTION.REFERENCE.END;
                date_range['condition']['reference'] = CAPSULE_SELECTION.REFERENCE.END
                # dateRange.condition.offset = 1;
                date_range['condition']['offset'] = 1
            # } else if (pick < -1) {
            elif pick < -1:
                # dateRange.condition.strategy = CAPSULE_SELECTION.STRATEGY.OFFSET_BY;
                date_range['condition']['strategy'] = CAPSULE_SELECTION.STRATEGY.OFFSET_BY
                # dateRange.condition.reference = CAPSULE_SELECTION.REFERENCE.END;
                date_range['condition']['reference'] = CAPSULE_SELECTION.REFERENCE.END
                # dateRange.condition.offset = Math.abs(pick) - 1;
                date_range['condition']['offset'] = abs(pick) - 1
            # }

        # };
        #
        # if (!dateRange.auto.enabled && !_.includes(dateRangeOutput.formula, 'Offset')) {
        if not date_range['auto']['enabled'] and 'Offset' not in date_range_output.formula:
            # // Fixed, with condition capsule selected from table
            # const fixedMatchPattern =
            #   /^\s*\/\/ searchStart=(.*?)ms\n\s*\/\/ searchEnd=(.*?)ms\n\s*\/\/ columns=(.*?)\n\s*\/\/ sortBy=(
            #  .*?)\n\s*\/\/ sortAsc=(.*?)\n\s* capsule\(.+\)$/m;
            fixed_match_pattern = re.compile(
                r'^\s*// searchStart=(.*?)ms\n\s*// searchEnd=(.*?)ms\n\s*// columns=(.*?)\n\s*// '
                r'sortBy=(.*?)\n\s*// sortAsc=(.*?)\n\s* capsule\(.+\)$', re.MULTILINE)
            # const fixedMatches = dateRangeOutput.formula?.match(fixedMatchPattern);
            fixed_matches = fixed_match_pattern.search(date_range_output.formula)
            # if (dateRangeOutput.condition?.id && fixedMatches) {
            if date_range_output.condition is not None and fixed_matches is not None:
                # const [notUsed, parsedStart, parsedEnd, columns, sortBy, sortAsc] = fixedMatches;
                parsed_start, parsed_end, columns, sort_by, sort_asc = fixed_matches.groups()
                # setConditionProperties(parsedStart, parsedEnd, undefined, columns, sortBy, sortAsc);
                set_condition_properties(parsed_start, parsed_end, None, columns, sort_by, sort_asc)
                # formulaFormValid = true;
                formula_form_valid = True
            # }
        # } else if (!dateRange.auto.enabled && dateRangeOutput.formula?.includes('Offset')) {
        elif (not date_range['auto']['enabled'] and date_range_output.formula is not None and 'Offset' in
              date_range_output.formula):
            # // Fixed, with selected relative capsule
            # const fixedConfigMatchPattern =
            #   /^\s*\/\/ searchStart=(.*?)ms\n\s*\/\/ searchEnd=(.*?)ms\n\s*\/\/ capsuleOffset=(.*?)\n\s*\/\/
            #  maxDuration=(.*?)\n\s*capsule\(.+\)$/m;
            fixed_config_match_pattern = re.compile(
                r'^\s*// searchStart=(.*?)ms\n\s*// searchEnd=(.*?)ms\n\s*// capsuleOffset=(.*?)\n\s*// '
                r'maxDuration=(.*?)\n\s*capsule\(.+\)$', re.MULTILINE)
            # const fixedMatches = dateRangeOutput.formula.match(fixedConfigMatchPattern);
            fixed_matches = fixed_config_match_pattern.search(date_range_output.formula)
            # if (dateRangeOutput.condition?.id && fixedMatches) {
            if (date_range_output.condition is not None and date_range_output.condition.get('id') is not None and
                    fixed_matches is not None):
                # const [notUsed, parsedStart, parsedEnd, capsuleOffset, parsedMaxDuration] = fixedMatches;
                parsed_start, parsed_end, capsule_offset, parsed_max_duration = fixed_matches.groups()
                # setConditionProperties(parsedStart, parsedEnd, parsedMaxDuration);
                set_condition_properties(parsed_start, parsed_end, parsed_max_duration)
                # setOffsetProperties(capsuleOffset);
                set_offset_properties(capsule_offset)
                # formulaFormValid = true;
                formula_form_valid = True
            # }
        # }
        #
        # // Auto-update with condition
        # const autoConditionMatchPattern =
        #   /^\$condition(.removeLongerThan\(.+?\))?.setCertain\(\).toGroup\(capsule\(.+\)\).pick\((-?[1-9]+[0-9]*)\)$/;
        auto_condition_match_pattern = re.compile(
            r'^\$condition(.removeLongerThan\(.+?\))?.setCertain\(\).toGroup\(capsule\(.+\)\).pick\((-?[1-9]+[0-9]*)\)$'
        )
        # const autoMatches = dateRangeOutput.formula?.match(autoConditionMatchPattern);
        auto_matches = (auto_condition_match_pattern.search(date_range_output.formula)
                        if date_range_output.formula is not None else None)
        # if (dateRangeOutput.condition?.id && autoMatches) {
        if date_range_output.condition is not None and auto_matches is not None:
            # // Closest to Start =>  $condition.setCertain().toGroup(capsule(start, end)).pick(1)
            # // Closest to End => $condition.setCertain().toGroup(capsule(start, end)).pick(-1)
            # // Offset by 1 from Start => $condition.setCertain().toGroup(capsule(start, end)).pick(2)
            # // Offset by 2 from Start => $condition.setCertain().toGroup(capsule(start, end)).pick(3)
            # // Offset by 1 from End => $condition.setCertain().toGroup(capsule(start, end)).pick(-2)
            parsed_pick = None
            parsed_maximum_duration = None
            if date_range_output.formula is not None:
                # const parsedPick = dateRangeOutput.formula?.match(/.*\.pick\((-?[1-9]+[0-9]*)\)$/)[1];
                parsed_pick = re.search(r'.*\.pick\((-?[1-9]+[0-9]*)\)$', date_range_output.formula).group(1)
                # const parsedMaximumDuration = dateRangeOutput.formula?.match(/removeLongerThan\((.+)\)/)?.[1];
                maximum_duration_match = re.search(r'removeLongerThan\((.+)\)', date_range_output.formula)
                if maximum_duration_match is not None:
                    parsed_maximum_duration = maximum_duration_match.group(1)
            # setConditionProperties(undefined, undefined, parsedMaximumDuration);
            set_condition_properties(None, None, parsed_maximum_duration)
            # setOffsetProperties(parsedPick);
            set_offset_properties(parsed_pick)
            # formulaFormValid = true;
            formula_form_valid = True
        # }
        #
        # // Auto-update, condition and non-condition
        # if (dateRange.auto.enabled) {
        if date_range['auto']['enabled']:
            # const background = dateRangeOutput.isBackground;
            background = date_range_output.is_background
            # const cronSchedule = dateRangeOutput.cronSchedule;
            cron_schedule = date_range_output.cron_schedule
            # const duration = extractDurationFromFormula(dateRangeOutput.formula);
            duration = _extract_duration_from_formula(date_range_output.formula)
            # const offsetAndDirection = extractOffsetAndDirectionFromFormula(dateRangeOutput.formula ?? '');
            offset_and_direction = _extract_offset_and_direction_from_formula(date_range_output.formula or '')
            # const offset = offsetAndDirection[0];
            offset = offset_and_direction[0]
            # const offsetDirection = offsetAndDirection[1];
            offset_direction = offset_and_direction[1]
            #
            # dateRange.auto = {
            date_range['auto'] = {
                # enabled: true,
                'enabled': True,
                # duration,
                'duration': duration,
                # offset,
                'offset': offset,
                # offsetDirection,
                'offsetDirection': offset_direction,
                # background,
                'background': background,
                # cronSchedule,
                'cronSchedule': cron_schedule,
                # };
            }
        # }
    # } catch (e) {
    except Exception as e:
        # logWarn(e);
        # formulaFormValid = false;
        formula_form_valid = False
    # }
    #
    # if (!formulaFormValid) {
    if not formula_form_valid:
        # logWarn(`Failed to parse date range formula "${dateRangeOutput.formula}" [${dateRangeOutput.id}]`);
        # dateRange.irregularFormula = dateRangeOutput.formula;
        date_range['irregularFormula'] = date_range_output.formula
    # }
    # return dateRange;
    return date_range
    # };


def compute_capsule_offset(condition):
    strategy = condition["strategy"]
    reference = condition["reference"]
    offset = condition.get("offset", 1)
    offset_value = 1 if strategy == "closestTo" else int(offset) + 1
    sign_value = 1 if reference == "start" else -1
    return offset_value * sign_value


def create_date_range_formula(date_range: dict) -> str:
    # if (!dateRange.auto.enabled && !dateRange.condition.id) {
    # return `capsule(${dateRange.range.start}ms, ${dateRange.range.end}ms)`;
    # }
    if (not date_range['auto']['enabled'] and
            (date_range['condition'] is None or date_range['condition'].get('id') is None)):
        # Fixed range, no condition
        return f"capsule({date_range['range']['start']}ms, {date_range['range']['end']}ms)"

    # const capsuleOffset = computeCapsuleOffset(dateRange.condition);
    capsule_offset = compute_capsule_offset(date_range['condition'])

    # const maximumDuration = dateRange.condition?.maximumDuration
    # ? `${dateRange.condition.maximumDuration.value}${dateRange.condition.maximumDuration.units}`
    # : '';
    maximum_duration = (
        f"{date_range['condition']['maximumDuration']['value']}{date_range['condition']['maximumDuration']['units']}"
        if (date_range['condition'] is not None and date_range['condition'].get('maximumDuration') is not None) else '')

    if (not date_range['auto']['enabled'] and date_range['condition'].get('id') is not None
            and date_range['condition'].get('isCapsuleFromTable', False)):
        # Fixed range, condition
        # For this configuration, we save the search range and other parameters used to find the capsule in a formula
        # comment, so that we can present it back to the user in the UI when editing the dateRange.
        return (f"""// searchStart={date_range['condition']['range']['start']}ms
                // searchEnd={date_range['condition']['range']['end']}ms
                // columns={','.join(date_range['condition']['columns'])}
                // sortBy={date_range['condition']['sortBy']}
                // sortAsc={str(date_range['condition']['sortAsc']).lower()}
                capsule({date_range['range']['start']}ms, {date_range['range']['end']}ms)""")
    elif (not date_range['auto']['enabled'] and date_range['condition'].get('id') is not None
          and not date_range['condition'].get('isCapsuleFromTable', False)):
        return (f"""// searchStart={date_range['condition']['range']['start']}ms
                // searchEnd={date_range['condition']['range']['end']}ms
                // capsuleOffset={capsule_offset}
                // maxDuration={maximum_duration}
                capsule({date_range['range']['start']}ms, {date_range['range']['end']}ms)""")
    else:
        # Auto-update range
        sign = '+' if date_range['auto']['offsetDirection'] == OFFSET_DIRECTION.FUTURE else '-'
        offset = f"{date_range['auto']['offset']['value']}{date_range['auto']['offset']['units']}"
        duration = f"{date_range['auto']['duration']}ms"
        range_formula = f"capsule($now {sign} {offset} - {duration}, $now {sign} {offset})"

    if date_range['condition'].get('id') is not None:
        maximum_duration_snippet = f".removeLongerThan({maximum_duration})" if maximum_duration else ''
        return f"$condition{maximum_duration_snippet}.setCertain().toGroup({range_formula}).pick({capsule_offset})"
    else:
        return range_formula


def _split_duration(duration_string):
    value = None
    units = None

    if isinstance(duration_string, str):
        value = re.search(r'[-+]?[0-9]*\.?[0-9]+', duration_string).group(0)

        if value:
            units = re.search(r'[a-zA-Z]+', duration_string).group(0)

            if units:
                return {'value': value, 'units': units}


def _extract_duration_from_formula(formula: str) -> Optional[int]:
    match = re.search(r'\$now\s*[+-]\s*(.*?)\s*[+-]\s*([\d.]+[a-z]+),', formula, re.IGNORECASE)
    duration_match = match.group(2) if match is not None else None
    return int(_parse_duration(duration_match).total_seconds() * 1000) if duration_match is not None else None


# export function parseDuration(durationInput: string): Duration {
def _parse_duration(duration_input: str) -> datetime.timedelta:
    # const invalidDuration = moment.duration(0);
    invalid_duration = datetime.timedelta(0)
    # const trimmedInput = _.trim(durationInput);
    trimmed_input = duration_input.strip()
    # if (isNaN(Number(trimmedInput[0]))) {
    if not trimmed_input[0].isnumeric():
        # return invalidDuration;
        return invalid_duration
    # }
    #
    # const patterns = i18next.t('DURATION_PATTERNS', { returnObjects: true });
    patterns = {
        'DURATION_PATTERNS': {
            'years': ['y', 'yr', 'yrs', 'year', 'years'],
            'months': ['mo', 'mos', 'month', 'months'],
            'weeks': ['w', 'wk', 'wks', 'week', 'weeks'],
            'days': ['d', 'day', 'days'],
            'hours': ['h', 'hr', 'hrs', 'hour', 'hours'],
            'minutes': ['m', 'min', 'mins', 'minute', 'minutes'],
            'seconds': ['s', 'sec', 'secs', 'second', 'seconds'],
            'milliseconds': ['ms', 'millisecond', 'milliseconds'],
        }
    }
    # const DURATION_CONVERSIONS = {
    DURATION_CONVERSIONS = {
        'years': 31536000000,
        'months': 2592000000,
        'weeks': 604800000,
        'days': 86400000,
        'hours': 3600000,
        'minutes': 60000,
        'seconds': 1000,
        'milliseconds': 1,
    }
    #
    # const matches: string[] = [];
    matches = []

    duration_in_ms = 0
    for key in DURATION_CONVERSIONS.keys():
        # for (const pattern of patterns[key]) {
        for pattern in patterns['DURATION_PATTERNS'][key]:
            # const regex = new RegExp(`((\\d+\\.\\d+)|\\d+)\\s?(${pattern}(?=\\s|\\d|$))`, 'gi');
            regex = re.compile(f"((\\d+\\.\\d+)|\\d+)\\s?({pattern}(?=\\s|\\d|$))", re.IGNORECASE)
            # for (const match of durationInput.matchAll(regex)) {
            for match in [[m.group(0)] + list(m.groups()) for m in regex.finditer(trimmed_input)]:
                # if (!_.includes(matches, match[0])) {
                if match[0] not in matches:
                    # matches.push(match[0]);
                    matches.append(match[0])
                    # return total + parseFloat(match[1]) * multiplier;
                    duration_in_ms += float(match[1]) * DURATION_CONVERSIONS[key]
                # }
            # }
        # }
        #
        # return total;
    # },

    # const durationInSeconds: number = _.reduce(
    #     (total, multiplier, key) => {
    #     DURATION_CONVERSIONS,
    #     0,
    # );

    #
    # if (_.isEmpty(matches)) {
    if not matches:
        # return invalidDuration;
        return invalid_duration
    # } else {
    else:
        # return moment.duration(durationInSeconds, 'seconds');
        return datetime.timedelta(milliseconds=duration_in_ms)
    # }


# }


#   export const extractOffsetAndDirectionFromFormula = (formula = '') => {
def _extract_offset_and_direction_from_formula(formula: str = '') -> Tuple[Dict[str, int], str]:
    # const formulaOffset = formula.match(/[+-](.*?)[-](.*?)(\$now)(.*)/)[4];
    formula_offset = re.search(r'[+-](.*?)[-](.*?)(\$now)(.*)', formula).group(4)
    # const value = toNumber(formulaOffset.match(/(\d+)(\w+)/)[1]);
    value = int(re.search(r'(\d+)(\w+)', formula_offset).group(1))
    # const units = formulaOffset.match(/(\d+)(\w+)/)[2];
    units = re.search(r'(\d+)(\w+)', formula_offset).group(2)
    # const offset = { value, units };
    offset = {'value': value, 'units': units}
    #
    # const offsetDirection = formulaOffset.match(/[+-]/)[0] === '-' ? OFFSET_DIRECTION.PAST : OFFSET_DIRECTION.FUTURE;
    offset_direction = (OFFSET_DIRECTION.PAST if re.search(r'[+-]', formula_offset).group(0) == '-' else
                        OFFSET_DIRECTION.FUTURE)
    #
    # return [offset, offsetDirection];
    return offset, offset_direction
# };
