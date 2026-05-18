# -*- coding: utf-8 -*-
"""
YARRRML generator for time-span date triples (Case 2).

Public API
----------
generate_yaml(row) -> str
"""

from pathlib import Path
from utils import get_value, get_binary_answer, build_prefixes_block

# ---------------------------------------------------------------------------
# Placeholders — replace with real LimeSurvey export keys when available.
# ---------------------------------------------------------------------------
JSON_KEYS: dict[str, str] = {
    "field_name":           "What's_the_field_name?",
    "range_yes":            "__PLACEHOLDER__Can_this_field_contain_a_range?_[yes]",
    "range_no":             "__PLACEHOLDER__Can_this_field_contain_a_range?_[no]",
    "custom_separator_yes": "__PLACEHOLDER__Can_the_two_dates_be_separated_by_characters_other_than_-?_[yes]",
    "custom_separator_no":  "__PLACEHOLDER__Can_the_two_dates_be_separated_by_characters_other_than_-?_[no]",
    "separator_value":      "__PLACEHOLDER__Which_separator_character(s)_is/are_used_to_divide_the_two_dates?",
}

FALLBACKS: dict = {
    "field_name":              "Data",
    "range_allowed":           True,
    "custom_separator_allowed":False,
    "separator_value":         "-",
}


def _get_separator(row: dict, range_allowed: bool, custom_allowed: bool) -> str:
    if not range_allowed or not custom_allowed:
        return "-"
    return get_value(row, JSON_KEYS["separator_value"],
        ["separator", "divide", "dates"], FALLBACKS["separator_value"],
        label="separator_value")


def generate_yaml(row: dict) -> str:
    """
    Parse a LimeSurvey response dict and return a YARRRML mapping string
    for the E52 Time-Span begin/end date triples (Case 2).
    """
    field_name = get_value(row, JSON_KEYS["field_name"],
        ["field", "name"], FALLBACKS["field_name"], label="field_name")

    range_allowed = get_binary_answer(
        row,
        yes_key=JSON_KEYS["range_yes"], no_key=JSON_KEYS["range_no"],
        yes_fragments=["range", "yes"], no_fragments=["range", "no"],
        fallback=FALLBACKS["range_allowed"], label="range_allowed",
    )
    custom_allowed = get_binary_answer(
        row,
        yes_key=JSON_KEYS["custom_separator_yes"], no_key=JSON_KEYS["custom_separator_no"],
        yes_fragments=["separator", "other than", "yes"],
        no_fragments=["separator", "other than", "no"],
        fallback=FALLBACKS["custom_separator_allowed"], label="custom_separator_allowed",
    )
    separator = _get_separator(row, range_allowed, custom_allowed)

    prefixes = build_prefixes_block()
    return f"""{prefixes}

mappings:
  object_timespan_dates:
    sources:
      - 'sample_input_file.csv~csv'
    s:
      # step 1: generate the IRI of the time-span from the date field
      function: idlab-fn:normalize_and_convert_to_iri
      parameters:
        - parameter: idlab-fn:valueParams
          value: $({field_name})
        - parameter: idlab-fn:valueType
          value: "tsp"
        - parameter: idlab-fn:valueNum
          value: ""
      type: iri
    po:
      - [rdf:type, crm:E52_Time-Span]
      - p: crm:P82a_begin_of_begin
        o:
          # step 2: derive begin-of-begin from the date field
          function: idlab-fn:split_year_range_to_dates
          parameters:
            - parameter: idlab-fn:param_string_e
              value: $({field_name})
            - parameter: idlab-fn:param_position_e
              value: "start"
            - parameter: idlab-fn:list_param_string_sep
              value: "{separator}"
          datatype: xsd:dateTime
      - p: crm:P82b_end_of_end
        o:
          # step 3: derive end-of-end from the date field
          function: idlab-fn:split_year_range_to_dates
          parameters:
            - parameter: idlab-fn:param_string_e
              value: $({field_name})
            - parameter: idlab-fn:param_position_e
              value: "end"
            - parameter: idlab-fn:list_param_string_sep
              value: "{separator}"
          datatype: xsd:dateTime
"""