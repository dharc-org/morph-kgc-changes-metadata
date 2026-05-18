# -*- coding: utf-8 -*-
"""
YARRRML generator for acquisition tool IRI triples (Case 3).

Public API
----------
generate_yaml(row) -> str
"""

import re
from utils import build_prefixes_block, find_key_by_fragments
from constants import MAPPING_LABELS, SURVEY_TO_LABEL


def _parse_separator(row: dict) -> str:
    """
    Locate the separator field and convert its value to the
    '---'-joined token format expected by multiple_separator_split_explode.

    Example: '; & ' -> ';---&'
    """
    for key in row:
        if (key.startswith("Which_separator_character(s)_is")
                and "divide_the_values?" in key):
            raw = str(row[key]).replace("\u00A0", " ").strip()
            tokens = re.findall(r"[^\w\s]", raw)
            return "---".join(tokens) if tokens else ""
    raise KeyError("Separator field key not found in JSON response.")


def _pick_type_code(row: dict) -> str:
    """
    Identify the first selected entity type and return its short code.

    LimeSurvey encodes each option as a separate boolean field with the
    pattern: 'What_does_the_value_of_this_field_represent?_[<Option>]'
    """
    prefix = "What_does_the_value_of_this_field_represent?_["
    for key, val in row.items():
        if key.startswith(prefix) and str(val).strip().lower() == "yes":
            option_key = key.split(prefix, 1)[-1].rstrip("]")
            if option_key not in SURVEY_TO_LABEL:
                raise ValueError(
                    f"Unrecognised entity type option: '{option_key}'. "
                    f"Update SURVEY_TO_LABEL in constants.py."
                )
            label = SURVEY_TO_LABEL[option_key]
            return MAPPING_LABELS[label]
    raise RuntimeError("No entity type was selected in the questionnaire response.")


def generate_yaml(row: dict) -> str:
    """
    Parse a LimeSurvey response dict and return a YARRRML mapping string
    for acquisition tool IRI generation with nested UDFs (Case 3).
    """
    field_name  = row["What's_the_field_name?"]
    sep_string  = _parse_separator(row)
    type_code   = _pick_type_code(row)
    prefixes    = build_prefixes_block()

    return f"""{prefixes}

mappings:
  acquisition_tools:
    sources:
      - 'sample_input_file.csv~csv'
    s:
      # step 2: normalise and convert IRIs for the acquisition instruments
      function: idlab-fn:normalize_and_convert_to_iri
      parameters:
        - parameter: idlab-fn:valueParams
          value:
            # step 1: split multi-valued cells
            function: idlab-fn:multiple_separator_split_explode
            parameters:
              - parameter: idlab-fn:list_param_string_sep
                value: "{sep_string}"
              - parameter: idlab-fn:valParam
                value: $({field_name})
        - parameter: idlab-fn:valueType
          value: "{type_code}"
        - parameter: idlab-fn:valueNum
          value: ""
      type: iri
"""