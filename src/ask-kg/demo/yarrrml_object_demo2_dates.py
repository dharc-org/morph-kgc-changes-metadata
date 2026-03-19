# -*- coding: utf-8 -*-
"""
Generate YAML mapping from LimeSurvey JSON for Case 2:
time-span-related triples generation.

This demo assumes that the questionnaire provides enough information to:
1. identify the CSV column containing the date or year information;
2. state whether the field may contain a single date only or also a range;
3. optionally state whether range separators other than '-' are used;
4. optionally provide the separator character(s) used for ranges.

Input:  src/ask-kg/test/input/sample_demo_2.json
Output: src/ask-kg/test/output/mapping_generated_demo2_dates.yaml

Notes
-----
- The script is intentionally written with placeholders because the real
  LimeSurvey JSON export for demo 2 is not yet available.
- Unlike demo 3, the separator string for split_year_range_to_dates must be
  passed in its raw questionnaire form (e.g. '; & '), because the UDF itself
  extracts and normalises the separators.
- The mapping generated here focuses on the E52 Time-Span fragment and on the
  begin/end date triples. The integration with the broader object mapping is
  expected to happen later in the main codebase.
"""

from pathlib import Path
import json
import unicodedata
import re

INPUT_JSON = Path("src/ask-kg/test/input/sample_demo_2.json")
OUTPUT_YAML = Path("src/ask-kg/test/output/mapping_generated_demo2_dates.yaml")

PREFIXES = [
    ('aat', "http://vocab.getty.edu/aat/"),
    ('ex', "https://w3id.org/dharc/ontology/chad-ap/data/example/"),
    ('crm', "http://www.cidoc-crm.org/cidoc-crm/"),
    ('crmdig', "http://www.cidoc-crm.org/extensions/crmdig/"),
    ('lrmoo', "http://iflastandards.info/ns/lrm/lrmoo/"),
    ('frbroo', "http://iflastandards.info/ns/fr/frbr/frbroo/"),
    ('owl', "http://www.w3.org/2002/07/owl#"),
    ('rdf', "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ('xml', "http://www.w3.org/XML/1998/namespace"),
    ('xsd', "http://www.w3.org/2001/XMLSchema#"),
    ('rdfs', "http://www.w3.org/2000/01/rdf-schema#"),
    ('rml', "http://w3id.org/rml/"),
    ('morph-kgc', "https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#"),
    ('grel', "http://users.ugent.be/~bjdmeest/function/grel.ttl#"),
    ('idlab-fn', "http://example.com/idlab/function/"),
    ('idlab-fn-m', "https://w3id.org/imec/idlab/function-mapping#"),
]

# ---------------------------------------------------------------------------
# Placeholders to replace once the real LimeSurvey export structure is known.
# ---------------------------------------------------------------------------
JSON_KEYS = {
    # This one is already likely correct based on demo 3.
    "field_name": "What's_the_field_name?",

    # Placeholder keys for the real demo 2 questionnaire export.
    "range_yes": "__PLACEHOLDER__Can_this_field_contain_a_range?_[yes]",
    "range_no": "__PLACEHOLDER__Can_this_field_contain_a_range?_[no]",
    "custom_separator_yes": "__PLACEHOLDER__Can_the_two_dates_be_separated_by_characters_other_than_-?_[yes]",
    "custom_separator_no": "__PLACEHOLDER__Can_the_two_dates_be_separated_by_characters_other_than_-?_[no]",
    "separator_value": "__PLACEHOLDER__Which_separator_character(s)_is/are_used_to_divide_the_two_dates?",
}

# ---------------------------------------------------------------------------
# Fallback values used only while the real JSON export is not available.
# These make the script executable even with a minimal mock JSON.
# ---------------------------------------------------------------------------
USE_FALLBACKS_IF_KEYS_ARE_MISSING = True
FALLBACKS = {
    "field_name": "Data",
    "range_allowed": True,
    "custom_separator_allowed": False,
    "separator_value": "-",
}


def normalize_text(value: str) -> str:
    """Normalize text for soft key matching."""
    value = unicodedata.normalize("NFKC", str(value))
    value = value.replace("\\/", "/")
    value = value.replace("\u00A0", " ")
    value = value.lower().strip()
    value = re.sub(r"\s+", " ", value)
    return value


def value_is_yes(value) -> bool:
    return str(value).strip().lower() in {"yes", "true", "1", "y"}


def find_key_by_fragments(row: dict, required_fragments: list[str]) -> str | None:
    """Return the first key whose normalized form contains all fragments."""
    normalized_targets = [normalize_text(fragment) for fragment in required_fragments]
    for key in row.keys():
        nk = normalize_text(key)
        if all(fragment in nk for fragment in normalized_targets):
            return key
    return None


def get_field_name(row: dict) -> str:
    exact_key = JSON_KEYS["field_name"]
    if exact_key in row:
        return str(row[exact_key]).strip()

    fuzzy_key = find_key_by_fragments(row, ["field", "name"])
    if fuzzy_key:
        return str(row[fuzzy_key]).strip()

    if USE_FALLBACKS_IF_KEYS_ARE_MISSING:
        return FALLBACKS["field_name"]

    raise KeyError(
        "Field name key not found. Update JSON_KEYS['field_name'] with the real LimeSurvey export key."
    )


def get_binary_answer(row: dict, yes_key_name: str, no_key_name: str, question_label: str, yes_fragments: list[str], no_fragments: list[str], fallback: bool):
    """
    Read a Yes/No answer from two LimeSurvey export keys.
    Returns True if the *_yes key is selected, False if the *_no key is selected.
    """
    yes_key = JSON_KEYS[yes_key_name]
    no_key = JSON_KEYS[no_key_name]

    if yes_key in row and value_is_yes(row[yes_key]):
        return True
    if no_key in row and value_is_yes(row[no_key]):
        return False

    fuzzy_yes_key = find_key_by_fragments(row, yes_fragments)
    if fuzzy_yes_key and value_is_yes(row[fuzzy_yes_key]):
        return True

    fuzzy_no_key = find_key_by_fragments(row, no_fragments)
    if fuzzy_no_key and value_is_yes(row[fuzzy_no_key]):
        return False

    if USE_FALLBACKS_IF_KEYS_ARE_MISSING:
        return fallback

    raise KeyError(
        f"Could not determine the answer for '{question_label}'. "
        f"Update JSON_KEYS['{yes_key_name}'] and JSON_KEYS['{no_key_name}']."
    )


def get_range_allowed(row: dict) -> bool:
    return get_binary_answer(
        row=row,
        yes_key_name="range_yes",
        no_key_name="range_no",
        question_label="whether the field can contain a range",
        yes_fragments=["range", "yes"],
        no_fragments=["range", "no"],
        fallback=FALLBACKS["range_allowed"],
    )


def get_custom_separator_allowed(row: dict) -> bool:
    return get_binary_answer(
        row=row,
        yes_key_name="custom_separator_yes",
        no_key_name="custom_separator_no",
        question_label="whether custom separators other than '-' are used",
        yes_fragments=["separator", "other than", "yes"],
        no_fragments=["separator", "other than", "no"],
        fallback=FALLBACKS["custom_separator_allowed"],
    )


def get_separator_value(row: dict, range_allowed: bool, custom_separator_allowed: bool) -> str:
    """
    Return the raw separator string to pass to split_year_range_to_dates.

    Important: here we keep the raw questionnaire value (e.g. '; & '), because
    the UDF itself extracts and normalises the separator tokens.
    """
    if not range_allowed:
        return "-"

    if not custom_separator_allowed:
        return "-"

    exact_key = JSON_KEYS["separator_value"]
    if exact_key in row:
        return str(row[exact_key]).strip()

    fuzzy_key = find_key_by_fragments(row, ["separator", "divide", "dates"])
    if fuzzy_key:
        return str(row[fuzzy_key]).strip()

    if USE_FALLBACKS_IF_KEYS_ARE_MISSING:
        return FALLBACKS["separator_value"]

    raise KeyError(
        "Separator value key not found. Update JSON_KEYS['separator_value'] with the real LimeSurvey export key."
    )


def build_yaml(field_name: str, separator_string: str) -> str:
    prefixes_lines = ["prefixes:"]
    for k, v in PREFIXES:
        prefixes_lines.append(f'  {k}: "{v}"')
    prefixes_block = "\n".join(prefixes_lines)

    return f"""{prefixes_block}

mappings:
  object_timespan_dates:
    sources:
      - 'sample_input_file.csv~csv'
    s:
      # step 1: generate the IRI of the time-span directly from the date field
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
          # step 2: derive the begin of begin directly from the date field
          function: idlab-fn:split_year_range_to_dates
          parameters:
            - parameter: idlab-fn:param_string_e
              value: $({field_name})
            - parameter: idlab-fn:param_position_e
              value: "start"
            - parameter: idlab-fn:list_param_string_sep
              value: "{separator_string}"
          datatype: xsd:dateTime
      - p: crm:P82b_end_of_end
        o:
          # step 3: derive the end of end directly from the date field
          function: idlab-fn:split_year_range_to_dates
          parameters:
            - parameter: idlab-fn:param_string_e
              value: $({field_name})
            - parameter: idlab-fn:param_position_e
              value: "end"
            - parameter: idlab-fn:list_param_string_sep
              value: "{separator_string}"
          datatype: xsd:dateTime
"""


def main():
    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    row = data["responses"][0]

    field_name = get_field_name(row)
    range_allowed = get_range_allowed(row)
    custom_separator_allowed = get_custom_separator_allowed(row)
    separator_string = get_separator_value(row, range_allowed, custom_separator_allowed)

    yaml_text = build_yaml(field_name, separator_string)
    OUTPUT_YAML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_YAML.write_text(yaml_text, encoding="utf-8")


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# PLACEHOLDERS USED IN THIS SCRIPT
# ---------------------------------------------------------------------------
# 1. INPUT/OUTPUT PATH PLACEHOLDERS
#    - INPUT_JSON  = "src/ask-kg/test/input/sample_demo_2.json"
#    - OUTPUT_YAML = "src/ask-kg/test/output/mapping_generated_demo2_dates.yaml"
#
# 2. JSON KEY PLACEHOLDERS TO REPLACE WHEN THE REAL Limesurvey EXPORT EXISTS
#    - JSON_KEYS["range_yes"]
#    - JSON_KEYS["range_no"]
#    - JSON_KEYS["custom_separator_yes"]
#    - JSON_KEYS["custom_separator_no"]
#    - JSON_KEYS["separator_value"]
#
# 3. TEMPORARY FALLBACK PLACEHOLDERS USED ONLY UNTIL THE REAL EXPORT EXISTS
#    - FALLBACKS["field_name"] = "Data"
#    - FALLBACKS["range_allowed"] = True
#    - FALLBACKS["custom_separator_allowed"] = False
#    - FALLBACKS["separator_value"] = "-"
#
# 4. MAPPING TEMPLATE PLACEHOLDERS TO REVIEW DURING INTEGRATION
#    - mapping name: object_timespan_dates
#    - source file:  sample_input_file.csv~csv
#    - subject IRI generation strategy for the E52 Time-Span node
#    - datatype xsd:dateTime for start/end values
#      (review this if BCE dates or years > 9999 are expected, because the UDF
#       may emit EDTF-like long years such as Y-0500-01-01 instead)
# ---------------------------------------------------------------------------
