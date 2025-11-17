# -*- coding: utf-8 -*-
"""
Generate YAML mapping from LimeSurvey JSON.
Input:  src/ask-kg/test/input/sample_demo_1.json
Output: src/ask-kg/test/output/mapping_generated.yaml
"""

from pathlib import Path
import json
import re

INPUT_JSON = Path("src/ask-kg/test/input/sample_demo_1.json")
OUTPUT_YAML = Path("src/ask-kg/test/output/mapping_generated.yaml")

MAPPING_LABELS = {
    "electronic device": "dev",
    "responsible agent (institution)": "acr",
    "appellation, human-readable label (EXCEPT license names)": "apl",
    "digital model": "mdl",
    "item": "itm",
    "software": "sfw",
    "time span": "tsp",
    "action, procedural step executed": "act",
    "appellation, human-readable label (ONLY license names)": "apl-lic",
    "identifier": "idf",
    "work": "wrk",
    "expression": "exp",
    "title": "ttl",
    "subject": "sub",
    "manifestation": "mnf",
    "place": "plc",
    "collection": "col",
}

SURVEY_TO_LABEL = {
    "Electronic_Device": "electronic device",
    "Responsible_Agent_(person)": "responsible agent (institution)",
    "Human-readable_Label_(except_for_license's_appellation)": "appellation, human-readable label (EXCEPT license names)",
    "Human-readable_Label_(only_for_license's_appellation)": "appellation, human-readable label (ONLY license names)",
    "Digital_Model": "digital model",
    "Item": "item",
    "Software": "software",
    "Date_or_Timespan": "time span",
    "Action_or_procedural_step": "action, procedural step executed",
    "Identifier": "identifier",
    "Work": "work",
    "Expression": "expression",
    "Title": "title",
    "Manifestation": "manifestation",
    "Subject_(depicted_or_represented_in/by_the_resource)": "subject",
    "Place": "place",
    "Collection": "collection",
}

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

def find_separator_key(row: dict) -> str:
    """Find the separator field key regardless of escaped slash/spaces."""
    # Match both 'is\/are' and 'is/are', ignore other suffix details
    for k in row.keys():
        if k.startswith("Which_separator_character(s)_is") and "divide_the_values?" in k:
            return k
    raise KeyError("Separator field key not found.")

def parse_separator(row: dict) -> str:
    """Extract separators in order and join with '---' (e.g., '; & ' -> ';---&')."""
    key = find_separator_key(row)
    raw = str(row.get(key, ""))
    # Normalize NBSP and trim
    raw = raw.replace("\u00A0", " ").strip()
    # Extract each non-word, non-space symbol individually (keeps order)
    tokens = re.findall(r"[^\w\s]", raw)
    return "---".join(tokens) if tokens else ""

def pick_first_code(row: dict) -> str:
    """Pick first selected type and map to code."""
    prefix = "What_does_the_value_of_this_field_represent?_["
    for k, v in row.items():
        if k.startswith(prefix) and isinstance(v, str) and v.strip().lower() == "yes":
            label_key = k.split(prefix, 1)[-1].rstrip("]")
            label = SURVEY_TO_LABEL[label_key]
            return MAPPING_LABELS[label]
    raise RuntimeError("No selected value type found.")

def build_yaml(field_name: str, code: str, sep_string: str) -> str:
    prefixes_lines = ["prefixes:"]
    for k, v in PREFIXES:
        prefixes_lines.append(f'  {k}: "{v}"')
    prefixes_block = "\n".join(prefixes_lines)
    return f"""{prefixes_block}

mappings:
  acquisition_tools:
    sources:
      - 'sample_input_file.csv~csv'
    s:
      # step 2: normalise and convert IRIs concerning instruments used for *
      function: idlab-fn:normalize_and_convert_to_iri
      parameters:
        - parameter: idlab-fn:valueParams
          value:
            # step 1: separate tools
            function: idlab-fn:multiple_separator_split_explode
            parameters:
              - parameter: idlab-fn:list_param_string_sep
                value: "{sep_string}"
              - parameter: idlab-fn:valParam
                value: $({field_name})
        - parameter: idlab-fn:valueType
          value: "{code}"
        - parameter: idlab-fn:valueNum
          value: ""
      type: iri
"""

def main():
    data = json.loads(Path(INPUT_JSON).read_text(encoding="utf-8"))
    row = data["responses"][0]
    field_name = row["What's_the_field_name?"]
    sep_string = parse_separator(row)          # -> ";---&" per "; & "
    code = pick_first_code(row)                # -> "dev"
    yaml_text = build_yaml(field_name, code, sep_string)
    OUTPUT_YAML.parent.mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_YAML).write_text(yaml_text, encoding="utf-8")

if __name__ == "__main__":
    main()
