# -*- coding: utf-8 -*-
"""
Generate or update a configuration.ini file from LimeSurvey JSON for Case 1:
guided overwriting of a minimal subset of configuration parameters.

This demo assumes that the questionnaire provides enough information to:
1. identify the CSV input dataset path;
2. identify the YARRRML mapping file path;
3. identify the output RDF file path;
4. choose the RDF serialisation;
5. provide the output dataset version number;
6. provide the base IRI path to be used in the configuration.

Input:  src/ask-kg/test/input/sample_demo_1_config.json
Output: src/ask-kg/test/output/configuration_generated_demo1.ini

Notes
-----
- The script is intentionally written with placeholders because the real
  LimeSurvey JSON export for demo 1 is not yet available.
- The script is designed to overwrite only a minimal subset of parameters in a
  precompiled configuration file, leaving all the remaining settings unchanged.
- If the template INI file is not found, an embedded fallback template is used,
  based on the example structure currently available.
"""

from pathlib import Path
import json
import configparser
import unicodedata
import re

INPUT_JSON = Path("src/ask-kg/test/input/sample_demo_1_config.json")
TEMPLATE_INI = Path("src/ask-kg/demo/configuration_template.ini")
OUTPUT_INI = Path("src/ask-kg/test/output/configuration_generated_demo1.ini")

DEFAULT_TEMPLATE_INI = """[CONFIGURATION]
na_values = ,,#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,nan,None,""
output_dir = results
monitor_report = results/monitor
quality_report = results/quality
output_format = N-TRIPLES
output_serialization = turtle
only_printable_characters = no
safe_percent_encoding =
mapping_partitioning = PARTIAL-AGGREGATIONS
infer_sql_datatypes = no
logging_level = INFO
logs_file =
oracle_client_lib_dir =
oracle_client_config_dir =
project_iri_base = https://w3id.org/changes/4/aldrovandi/
versione = 1

[DataSource1]
mappings = src/morph_kgc_changes_metadata_conversions/sample_mapping_file.yaml
mapping_format = YARRRML
file_path = input/aldrovandi_obj.csv
ready_input_dir = input/ready_to_convert
output_file = knowledge-graph_obj.ttl
delimiter = ,
quotechar = "
encoding = utf-8

[QUALITY]
http_timeout = 5
max_links = 200
sample_size = 25
link_namespaces = vocab.getty.edu,viaf.org
"""

# ---------------------------------------------------------------------------
# Placeholders to replace once the real LimeSurvey export structure is known.
# ---------------------------------------------------------------------------
JSON_KEYS = {
    # Preferred exact keys for the future LimeSurvey export.
    "csv_path": "__PLACEHOLDER__What's_the_path_of_the_input_dataset_CSV_file?",
    "mapping_path": "__PLACEHOLDER__What's_the_path_of_the_mapping_file?",
    "output_file_path": "__PLACEHOLDER__Where_should_the_output_file_be_generated?",
    "rdf_serialization": "__PLACEHOLDER__Which_RDF_serialisation_should_be_used?",
    "dataset_version": "__PLACEHOLDER__What_is_the_version_number_of_the_output_dataset?",
    "project_iri_base": "__PLACEHOLDER__What_is_the_base_path_of_the_generated_IRIs?",
}

# ---------------------------------------------------------------------------
# Fallback values used only while the real JSON export is not available.
# These make the script executable even with a minimal mock JSON.
# ---------------------------------------------------------------------------
USE_FALLBACKS_IF_KEYS_ARE_MISSING = True
FALLBACKS = {
    "csv_path": "input/aldrovandi_obj.csv",
    "mapping_path": "src/morph_kgc_changes_metadata_conversions/sample_mapping_file.yaml",
    "output_file_path": "knowledge-graph_obj.ttl",
    "rdf_serialization": "turtle",
    "dataset_version": "1",
    "project_iri_base": "https://w3id.org/changes/4/aldrovandi/",
}

SERIALIZATION_ALIASES = {
    "turtle": "turtle",
    "ttl": "turtle",
    "n-triples": "ntriples",
    "ntriples": "ntriples",
    "nt": "ntriples",
    "json-ld": "jsonld",
    "jsonld": "jsonld",
    "rdf/xml": "rdfxml",
    "rdfxml": "rdfxml",
    "xml": "rdfxml",
    "trig": "trig",
    "n-quads": "nquads",
    "nquads": "nquads",
}


def normalize_text(value: str) -> str:
    """Normalize text for soft key matching."""
    value = unicodedata.normalize("NFKC", str(value))
    value = value.replace("\\/", "/")
    value = value.replace("\u00A0", " ")
    value = value.lower().strip()
    value = re.sub(r"\s+", " ", value)
    return value



def find_key_by_fragments(row: dict, required_fragments: list[str]) -> str | None:
    """Return the first key whose normalized form contains all fragments."""
    normalized_targets = [normalize_text(fragment) for fragment in required_fragments]
    for key in row.keys():
        nk = normalize_text(key)
        if all(fragment in nk for fragment in normalized_targets):
            return key
    return None



def get_value(row: dict, key_name: str, fragments: list[str], fallback_key: str) -> str:
    """Get a questionnaire value via exact key, fuzzy key, or fallback."""
    exact_key = JSON_KEYS[key_name]
    if exact_key in row:
        return str(row[exact_key]).strip()

    fuzzy_key = find_key_by_fragments(row, fragments)
    if fuzzy_key:
        return str(row[fuzzy_key]).strip()

    if USE_FALLBACKS_IF_KEYS_ARE_MISSING:
        return FALLBACKS[fallback_key]

    raise KeyError(
        f"Could not resolve the value for '{key_name}'. "
        f"Update JSON_KEYS['{key_name}'] with the real LimeSurvey export key."
    )



def normalize_serialization(value: str) -> str:
    """
    Normalize the RDF serialisation label to a Morph-KGC-compatible value.
    Unknown values are preserved as-is after whitespace trimming.
    """
    cleaned = normalize_text(value).replace(" ", "")
    return SERIALIZATION_ALIASES.get(cleaned, value.strip())



def normalize_project_iri_base(value: str) -> str:
    """Ensure that the project IRI base ends with a slash."""
    cleaned = str(value).strip()
    if cleaned and not cleaned.endswith("/"):
        cleaned += "/"
    return cleaned



def read_template_config() -> configparser.ConfigParser:
    """Load the precompiled configuration template or use the fallback one."""
    config = configparser.ConfigParser(interpolation=None)
    config.optionxform = str

    if TEMPLATE_INI.exists():
        config.read(TEMPLATE_INI, encoding="utf-8")
    else:
        config.read_string(DEFAULT_TEMPLATE_INI)

    return config



def get_first_datasource_section(config: configparser.ConfigParser) -> str:
    """
    Return the first existing [DataSourceX] section.
    If none exists, create and return [DataSource1].
    """
    datasource_sections = [s for s in config.sections() if s.startswith("DataSource")]
    if datasource_sections:
        return datasource_sections[0]

    section_name = "DataSource1"
    config.add_section(section_name)
    return section_name



def overwrite_minimal_subset(config: configparser.ConfigParser, row: dict) -> configparser.ConfigParser:
    """Overwrite only the minimal subset of configuration parameters required by Case 1."""
    csv_path = get_value(
        row=row,
        key_name="csv_path",
        fragments=["input", "dataset", "csv", "path"],
        fallback_key="csv_path",
    )
    mapping_path = get_value(
        row=row,
        key_name="mapping_path",
        fragments=["mapping", "file", "path"],
        fallback_key="mapping_path",
    )
    output_file_path = get_value(
        row=row,
        key_name="output_file_path",
        fragments=["output", "file", "generated"],
        fallback_key="output_file_path",
    )
    rdf_serialization = normalize_serialization(
        get_value(
            row=row,
            key_name="rdf_serialization",
            fragments=["rdf", "serialisation"],
            fallback_key="rdf_serialization",
        )
    )
    dataset_version = get_value(
        row=row,
        key_name="dataset_version",
        fragments=["version", "dataset"],
        fallback_key="dataset_version",
    )
    project_iri_base = normalize_project_iri_base(
        get_value(
            row=row,
            key_name="project_iri_base",
            fragments=["base", "path", "iri"],
            fallback_key="project_iri_base",
        )
    )

    if not config.has_section("CONFIGURATION"):
        config.add_section("CONFIGURATION")

    # Minimal subset to overwrite in [CONFIGURATION].
    config["CONFIGURATION"]["output_serialization"] = rdf_serialization
    config["CONFIGURATION"]["project_iri_base"] = project_iri_base
    config["CONFIGURATION"]["versione"] = dataset_version

    # Minimal subset to overwrite in the first available data source section.
    datasource_section = get_first_datasource_section(config)
    config[datasource_section]["mappings"] = mapping_path
    config[datasource_section]["file_path"] = csv_path
    config[datasource_section]["output_file"] = output_file_path

    return config



def write_config(config: configparser.ConfigParser, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        config.write(f)



def main():
    data = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    row = data["responses"][0]

    config = read_template_config()
    config = overwrite_minimal_subset(config, row)
    write_config(config, OUTPUT_INI)


if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------
# PLACEHOLDERS USED IN THIS SCRIPT
# ---------------------------------------------------------------------------
# 1. INPUT/OUTPUT PATH PLACEHOLDERS
#    - INPUT_JSON   = "src/ask-kg/test/input/sample_demo_1_config.json"
#    - TEMPLATE_INI = "src/ask-kg/demo/configuration_template.ini"
#    - OUTPUT_INI   = "src/ask-kg/test/output/configuration_generated_demo1.ini"
#
# 2. JSON KEY PLACEHOLDERS TO REPLACE WHEN THE REAL LimeSurvey EXPORT EXISTS
#    - JSON_KEYS["csv_path"]
#    - JSON_KEYS["mapping_path"]
#    - JSON_KEYS["output_file_path"]
#    - JSON_KEYS["rdf_serialization"]
#    - JSON_KEYS["dataset_version"]
#    - JSON_KEYS["project_iri_base"]
#
# 3. TEMPORARY FALLBACK PLACEHOLDERS USED ONLY UNTIL THE REAL EXPORT EXISTS
#    - FALLBACKS["csv_path"]
#    - FALLBACKS["mapping_path"]
#    - FALLBACKS["output_file_path"]
#    - FALLBACKS["rdf_serialization"]
#    - FALLBACKS["dataset_version"]
#    - FALLBACKS["project_iri_base"]
#
# 4. CURRENT INTEGRATION ASSUMPTIONS TO REVIEW LATER
#    - The questionnaire currently updates only the first available DataSource
#      section, i.e. [DataSource1] if present.
#    - The fields intentionally overwritten are:
#        [CONFIGURATION] -> output_serialization, project_iri_base, versione
#        [DataSourceX]   -> mappings, file_path, output_file
#    - All other fields in the template INI are preserved unchanged.
#    - If multiple input CSV/mapping pairs must be supported later, the JSON
#      structure and the overwrite logic should be extended accordingly.
# ---------------------------------------------------------------------------
