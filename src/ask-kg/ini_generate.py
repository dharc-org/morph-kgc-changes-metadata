# -*- coding: utf-8 -*-
"""
INI configuration generator (Case 1).

Public API
----------
generate_ini(row, template_ini_path) -> ConfigParser
write_ini(config, output_path) -> None
"""

import configparser
from pathlib import Path

from constants import DEFAULT_TEMPLATE_INI, SERIALIZATION_ALIASES
from utils import get_value, normalize_text

# ---------------------------------------------------------------------------
# Placeholders — replace with real LimeSurvey export keys when available.
# ---------------------------------------------------------------------------
JSON_KEYS: dict[str, str] = {
    "csv_path":          "__PLACEHOLDER__What's_the_path_of_the_input_dataset_CSV_file?",
    "mapping_path":      "__PLACEHOLDER__What's_the_path_of_the_mapping_file?",
    "output_file_path":  "__PLACEHOLDER__Where_should_the_output_file_be_generated?",
    "rdf_serialization": "__PLACEHOLDER__Which_RDF_serialisation_should_be_used?",
    "dataset_version":   "__PLACEHOLDER__What_is_the_version_number_of_the_output_dataset?",
    "project_iri_base":  "__PLACEHOLDER__What_is_the_base_path_of_the_generated_IRIs?",
}

FALLBACKS: dict[str, str] = {
    "csv_path":          "input/aldrovandi_obj.csv",
    "mapping_path":      "src/morph_kgc_changes_metadata_conversions/sample_mapping_file.yaml",
    "output_file_path":  "knowledge-graph_obj.ttl",
    "rdf_serialization": "turtle",
    "dataset_version":   "1",
    "project_iri_base":  "https://w3id.org/changes/4/aldrovandi/",
}


def _normalize_serialization(value: str) -> str:
    cleaned = normalize_text(value).replace(" ", "")
    return SERIALIZATION_ALIASES.get(cleaned, value.strip())


def _normalize_iri_base(value: str) -> str:
    cleaned = str(value).strip()
    if cleaned and not cleaned.endswith("/"):
        cleaned += "/"
    return cleaned


def _read_template(template_ini_path: Path | None) -> configparser.ConfigParser:
    config = configparser.ConfigParser(interpolation=None)
    config.optionxform = str
    if template_ini_path is not None and template_ini_path.exists():
        config.read(template_ini_path, encoding="utf-8")
    else:
        config.read_string(DEFAULT_TEMPLATE_INI)
    return config


def _first_datasource_section(config: configparser.ConfigParser) -> str:
    sections = [s for s in config.sections() if s.startswith("DataSource")]
    if sections:
        return sections[0]
    config.add_section("DataSource1")
    return "DataSource1"


def generate_ini(
    row: dict,
    template_ini_path: Path | None = None,
) -> configparser.ConfigParser:
    """
    Parse a LimeSurvey response dict and return a populated ConfigParser.

    Uses a three-level resolution strategy per field: exact key → fuzzy
    fragment match → hardcoded fallback. Fallbacks keep the script runnable
    while JSON_KEYS still contains placeholder strings.
    """
    config = _read_template(template_ini_path)

    csv_path = get_value(row, JSON_KEYS["csv_path"],
        ["input", "dataset", "csv", "path"], FALLBACKS["csv_path"], label="csv_path")
    mapping_path = get_value(row, JSON_KEYS["mapping_path"],
        ["mapping", "file", "path"], FALLBACKS["mapping_path"], label="mapping_path")
    output_file_path = get_value(row, JSON_KEYS["output_file_path"],
        ["output", "file", "generated"], FALLBACKS["output_file_path"], label="output_file_path")
    rdf_serialization = _normalize_serialization(get_value(
        row, JSON_KEYS["rdf_serialization"],
        ["rdf", "serialisation"], FALLBACKS["rdf_serialization"], label="rdf_serialization"))
    dataset_version = get_value(row, JSON_KEYS["dataset_version"],
        ["version", "dataset"], FALLBACKS["dataset_version"], label="dataset_version")
    project_iri_base = _normalize_iri_base(get_value(
        row, JSON_KEYS["project_iri_base"],
        ["base", "path", "iri"], FALLBACKS["project_iri_base"], label="project_iri_base"))

    if not config.has_section("CONFIGURATION"):
        config.add_section("CONFIGURATION")
    config["CONFIGURATION"]["output_serialization"] = rdf_serialization
    config["CONFIGURATION"]["project_iri_base"]     = project_iri_base
    config["CONFIGURATION"]["versione"]              = dataset_version

    ds = _first_datasource_section(config)
    config[ds]["mappings"]    = mapping_path
    config[ds]["file_path"]   = csv_path
    config[ds]["output_file"] = output_file_path

    return config


def write_ini(config: configparser.ConfigParser, output_path: Path) -> None:
    """Write config to output_path, creating parent directories if needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        config.write(fh)