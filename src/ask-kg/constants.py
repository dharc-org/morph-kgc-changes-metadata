# -*- coding: utf-8 -*-
"""
Shared constants for the CHAD-ASK plugin.

All values that were previously duplicated across demo scripts are
centralised here so that a single edit propagates everywhere.
"""

# ---------------------------------------------------------------------------
# RDF / YARRRML prefix declarations
# Used in every generated YARRRML mapping file.
# ---------------------------------------------------------------------------
PREFIXES: list[tuple[str, str]] = [
    ("aat",       "http://vocab.getty.edu/aat/"),
    ("ex",        "https://w3id.org/dharc/ontology/chad-ap/data/example/"),
    ("crm",       "http://www.cidoc-crm.org/cidoc-crm/"),
    ("crmdig",    "http://www.cidoc-crm.org/extensions/crmdig/"),
    ("lrmoo",     "http://iflastandards.info/ns/lrm/lrmoo/"),
    ("frbroo",    "http://iflastandards.info/ns/fr/frbr/frbroo/"),
    ("owl",       "http://www.w3.org/2002/07/owl#"),
    ("rdf",       "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("xml",       "http://www.w3.org/XML/1998/namespace"),
    ("xsd",       "http://www.w3.org/2001/XMLSchema#"),
    ("rdfs",      "http://www.w3.org/2000/01/rdf-schema#"),
    ("rml",       "http://w3id.org/rml/"),
    ("morph-kgc", "https://github.com/morph-kgc/morph-kgc/function/built-in.ttl#"),
    ("grel",      "http://users.ugent.be/~bjdmeest/function/grel.ttl#"),
    ("idlab-fn",  "http://example.com/idlab/function/"),
    ("idlab-fn-m","https://w3id.org/imec/idlab/function-mapping#"),
]

# ---------------------------------------------------------------------------
# Entity-type short codes used in IRI generation.
# Maps a human-readable label to the short code passed to
# idlab-fn:normalize_and_convert_to_iri as valueType.
# ---------------------------------------------------------------------------
MAPPING_LABELS: dict[str, str] = {
    "electronic device":                                          "dev",
    "responsible agent (institution)":                           "acr",
    "appellation, human-readable label (except license names)":  "apl",
    "appellation, human-readable label (only license names)":    "apl-lic",
    "digital model":                                             "mdl",
    "item":                                                      "itm",
    "software":                                                  "sfw",
    "time span":                                                 "tsp",
    "action, procedural step executed":                          "act",
    "identifier":                                                "idf",
    "work":                                                      "wrk",
    "expression":                                                "exp",
    "title":                                                     "ttl",
    "subject":                                                   "sub",
    "manifestation":                                             "mnf",
    "place":                                                     "plc",
    "collection":                                                "col",
}

# ---------------------------------------------------------------------------
# Maps LimeSurvey answer-option keys to the canonical MAPPING_LABELS keys.
# The LimeSurvey export encodes each option as a separate key whose suffix
# (inside square brackets) is one of the values below.
# ---------------------------------------------------------------------------
SURVEY_TO_LABEL: dict[str, str] = {
    "Electronic_Device":
        "electronic device",
    "Responsible_Agent_(person)\u00a0":
        "responsible agent (institution)",
    "Human-readable_Label_(except_for_license's_appellation)":
        "appellation, human-readable label (except license names)",
    "Human-readable_Label_(only_for_license's_appellation)":
        "appellation, human-readable label (only license names)",
    "Digital_Model":
        "digital model",
    "Item":
        "item",
    "Software":
        "software",
    "Date_or_Timespan":
        "time span",
    "Action_or_procedural_step":
        "action, procedural step executed",
    "Identifier":
        "identifier",
    "Work":
        "work",
    "Expression":
        "expression",
    "Title":
        "title",
    "Manifestation":
        "manifestation",
    "Subject_(depicted_or_represented_in\\/by_the_resource)":
        "subject",
    "Place":
        "place",
    "Collection":
        "collection",
}

# ---------------------------------------------------------------------------
# Normalise LimeSurvey serialisation labels to Morph-KGC accepted values.
# ---------------------------------------------------------------------------
SERIALIZATION_ALIASES: dict[str, str] = {
    "turtle":   "turtle",
    "ttl":      "turtle",
    "n-triples":"ntriples",
    "ntriples": "ntriples",
    "nt":       "ntriples",
    "json-ld":  "jsonld",
    "jsonld":   "jsonld",
    "rdf/xml":  "rdfxml",
    "rdfxml":   "rdfxml",
    "xml":      "rdfxml",
    "trig":     "trig",
    "n-quads":  "nquads",
    "nquads":   "nquads",
}

# ---------------------------------------------------------------------------
# Fallback INI template used when no precompiled template file is found.
# ---------------------------------------------------------------------------
DEFAULT_TEMPLATE_INI: str = """\
[CONFIGURATION]
na_values = ,,#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,nan,None,
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