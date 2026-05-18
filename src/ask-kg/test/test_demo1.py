# -*- coding: utf-8 -*-
"""
Tests for Case 1: INI configuration generation.

Covers: field extraction via fuzzy matching, serialisation normalisation,
IRI base trailing-slash enforcement, and fallback behaviour.
"""

import configparser
import json
import sys
import unittest
from pathlib import Path

# Ensure package root is importable.
sys.path.insert(0, str(Path(__file__).parent.parent))

from ini_generate import generate_ini


class TestGenerateIniFromMockJson(unittest.TestCase):
    """End-to-end test: real mock JSON → ConfigParser fields."""

    def setUp(self):
        json_path = Path(__file__).parent / "input" / "sample_demo_1_config.json"
        data      = json.loads(json_path.read_text(encoding="utf-8"))
        self.row  = data["responses"][0]
        self.config = generate_ini(self.row, template_ini_path=None)

    def test_configuration_section_exists(self):
        self.assertIn("CONFIGURATION", self.config.sections())

    def test_serialization_normalised(self):
        self.assertEqual(self.config["CONFIGURATION"]["output_serialization"], "turtle")

    def test_iri_base_has_trailing_slash(self):
        base = self.config["CONFIGURATION"]["project_iri_base"]
        self.assertTrue(base.endswith("/"),
            f"IRI base should end with '/': {base!r}")

    def test_iri_base_value(self):
        self.assertEqual(
            self.config["CONFIGURATION"]["project_iri_base"],
            "https://example.org/test/",
        )

    def test_version(self):
        self.assertEqual(self.config["CONFIGURATION"]["versione"], "2")

    def test_datasource_csv_path(self):
        ds = next(s for s in self.config.sections() if s.startswith("DataSource"))
        self.assertEqual(self.config[ds]["file_path"], "input/test_objects.csv")

    def test_datasource_mapping_path(self):
        ds = next(s for s in self.config.sections() if s.startswith("DataSource"))
        self.assertEqual(self.config[ds]["mappings"], "mappings/test_mapping.yaml")

    def test_datasource_output_file(self):
        ds = next(s for s in self.config.sections() if s.startswith("DataSource"))
        self.assertEqual(self.config[ds]["output_file"], "output/test_graph.ttl")


class TestIriBaseNormalisation(unittest.TestCase):
    """Unit tests for IRI base trailing-slash logic."""

    def _run(self, iri_value: str) -> str:
        row    = {"base path iri": iri_value}
        config = generate_ini(row, template_ini_path=None)
        return config["CONFIGURATION"]["project_iri_base"]

    def test_adds_slash_when_missing(self):
        result = self._run("https://example.org/test")
        self.assertTrue(result.endswith("/"))

    def test_preserves_existing_slash(self):
        result = self._run("https://example.org/test/")
        self.assertEqual(result.count("//"), 1)
        self.assertTrue(result.endswith("/"))


class TestSerializationAliases(unittest.TestCase):
    """Unit tests for serialisation label normalisation."""

    def _serialization(self, label: str) -> str:
        row    = {"rdf serialisation": label}
        config = generate_ini(row, template_ini_path=None)
        return config["CONFIGURATION"]["output_serialization"]

    def test_turtle_lowercase(self):
        self.assertEqual(self._serialization("turtle"), "turtle")

    def test_ttl_alias(self):
        self.assertEqual(self._serialization("ttl"), "turtle")

    def test_ntriples_hyphen(self):
        self.assertEqual(self._serialization("n-triples"), "ntriples")

    def test_jsonld_hyphen(self):
        self.assertEqual(self._serialization("json-ld"), "jsonld")


class TestFallbackBehaviour(unittest.TestCase):
    """Fallbacks are used when the JSON contains no matching keys."""

    def test_empty_row_uses_fallbacks(self):
        config = generate_ini({}, template_ini_path=None)
        # Should not raise; all fields resolved via fallback.
        self.assertIn("CONFIGURATION", config.sections())
        self.assertEqual(
            config["CONFIGURATION"]["project_iri_base"],
            "https://w3id.org/changes/4/aldrovandi/",
        )


if __name__ == "__main__":
    unittest.main()