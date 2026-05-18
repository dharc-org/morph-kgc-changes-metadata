# -*- coding: utf-8 -*-
"""
Tests for Case 2: time-span date YARRRML generation.

Covers: field name extraction, separator logic, default separator,
range/no-range branches, and YARRRML structural validity.
"""

import json
import sys
import unittest
from pathlib import Path

import yaml  # PyYAML

sys.path.insert(0, str(Path(__file__).parent.parent))

from yarrrml_generate_obj import generate_yaml


class TestGenerateYamlFromMockJson(unittest.TestCase):
    """End-to-end: mock JSON → valid parseable YARRRML."""

    def setUp(self):
        json_path   = Path(__file__).parent / "input" / "sample_demo_2.json"
        data        = json.loads(json_path.read_text(encoding="utf-8"))
        self.row    = data["responses"][0]
        self.yaml_text = generate_yaml(self.row)
        self.parsed = yaml.safe_load(self.yaml_text)

    def test_output_is_valid_yaml(self):
        self.assertIsNotNone(self.parsed)

    def test_prefixes_present(self):
        self.assertIn("prefixes", self.parsed)
        self.assertIn("crm", self.parsed["prefixes"])
        self.assertIn("xsd", self.parsed["prefixes"])

    def test_mappings_key_present(self):
        self.assertIn("mappings", self.parsed)

    def test_mapping_block_name(self):
        self.assertIn("object_timespan_dates", self.parsed["mappings"])

    def test_field_name_in_output(self):
        self.assertIn("year_range", self.yaml_text)

    def test_separator_in_output(self):
        # Mock JSON specifies ';' as separator with custom separator = yes
        self.assertIn(";", self.yaml_text)

    def test_begin_and_end_predicates(self):
        self.assertIn("P82a_begin_of_begin", self.yaml_text)
        self.assertIn("P82b_end_of_end",     self.yaml_text)

    def test_datatype_datetime(self):
        self.assertIn("xsd:dateTime", self.yaml_text)


class TestDefaultSeparator(unittest.TestCase):
    """When range is not allowed, separator defaults to '-'."""

    def test_no_range_uses_dash(self):
        row = {
            "What's_the_field_name?":                    "Data",
            "can this field contain a range? [no]":      "Yes",
            "can this field contain a range? [yes]":     "No",
        }
        yaml_text = generate_yaml(row)
        self.assertIn('"-"', yaml_text)


class TestFallbackBehaviour(unittest.TestCase):
    """Empty row should not raise; all fields resolved via fallback."""

    def test_empty_row_produces_valid_yaml(self):
        yaml_text = generate_yaml({})
        parsed    = yaml.safe_load(yaml_text)
        self.assertIn("mappings", parsed)


class TestFieldNameInjection(unittest.TestCase):
    """Field name from JSON is correctly injected into the template."""

    def test_custom_field_name(self):
        row       = {"What's_the_field_name?": "my_custom_column"}
        yaml_text = generate_yaml(row)
        self.assertIn("my_custom_column", yaml_text)


if __name__ == "__main__":
    unittest.main()