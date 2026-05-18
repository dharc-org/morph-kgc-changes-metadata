# -*- coding: utf-8 -*-
"""
Tests for Case 3: acquisition tool IRI YARRRML generation.

Verifies that the generated YAML matches the expected output file,
ignoring comments (PyYAML strips them on parse).
"""

import json
import sys
import unittest
from pathlib import Path

import yaml

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent))

from yarrrml_generate_pro import generate_yaml

INPUT_JSON    = HERE / "input"            / "sample_demo_1.json"
EXPECTED_YAML = HERE / "expected_results" / "sample_demo_1_pro.yaml"
OUTPUT_YAML   = HERE / "output"           / "mapping_generated.yaml"


class TestCaseStudy3(unittest.TestCase):

    def setUp(self):
        data     = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
        row      = data["responses"][0]
        yaml_str = generate_yaml(row)
        OUTPUT_YAML.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_YAML.write_text(yaml_str, encoding="utf-8")

    def test_case_study_3(self):
        """Generated YAML must equal the expected file (comments excluded)."""
        self.assertTrue(OUTPUT_YAML.exists(),   f"Output file not found: {OUTPUT_YAML}")
        self.assertTrue(EXPECTED_YAML.exists(), f"Expected file not found: {EXPECTED_YAML}")

        with OUTPUT_YAML.open(encoding="utf-8") as f:
            output = yaml.safe_load(f)
        with EXPECTED_YAML.open(encoding="utf-8") as f:
            expected = yaml.safe_load(f)

        self.assertEqual(output, expected)

    def test_case_study_3_with_detailed_diff(self):
        """Same check with assertDictEqual for clearer failure messages."""
        with OUTPUT_YAML.open(encoding="utf-8") as f:
            output = yaml.safe_load(f)
        with EXPECTED_YAML.open(encoding="utf-8") as f:
            expected = yaml.safe_load(f)

        if isinstance(output, dict) and isinstance(expected, dict):
            self.assertDictEqual(output, expected)
        else:
            self.assertEqual(output, expected)


if __name__ == "__main__":
    unittest.main()