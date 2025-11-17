import unittest
from pathlib import Path
import yaml


class MyTestCase(unittest.TestCase):
    def test_case_study_3(self):
        """Test that two YAML files are equal, ignoring comments."""

        # Define file paths
        output_file = Path("src/ask-kg/test/output/mapping_generated.yaml")
        expected_file = Path("src/ask-kg/test/expected_results/sample_demo_1_pro.yaml")

        # Check that both files exist
        self.assertTrue(output_file.exists(), f"Output file not found: {output_file}")
        self.assertTrue(expected_file.exists(), f"Expected file not found: {expected_file}")

        # Load YAML files (yaml.safe_load automatically ignores comments)
        with open(output_file, 'r', encoding='utf-8') as f:
            output_data = yaml.safe_load(f)

        with open(expected_file, 'r', encoding='utf-8') as f:
            expected_data = yaml.safe_load(f)

        # Compare the loaded data structures
        self.assertEqual(
            output_data,
            expected_data,
            msg="YAML files differ in content (excluding comments)"
        )

    def test_case_study_3_with_detailed_diff(self):
        """Test YAML equality with detailed difference reporting."""

        output_file = Path("src/ask-kg/test/output/mapping_generated.yaml")
        expected_file = Path("src/ask-kg/test/expected_results/sample_demo_1_pro.yaml")

        # Check files exist
        self.assertTrue(output_file.exists(), f"Output file not found: {output_file}")
        self.assertTrue(expected_file.exists(), f"Expected file not found: {expected_file}")

        # Load YAML files
        with open(output_file, 'r', encoding='utf-8') as f:
            output_data = yaml.safe_load(f)

        with open(expected_file, 'r', encoding='utf-8') as f:
            expected_data = yaml.safe_load(f)

        # Use assertDictEqual for better error messages (if root is a dict)
        if isinstance(output_data, dict) and isinstance(expected_data, dict):
            self.assertDictEqual(
                output_data,
                expected_data,
                msg="YAML files contain different data"
            )
        else:
            # Fallback for lists or other types
            self.assertEqual(
                output_data,
                expected_data,
                msg="YAML files contain different data"
            )


if __name__ == '__main__':
    unittest.main()