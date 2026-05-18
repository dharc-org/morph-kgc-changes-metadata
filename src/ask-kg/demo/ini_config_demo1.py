# -*- coding: utf-8 -*-
"""
Demo runner — Case 1: guided INI configuration generation.

This script is a thin wrapper around ``ini_generate.generate_ini()``.
All logic lives in the core module; only paths are defined here.
"""

import json
import sys
from pathlib import Path

# Allow imports from the package root when run directly.
sys.path.insert(0, str(Path(__file__).parent.parent))

from ini_generate import generate_ini, write_ini

INPUT_JSON   = Path("src/ask-kg/test/input/sample_demo_1_config.json")
TEMPLATE_INI = Path("src/ask-kg/demo/configuration_template.ini")
OUTPUT_INI   = Path("src/ask-kg/test/output/configuration_generated_demo1.ini")


def main() -> None:
    data   = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    row    = data["responses"][0]
    config = generate_ini(row, template_ini_path=TEMPLATE_INI)
    write_ini(config, OUTPUT_INI)
    print(f"Written: {OUTPUT_INI}")


if __name__ == "__main__":
    main()