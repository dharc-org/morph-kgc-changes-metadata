# -*- coding: utf-8 -*-
"""
Demo runner — Case 3: acquisition tool IRI YARRRML generation.

Thin wrapper around ``yarrrml_generate_pro.generate_yaml()``.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from yarrrml_generate_pro import generate_yaml

INPUT_JSON  = Path("src/ask-kg/test/input/sample_demo_1.json")
OUTPUT_YAML = Path("src/ask-kg/test/output/mapping_generated.yaml")


def main() -> None:
    data      = json.loads(INPUT_JSON.read_text(encoding="utf-8"))
    row       = data["responses"][0]
    yaml_text = generate_yaml(row)
    OUTPUT_YAML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_YAML.write_text(yaml_text, encoding="utf-8")
    print(f"Written: {OUTPUT_YAML}")


if __name__ == "__main__":
    main()