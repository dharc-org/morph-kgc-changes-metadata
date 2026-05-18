# -*- coding: utf-8 -*-
"""
CHAD-ASK orchestrator — entry point for the full pipeline.

Usage
-----
    python -m ask_kg --input responses.json --output-dir out/ [--template-ini template.ini]

The script reads a single LimeSurvey JSON export and runs all three
generators in sequence, writing their outputs to --output-dir:

    out/configuration.ini
    out/mapping_dates.yaml
    out/mapping_iris.yaml

Generators that cannot find their required keys in the JSON fall back to
hardcoded defaults, so a partial questionnaire export still produces
runnable artefacts.
"""

import argparse
import json
import sys
from pathlib import Path

# Ensure the package root is on sys.path when invoked as a script.
sys.path.insert(0, str(Path(__file__).parent))

import ini_generate
import yarrrml_generate_obj
import yarrrml_generate_pro


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ask_kg",
        description="Generate Morph-KGC artefacts from a LimeSurvey JSON export.",
    )
    parser.add_argument(
        "--input", required=True, type=Path,
        help="Path to the LimeSurvey JSON export file.",
    )
    parser.add_argument(
        "--output-dir", required=True, type=Path, dest="output_dir",
        help="Directory where generated artefacts will be written.",
    )
    parser.add_argument(
        "--template-ini", type=Path, default=None, dest="template_ini",
        help="Optional path to a precompiled Morph-KGC .ini template.",
    )
    return parser.parse_args(argv)


def run(
    input_path: Path,
    output_dir: Path,
    template_ini: Path | None = None,
) -> None:
    """
    Execute all three generators and write outputs to *output_dir*.

    Parameters
    ----------
    input_path:
        LimeSurvey JSON export (expects ``{"responses": [{...}]}``)
    output_dir:
        Directory for generated files (created if absent).
    template_ini:
        Optional precompiled ``.ini`` template for the INI generator.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    data = json.loads(input_path.read_text(encoding="utf-8"))
    row  = data["responses"][0]

    # -- Case 1: INI configuration -----------------------------------------
    config = ini_generate.generate_ini(row, template_ini_path=template_ini)
    ini_out = output_dir / "configuration.ini"
    ini_generate.write_ini(config, ini_out)
    print(f"[ok] INI configuration  -> {ini_out}")

    # -- Case 2: dates / time-span YARRRML ---------------------------------
    yaml_dates = yarrrml_generate_obj.generate_yaml(row)
    dates_out  = output_dir / "mapping_dates.yaml"
    dates_out.write_text(yaml_dates, encoding="utf-8")
    print(f"[ok] Dates YARRRML      -> {dates_out}")

    # -- Case 3: IRI generation YARRRML ------------------------------------
    try:
        yaml_iris = yarrrml_generate_pro.generate_yaml(row)
        iris_out  = output_dir / "mapping_iris.yaml"
        iris_out.write_text(yaml_iris, encoding="utf-8")
        print(f"[ok] IRI YARRRML        -> {iris_out}")
    except (KeyError, RuntimeError) as exc:
        # Case 3 requires specific fields (entity type selection) that may
        # not be present in every questionnaire export.
        print(f"[skip] IRI YARRRML: {exc}", file=sys.stderr)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    run(
        input_path=args.input,
        output_dir=args.output_dir,
        template_ini=args.template_ini,
    )


if __name__ == "__main__":
    main()