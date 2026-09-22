"""Valida un grafo RDF prodotto dalla pipeline (results/merged_graph_output*.ttl)
con le shape SHACL estratte da CHAD-AP. Wrapper minimale attorno a
src/morph_kgc_changes_metadata_conversions/shacl_validator/cli.py, coerente
con gli altri script eseguibili dalla root (main_object_demo.py, ecc.).

Esempi:
    python3 run_shacl_validation.py
    python3 run_shacl_validation.py --data-source results/merged_graph_output_capellini.ttl
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from morph_kgc_changes_metadata_conversions.shacl_validator.cli import main

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args or args[0] not in {"extract", "validate", "run"}:
        # nessun sottocomando esplicito: valida con gli output di default,
        # passando eventuali argomenti extra (es. --data-source) cosi' come sono
        args = [
            "validate",
            "--summary-json", "results/quality/shacl_summary.json",
            "--text-report", "results/quality/shacl_report.txt",
            "--independent-issues-json", "results/quality/shacl_independent_issues.json",
            "--independent-issues-text", "results/quality/shacl_independent_issues.txt",
        ] + args
    sys.argv = [sys.argv[0]] + args
    main()
