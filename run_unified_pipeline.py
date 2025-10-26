#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Orchestratore unificato:
  1) Legge src/morph_kgc_changes_metadata_conversions/config.ini
  2) Lancia i due script:
       - main_object_demo.py
       - main_process_demo.py
  3) Recupera i due TTL di output dai path indicati in config.ini
  4) Unisce i due grafi e salva un TTL unificato
  5) Legge il report JSON scritto dai due script e aggiunge il riepilogo 'overall'

Assunzioni:
  - Gli script di lancio usano PerfMonitor.update_json_report(...) per scrivere le metriche nel JSON condiviso.
  - Questo file non misura i tempi dei due script (responsabilità dei singoli script).
  - Può essere eseguito dalla root del repository; crea automaticamente le cartelle di output quando necessario.

Esecuzione (esempio):
  python run_unified_pipeline.py \
      --config src/morph_kgc_changes_metadata_conversions/config.ini \
      --report results/monitor/perf_report.json \
      --merged-out results/merged_graph_output.ttl
"""

from __future__ import annotations
import argparse
import configparser
import os
import subprocess
import sys

from rdflib import Graph
from perf_monitor import PerfMonitor

DEFAULT_CONFIG = "src/morph_kgc_changes_metadata_conversions/config.ini"
DEFAULT_REPORT = None  # se None, usa monitor_report/perf_report.json da config.ini
DEFAULT_MERGED_OUT = None  # se None, usa <output_dir>/merged_graph_output.ttl da config.ini

def read_paths_from_config(cfg_path: str):
    cfg = configparser.ConfigParser()
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg.read_file(f)

    out_dir = cfg.get("CONFIGURATION", "output_dir", fallback=None)
    monitor_report = cfg.get("CONFIGURATION", "monitor_report", fallback="results/monitor")
    ser = cfg.get("CONFIGURATION", "output_serialization", fallback="turtle").strip().lower()

    ds1_out = cfg.get("DataSource1", "output_file", fallback=None)
    ds2_out = cfg.get("DataSource2", "output_file", fallback=None)

    if not out_dir or not ds1_out or not ds2_out:
        raise RuntimeError(
            "Config incompleta: assicurarsi che 'output_dir' e 'output_file' siano presenti per DataSource1 e DataSource2."
        )

    def corrected(name: str) -> str:
        return (name[:-4] + "_corretto.ttl") if name.lower().endswith(".ttl") else (name + "_corretto.ttl")

    # Struttura di output allineata ai sottoprocessi
    obj_dir = os.path.join(out_dir, "object_dataset")
    pro_dir = os.path.join(out_dir, "process_dataset")

    ttl1 = os.path.join(obj_dir, corrected(ds1_out))  # es. results/object_dataset/knowledge-graph_obj_corretto.ttl
    ttl2 = os.path.join(pro_dir, corrected(ds2_out))  # es. results/process_dataset/knowledge-graph_pro_corretto.ttl

    fmt = "turtle" if ser == "turtle" else "turtle"
    return ttl1, ttl2, fmt, out_dir, monitor_report

def run_script(path: str) -> None:
    """Esegue uno script figlio in un processo separato."""
    print(f"[orchestrator] Avvio: {path}")
    result = subprocess.run([sys.executable, path], stdout=sys.stdout, stderr=sys.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Script {path} terminato con exit code {result.returncode}")
    print(f"[orchestrator] Completato: {path}")

def merge_graphs(ttl1: str, ttl2: str, merged_out: str, fmt_in: str = "turtle", fmt_out: str = "turtle") -> None:
    print(f"[orchestrator] Merge dei grafi:\n  - {ttl1}\n  - {ttl2}")
    g1 = Graph()
    g1.parse(ttl1, format=fmt_in)
    g2 = Graph()
    g2.parse(ttl2, format=fmt_in)

    merged = Graph()
    for t in g1:
        merged.add(t)
    for t in g2:
        merged.add(t)

    # Conserva i namespace
    for prefix, uri in set(g1.namespaces()) | set(g2.namespaces()):
        merged.bind(prefix, uri)

    os.makedirs(os.path.dirname(merged_out) or ".", exist_ok=True)
    merged.serialize(destination=merged_out, format=fmt_out)
    print(f"[orchestrator] Grafo unificato scritto in: {merged_out}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="Percorso al file config.ini")
    ap.add_argument("--report", default=DEFAULT_REPORT, help="Percorso al report JSON (se omesso, usa monitor_report da config.ini)")
    ap.add_argument(
        "--merged-out",
        default=DEFAULT_MERGED_OUT,
        help="Percorso TTL unificato di output (se omesso, usa output_dir da config.ini)"
    )
    ap.add_argument("--object-script", default="main_object_demo.py", help="Script di materializzazione oggetti")
    ap.add_argument("--process-script", default="main_process_demo.py", help="Script di materializzazione processo")
    args = ap.parse_args()

    # Lettura configurazioni e costruzione dei path deterministici
    ttl1, ttl2, fmt, out_dir, monitor_report = read_paths_from_config(args.config)

    # Percorso del report
    report_path = args.report or os.path.join(monitor_report, "perf_report.json")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    # Esecuzione dei sottoprocessi
    run_script(args.object_script)
    run_script(args.process_script)

    # Output del merge
    merged_out = args.merged_out or os.path.join(out_dir, "merged_graph_output.ttl")
    merge_graphs(ttl1, ttl2, merged_out, fmt_in="turtle", fmt_out="turtle")

    # Aggiornamento riepilogo complessivo
    print(f"[orchestrator] Aggiornamento riepilogo complessivo in: {report_path}")
    data = PerfMonitor.add_overall_summary(report_path, overall_key="overall")

    overall = data.get("overall", {})
    print("[orchestrator] OVERALL:")
    for k in (
        "elapsed_time_s",
        "peak_rss_mb",
        "rows_processed",
        "iri_generated",
        "materialisation_time_per_1000_rows_s",
        "iri_rate_per_s",
        "runs_count",
        "timestamp",
    ):
        print(f"  {k}: {overall.get(k)}")

if __name__ == "__main__":
    main()
