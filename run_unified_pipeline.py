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
import re

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import XSD
from perf_monitor import PerfMonitor

# --- rdflib: disabilita cast automatico di xsd:dateTime (e opzionalmente xsd:date) ---
try:
    from rdflib.term import _toPythonMapping
    from rdflib.namespace import XSD
    _toPythonMapping.pop(XSD.dateTime, None)
    _toPythonMapping.pop(XSD.date, None)  # se comparisse xsd:date
    print("[INFO] rdflib datetime cast disabled in this process")
except Exception as e:
    print(f"[WARN] rdflib datetime cast patch skipped: {e}")


DEFAULT_CONFIG = "src/morph_kgc_changes_metadata_conversions/config.ini"
DEFAULT_REPORT = None  # se None, usa monitor_report/perf_report.json da config.ini
DEFAULT_MERGED_OUT = None  # se None, usa <output_dir>/merged_graph_output.ttl da config.ini

YEAR4_RE   = re.compile(r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})(?:T.*)?$")
LONG_RE    = re.compile(r"^(?P<y>-?\d{5,})-(?P<m>\d{2})-(?P<d>\d{2})(?:T.*)?$")
Y_PREFIX   = re.compile(r"^Y-?\d+-\d{2}-\d{2}(?:T.*)?$")
YEAR0_RE   = re.compile(r"^0000-\d{2}-\d{2}(?:T.*)?$")

CRM_P82A = URIRef("http://www.cidoc-crm.org/cidoc-crm/P82a_begin_of_the_begin")
CRM_P82B = URIRef("http://www.cidoc-crm.org/cidoc-crm/P82b_end_of_the_end")
EDTF_DT  = URIRef("http://id.loc.gov/datatypes/edtf/EDTF")

def _needs_edtf(lex: str) -> bool:
    """True se la stringa rappresenta un long/negative/anno 0 → serve EDTF."""
    return (
        Y_PREFIX.match(lex) is not None or
        LONG_RE.match(lex) is not None or
        YEAR0_RE.match(lex) is not None or
        lex.startswith("-")  # anni negativi formattati senza Y (es. "-0099-01-01")
    )

def _ensure_t(lex: str, is_begin: bool) -> str:
    """Aggiunge 'T..Z' se manca; lascia intatta la parte data."""
    if "T" in lex:
        return lex
    time = "00:00:00Z" if is_begin else "23:59:59Z"
    return f"{lex}T{time}"

def normalize_time_literals(g: Graph) -> int:
    """
    Converte:
      - long/negative/anno 0 → datatype edtf:EDTF (senza T)
      - anni 0001..9999      → xsd:dateTime con T e Z
    Ritorna il numero di sostituzioni.
    """
    to_fix = []
    for s, p, o in g.triples((None, None, None)):
        if p not in (CRM_P82A, CRM_P82B):
            continue
        if not isinstance(o, Literal):
            continue
        lex = str(o)

        # Caso EDTF: Y..., year a 5+ cifre, anno 0 o negativo
        if _needs_edtf(lex):
            # togli eventuale T... (EDTF per giorno completo non richiede l'ora)
            base = lex.split("T", 1)[0]
            if o.datatype != EDTF_DT or base != lex:
                to_fix.append((s, p, o, Literal(base, datatype=EDTF_DT)))
            continue

        # Caso normale 4 cifre (1..9999): assicurare xsd:dateTime con T
        m = YEAR4_RE.match(lex)
        if m:
            y = int(m.group("y"))
            if 1 <= y <= 9999:
                want = _ensure_t(lex.split("T", 1)[0], is_begin=(p == CRM_P82A))
                if o.datatype != XSD.dateTime or want != lex:
                    to_fix.append((s, p, o, Literal(want, datatype=XSD.dateTime)))
            else:
                # anno 0000 catturato da YEAR4_RE ma non valido → passa al ramo EDTF
                base = lex.split("T", 1)[0]
                to_fix.append((s, p, o, Literal(base, datatype=EDTF_DT)))
        else:
            # pattern inatteso: non toccare
            pass

    for s, p, old, new in to_fix:
        g.remove((s, p, old))
        g.add((s, p, new))
    return len(to_fix)

def normalize_newlines_in_ttl(src_ttl: str, dst_ttl: str) -> None:
    """
    Crea una copia del file Turtle sostituendo la sequenza letterale \\n
    (backslash-backslash-n nel file) con \n (backslash-n).
    NON introduce newline reali e NON riscrive i literal: Turtle resta valido.
    """
    with open(src_ttl, "r", encoding="utf-8") as f:
        content = f.read()

    # Nel testo del file: \\n -> \n
    normalized = content.replace("\\\\n", "\\n")

    os.makedirs(os.path.dirname(dst_ttl) or ".", exist_ok=True)
    with open(dst_ttl, "w", encoding="utf-8") as f:
        f.write(normalized)

    print(f"[orchestrator] Grafo con newline normalizzati scritto in: {dst_ttl}")

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

    # bind prefix per leggibilità (opzionale)
    merged.bind("edtf", "http://id.loc.gov/datatypes/edtf/")
    fixed = normalize_time_literals(merged)
    print(f"[orchestrator] Normalizzati {fixed} literal di tempo (xsd:dateTime/edtf:EDTF).")

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
    ap.add_argument("--quality-script", default="run_quality_threat_model.py", help="Script di quality threat model")

    ap.add_argument(
        "--new_line_normaliser",
        action="store_true",
        help="Genera una copia del grafo finale con normalizzazione \\n -> newline reale"
    )

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

    # --- newline normaliser (opzionale) ---
    if args.new_line_normaliser:
        base, ext = os.path.splitext(merged_out)
        norm_out = f"{base}_new_line_norm{ext}"
        normalize_newlines_in_ttl(merged_out, norm_out)

    print(f"[orchestrator] Avvio quality threat model su: {merged_out}")
    res = subprocess.run(
        [sys.executable, args.quality_script, "--config", args.config],
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    if res.returncode != 0:
        print(f"[orchestrator] Quality threat model ha segnalato anomalie (exit {res.returncode}).", file=sys.stderr)
    else:
        print("[orchestrator] Quality threat model completato.")

    # riepilogo complessivo
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
