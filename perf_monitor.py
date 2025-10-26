#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PerfMonitor: misure di performance + report JSON condiviso per più run.

Metriche base (per singolo run):
  - elapsed_time_s
  - peak_rss_mb
  - rows_processed
  - iri_generated
  - materialisation_time_per_1000_rows_s
  - iri_rate_per_s

Operazioni su JSON:
  - update_json_report(report_path, run_key, results_dict, extra_fields=None)
      Aggiorna/crea il nodo runs[run_key] con i risultati del run.
  - read_json_report(report_path) -> dict
  - add_overall_summary(report_path, overall_key="overall")
      Calcola i totali/medie e aggiorna report['overall'].

Schema file JSON (esempio):
{
  "created_at": "2025-10-26T10:21:47+00:00",
  "updated_at": "2025-10-26T10:23:02+00:00",
  "runs": {
    "objects": {...},
    "process": {...}
  },
  "overall": {...}
}
"""

from __future__ import annotations
import json
import os
import sys
import time
import datetime as _dt
from dataclasses import dataclass, asdict
from typing import Optional, Any, Dict

# ---- Peak RSS via resource (Unix/macOS) con fallback psutil
try:
    import resource  # Unix/macOS
    _HAS_RESOURCE = True
except Exception:
    _HAS_RESOURCE = False

try:
    import psutil
    _HAS_PSUTIL = True
except Exception:
    _HAS_PSUTIL = False

# ---- File locking cross-platform (best-effort)
_IS_WINDOWS = os.name == "nt"
if _IS_WINDOWS:
    try:
        import msvcrt  # type: ignore
        _HAS_LOCK = True
    except Exception:
        _HAS_LOCK = False
else:
    try:
        import fcntl  # type: ignore
        _HAS_LOCK = True
    except Exception:
        _HAS_LOCK = False


@dataclass
class PerfResults:
    elapsed_time_s: float
    peak_rss_mb: Optional[float]
    rows_processed: Optional[int]
    iri_generated: Optional[int]
    materialisation_time_per_1000_rows_s: Optional[float]
    iri_rate_per_s: Optional[float]
    timestamp: str  # ISO 8601, quando creata la misura


class PerfMonitor:
    """
    Context manager per misurare tempo e memoria di una sezione di pipeline.
    """

    def __init__(self, label: str = "run", verbose: bool = True):
        self.label = label
        self.verbose = verbose
        self._t0: Optional[float] = None
        self._t1: Optional[float] = None
        self._peak_rss_mb: Optional[float] = None

    def __enter__(self) -> "PerfMonitor":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._t1 = time.perf_counter()
        self._peak_rss_mb = self._get_peak_rss_mb()
        if exc and self.verbose:
            print(f"[PerfMonitor:{self.label}] Exception: {exc}", file=sys.stderr)
        return False  # non sopprime eccezioni

    # principale

    def report(self, rows_processed: Optional[int], iris_generated: Optional[int]) -> PerfResults:
        elapsed = self.elapsed_time_s
        mt_per_1000 = None
        iri_rate = None

        if rows_processed and rows_processed > 0:
            mt_per_1000 = elapsed / (rows_processed / 1000.0)
        if iris_generated is not None and elapsed > 0:
            iri_rate = iris_generated / elapsed

        results = PerfResults(
            elapsed_time_s=elapsed,
            peak_rss_mb=self._peak_rss_mb,
            rows_processed=rows_processed,
            iri_generated=iris_generated,
            materialisation_time_per_1000_rows_s=mt_per_1000,
            iri_rate_per_s=iri_rate,
            timestamp=_now_iso(),
        )
        if self.verbose:
            self._print_summary(results)
        return results

    @staticmethod
    def count_iris_from_graph(graph: Any, mode: str = "subjects_unique") -> int:
        """
        Conta IRI in un grafo rdflib.
        mode:
          - 'subjects_unique' (default): soggetti URIRef unici
          - 'all_uri_nodes': tutti i nodi URIRef distinti (s, p, o)
        """
        try:
            from rdflib.term import URIRef
        except Exception:
            raise RuntimeError("rdflib non è installato: pip install rdflib")

        if mode == "subjects_unique":
            uris = set()
            for s, _, _ in graph.triples((None, None, None)):
                if isinstance(s, URIRef):
                    uris.add(s)
            return len(uris)
        elif mode == "all_uri_nodes":
            uris = set()
            from rdflib.term import URIRef
            for s, p, o in graph.triples((None, None, None)):
                if isinstance(s, URIRef): uris.add(s)
                if isinstance(p, URIRef): uris.add(p)
                if isinstance(o, URIRef): uris.add(o)
            return len(uris)
        else:
            raise ValueError("mode deve essere 'subjects_unique' o 'all_uri_nodes'")

    # JSON report helpers

    @staticmethod
    def update_json_report(
        report_path: str,
        run_key: str,
        results: PerfResults | Dict[str, Any],
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Aggiorna/crea report JSON con il run 'run_key'.
        Se il file non esiste, viene creato.
        """
        data = PerfMonitor._safe_read_json(report_path)
        if "runs" not in data:
            data["runs"] = {}
        payload = asdict(results) if isinstance(results, PerfResults) else dict(results)
        if extra_fields:
            payload.update(extra_fields)
        data["runs"][run_key] = payload
        PerfMonitor._safe_write_json(report_path, data)

    @staticmethod
    def read_json_report(report_path: str) -> Dict[str, Any]:
        return PerfMonitor._safe_read_json(report_path)

    @staticmethod
    def add_overall_summary(report_path: str, overall_key: str = "overall") -> Dict[str, Any]:
        """
        Calcola il riepilogo complessivo sulle run presenti e aggiorna il JSON.
        Ritorna l'intero dizionario aggiornato.
        """
        data = PerfMonitor._safe_read_json(report_path)
        runs = data.get("runs", {})
        totals = _compute_overall_from_runs(runs)
        data[overall_key] = totals
        PerfMonitor._safe_write_json(report_path, data)
        return data

    # interne

    @property
    def elapsed_time_s(self) -> float:
        if self._t0 is None:
            return 0.0
        t1 = self._t1 if self._t1 is not None else time.perf_counter()
        return max(0.0, t1 - self._t0)

    def _get_peak_rss_mb(self) -> Optional[float]:
        # Preferisce resource (peak storico)
        if _HAS_RESOURCE:
            try:
                usage = resource.getrusage(resource.RUSAGE_SELF)
                ru = float(usage.ru_maxrss)
                # Normalizza: macOS bytes, Linux KB
                if ru > 10**9 or ru > 10**6:  # euristica: bytes
                    return ru / (1024**2)
                return ru / 1024.0  # KB -> MB
            except Exception:
                pass
        # Fallback: RSS corrente (non peak) via psutil
        if _HAS_PSUTIL:
            try:
                rss = float(psutil.Process(os.getpid()).memory_info().rss) / (1024**2)
                return rss
            except Exception:
                pass
        return None

    def _print_summary(self, results: PerfResults) -> None:
        d = asdict(results)
        print(f"\n[PerfMonitor:{self.label}] Risultati")
        print("-" * 60)
        print(f"Elapsed time (s):                    {d['elapsed_time_s']:.4f}")
        print(f"Peak RSS (MB):                       {d['peak_rss_mb'] if d['peak_rss_mb'] is not None else 'n/a'}")
        print(f"Rows processed:                      {d['rows_processed'] if d['rows_processed'] is not None else 'n/a'}")
        print(f"IRI generated:                       {d['iri_generated'] if d['iri_generated'] is not None else 'n/a'}")
        if d['materialisation_time_per_1000_rows_s'] is not None:
            print(f"Materialisation time per 1000 rows:  {d['materialisation_time_per_1000_rows_s']:.6f} s")
        else:
            print("Materialisation time per 1000 rows:  n/a")
        if d['iri_rate_per_s'] is not None:
            print(f"IRI generation rate:                 {d['iri_rate_per_s']:.6f} IRI/s")
        else:
            print("IRI generation rate:                 n/a")
        print(f"Timestamp:                           {d['timestamp']}")
        print("-" * 60)

    #  JSON I/O con lock best-effort

    @staticmethod
    def _safe_read_json(path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            return {"created_at": _now_iso(), "updated_at": _now_iso(), "runs": {}}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            # File corrotto o vuoto: riparti pulito
            return {"created_at": _now_iso(), "updated_at": _now_iso(), "runs": {}}

    @staticmethod
    def _safe_write_json(path: str, data: Dict[str, Any]) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        data["updated_at"] = _now_iso()
        # Lock esclusivo best-effort
        if _HAS_LOCK:
            with open(path, "a+b") as f:
                try:
                    if _IS_WINDOWS:
                        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
                    else:
                        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                except Exception:
                    pass
                try:
                    # riscrivi intero file tramite tmp
                    tmp = path + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as out:
                        json.dump(data, out, ensure_ascii=False, indent=2)
                    os.replace(tmp, path)
                finally:
                    try:
                        if _IS_WINDOWS:
                            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
                        else:
                            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                    except Exception:
                        pass
        else:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as out:
                json.dump(data, out, ensure_ascii=False, indent=2)
            os.replace(tmp, path)


#  funzioni di supporto

def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).astimezone().isoformat(timespec="seconds")

def _safe_sum(values):
    vals = [v for v in values if isinstance(v, (int, float))]
    return sum(vals) if vals else None

def _safe_max(values):
    vals = [v for v in values if isinstance(v, (int, float))]
    return max(vals) if vals else None

def _safe_div(n, d):
    try:
        if n is None or d in (None, 0):
            return None
        return n / d
    except Exception:
        return None

def _compute_overall_from_runs(runs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calcola i complessivi a partire dalle run contenute in `runs`.
    Regole:
      - elapsed_time_s: somma
      - peak_rss_mb: massimo
      - rows_processed, iri_generated: somma
      - materialisation_time_per_1000_rows_s: ricalcolo globale = elapsed_tot / (rows_tot/1000)
      - iri_rate_per_s: ricalcolo globale = iri_tot / elapsed_tot
    """
    elapsed_list = []
    peak_list = []
    rows_list = []
    iri_list = []

    for _key, r in runs.items():
        elapsed_list.append(r.get("elapsed_time_s"))
        peak_list.append(r.get("peak_rss_mb"))
        rows_list.append(r.get("rows_processed"))
        iri_list.append(r.get("iri_generated"))

    elapsed_tot = _safe_sum(elapsed_list)
    rows_tot = _safe_sum(rows_list)
    iri_tot = _safe_sum(iri_list)
    peak_max = _safe_max(peak_list)

    mt_per_1000_global = _safe_div(elapsed_tot, _safe_div(rows_tot, 1000.0))
    iri_rate_global = _safe_div(iri_tot, elapsed_tot)

    return {
        "elapsed_time_s": elapsed_tot,
        "peak_rss_mb": peak_max,
        "rows_processed": rows_tot,
        "iri_generated": iri_tot,
        "materialisation_time_per_1000_rows_s": mt_per_1000_global,
        "iri_rate_per_s": iri_rate_global,
        "timestamp": _now_iso(),
        "runs_count": len(runs),
    }
