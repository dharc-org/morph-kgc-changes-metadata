#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Threat Model and Data Quality Checks

This script was integrated into the codebase to assess the quality of the RDF data just generated (run_quality_threat_model.py).
Threat Model and Data Quality Checks identify structural and semantic anomalies in the unified RDF graph.

(1) Duplicate IRIs and Type Conflicts
Risk: the same IRI is typed with classes belonging to disjoint categories (e.g. Person and Group).
Check: verify types against the categories defined in disjoint_buckets and report conflicts.

(2) Stale External Links
Risk: links to authorities (e.g. Getty, VIAF) are no longer accessible.
Check: collects sample URIs in the link_namespaces domains, makes HTTP requests, and reports invalid links.

(3) Inconsistent Time Spans
Risk: intervals with a starting date later than the end date.
Check: for each E52_Time-Span, compare the start and end dates and report inconsistencies.

(4) Single-Valued Property Violations
Risk: properties that should have only one value, such as an identifier, appear multiple times.
Check: counts the occurrences of predicates in single_valued_props and reports if more than one value is retrieved.

An output report (quality_report.json) is generated in the path of the folder declared as the value of the quality_report parameter in the configuration file.
It includes the fields below:
- timestamp: date and time of the check.
- input_graph: path of the analysed graph.
- summary: counts of anomalies for each check.
- details: sampled list (up to 50 items) of cases detected.
- params: configuration used in executing the quality check  (timeout, link domains, buckets, properties, etc.).

This script is executed after all the other tasks in run_unified_pipeline.py, to assess the quality of the just generated graph.
'''
from __future__ import annotations
import argparse
import configparser
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Set, Tuple

from rdflib import Graph, URIRef, RDF, Namespace, Literal

# Optional dependency for HTTP probing; if unavailable, link checks are skipped.
try:
    import requests  # optional: link check will be skipped if missing
except Exception:
    requests = None


def read_cfg(cfg_path: str) -> Tuple[str, str, dict]:
    """
    Read configuration from config.ini:
      - output_dir & quality_report location
      - QUALITY parameters: sampling, timeouts, link namespaces, disjoint buckets,
        single-valued predicates, and begin/end properties.
    """
    cfg = configparser.ConfigParser()
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg.read_file(f)

    out_dir = cfg.get("CONFIGURATION", "output_dir", fallback="results").strip()
    quality_dir = cfg.get("CONFIGURATION", "quality_report", fallback="results/quality").strip()

    q = {}
    q["http_timeout"] = cfg.getint("QUALITY", "http_timeout", fallback=5)
    q["max_links"] = cfg.getint("QUALITY", "max_links", fallback=200)
    q["sample_size"] = cfg.getint("QUALITY", "sample_size", fallback=25)
    q["link_namespaces"] = [
        d.strip() for d in cfg.get("QUALITY", "link_namespaces", fallback="").split(",") if d.strip()
    ]

    # Parse disjoint bucket specification "name=IRI|IRI; name=IRI|IRI; ..."
    disj_raw = cfg.get("QUALITY", "disjoint_buckets", fallback="")
    disjoint_buckets: Dict[str, Set[str]] = {}
    for part in [p.strip() for p in disj_raw.split(";") if p.strip()]:
        if "=" in part:
            name, rhs = part.split("=", 1)
            iris = set(u.strip() for u in rhs.split("|") if u.strip())
            if iris:
                disjoint_buckets[name.strip()] = iris
    q["disjoint_buckets"] = disjoint_buckets

    q["single_valued_props"] = [
        u.strip() for u in cfg.get("QUALITY", "single_valued_props", fallback="").split(",") if u.strip()
    ]
    q["begin_props"] = [u.strip() for u in cfg.get("QUALITY", "begin_props", fallback="").split(",") if u.strip()]
    q["end_props"] = [u.strip() for u in cfg.get("QUALITY", "end_props", fallback="").split(",") if u.strip()]

    return out_dir, quality_dir, q


def load_graph(path: str) -> Graph:
    """Load a Turtle graph from path."""
    g = Graph()
    g.parse(path, format="turtle")
    return g


def map_types_to_buckets(g: Graph, disjoint_buckets: Dict[str, Set[str]]) -> Dict[URIRef, Set[str]]:
    """Build a mapping: IRI → bucket set based on rdf:type membership."""
    iri2buckets: Dict[URIRef, Set[str]] = {}
    class2bucket: Dict[str, str] = {}
    for bname, iris in disjoint_buckets.items():
        for c in iris:
            class2bucket[c] = bname
    for s, _, o in g.triples((None, RDF.type, None)):
        if isinstance(s, URIRef) and isinstance(o, URIRef):
            b = class2bucket.get(str(o))
            if b:
                iri2buckets.setdefault(s, set()).add(b)
    return iri2buckets


def find_bucket_conflicts(iri2buckets: Dict[URIRef, Set[str]]) -> List[dict]:
    """Return IRIs whose bucket set size > 1 (conflicts across disjoint buckets)."""
    return [{"iri": str(iri), "buckets": sorted(buckets)} for iri, buckets in iri2buckets.items() if len(buckets) > 1]


def collect_external_links(g: Graph, filter_domains: List[str], max_links: int) -> List[str]:
    """Collect up to max_links external HTTP(S) IRIs, filtered by allowed authority domains."""
    links: List[str] = []
    for _, _, o in g:
        if isinstance(o, URIRef):
            url = str(o)
            if url.startswith(("http://", "https://")):
                if not filter_domains or any(d in url for d in filter_domains):
                    links.append(url)
                    if len(links) >= max_links:
                        break
    return links


def is_url_ok(url: str, timeout: int) -> Tuple[bool, int]:
    """Probe a URL with HEAD (fallback GET on 405). Return (ok, status_code)."""
    if requests is None:
        return True, 0
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        code = r.status_code
        if code == 405:
            r = requests.get(url, allow_redirects=True, timeout=timeout, stream=True)
            code = r.status_code
        return (200 <= code < 400), code
    except Exception:
        return False, 0


def sample_check_links(links: List[str], sample_size: int, timeout: int) -> List[dict]:
    """Check the first N links and return the failing subset {url, http_status}."""
    out = []
    sample = links[: max(sample_size, 0)]
    for url in sample:
        ok, code = is_url_ok(url, timeout)
        if not ok:
            out.append({"url": url, "http_status": code or None})
    return out


def parse_xsd_datetime(lit: Literal):
    """Parse xsd:dateTime literal to datetime; return None on failure."""
    try:
        return datetime.fromisoformat(str(lit).replace("Z", "+00:00"))
    except Exception:
        return None


def find_timespan_inconsistencies(g: Graph, begin_props: List[str], end_props: List[str]) -> List[dict]:
    """
    For each crm:E52_Time-Span, gather begin/end literals; if the earliest begin
    is later than the latest end, flag the time-span as inconsistent.
    """
    crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
    E52 = URIRef(str(crm) + "E52_Time-Span")

    begin_predicates = [URIRef(u) for u in begin_props] if begin_props else [URIRef(str(crm) + "P82a_begin_of_the_begin")]
    end_predicates   = [URIRef(u) for u in end_props]   if end_props   else [URIRef(str(crm) + "P82b_end_of_the_end")]

    issues: List[dict] = []
    timespans = set(s for s, p, o in g.triples((None, RDF.type, E52)))

    for ts in timespans:
        begins, ends = [], []
        for bp in begin_predicates:
            for _, _, o in g.triples((ts, bp, None)):
                if isinstance(o, Literal):
                    begins.append(o)
        for ep in end_predicates:
            for _, _, o in g.triples((ts, ep, None)):
                if isinstance(o, Literal):
                    ends.append(o)

        if begins and ends:
            b_vals = [parse_xsd_datetime(b) for b in begins]
            e_vals = [parse_xsd_datetime(e) for e in ends]
            b_vals = [b for b in b_vals if b is not None]
            e_vals = [e for e in e_vals if e is not None]
            if b_vals and e_vals and min(b_vals) > max(e_vals):
                issues.append({
                    "timespan": str(ts),
                    "min_begin": min(b_vals).isoformat(),
                    "max_end": max(e_vals).isoformat(),
                })
    return issues


def find_multi_valued_props(g: Graph, props: List[str], sample_limit: int = 50) -> List[dict]:
    """
    For each configured predicate, count the number of values per subject and
    flag subjects with multiplicity > 1 (sampled up to sample_limit findings).
    """
    out = []
    predicates = [URIRef(u) for u in props]
    for p in predicates:
        counts = {}
        for s, _, _ in g.triples((None, p, None)):
            if isinstance(s, URIRef):
                counts[s] = counts.get(s, 0) + 1
        for s, cnt in counts.items():
            if cnt > 1:
                out.append({"subject": str(s), "predicate": str(p), "count": cnt})
                if len(out) >= sample_limit:
                    return out
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.ini")
    args = ap.parse_args()

    # =========================================================================
    # CONFIGURATION & ENVIRONMENT SANITY
    # - Load output_dir and quality report directory
    # - Load QUALITY parameters (timeouts, sampling, link namespaces, buckets,
    #   single-valued predicates, begin/end properties)
    # - Resolve merged graph path and ensure it exists
    # - Prepare quality report output path
    # =========================================================================
    out_dir, quality_dir, q = read_cfg(args.config)
    merged_path = os.path.join(out_dir, "merged_graph_output.ttl")

    if not os.path.exists(merged_path):
        print(f"[quality] Unified TTL not found: {merged_path}", file=sys.stderr)
        sys.exit(2)

    os.makedirs(quality_dir, exist_ok=True)
    report_path = os.path.join(quality_dir, "quality_report.json")

    g = load_graph(merged_path)

    # =========================================================================
    # DUPLICATE IRI CONFLICTS ACROSS DISJOINT TYPE BUCKETS
    # - Map each resource's rdf:types to configured buckets
    # - Flag resources typed in more than one bucket (identity/modeling conflicts)
    # =========================================================================
    iri2buckets = map_types_to_buckets(g, q["disjoint_buckets"])
    bucket_conflicts = find_bucket_conflicts(iri2buckets)

    # =========================================================================
    # STALE AUTHORITY LINKS (SAMPLED HTTP PROBES)
    # - Collect up to max_links external IRIs filtered by allowed domains
    # - Probe the first sample_size links; flag those with failing HTTP status
    # - Skipped if 'requests' is not available
    # =========================================================================
    links = collect_external_links(g, q["link_namespaces"], q["max_links"])
    stale_links = sample_check_links(links, q["sample_size"], q["http_timeout"])

    # =========================================================================
    # INCONSISTENT TIME SPANS (E52)
    # - For each crm:E52_Time-Span, parse begins/ends and verify min(begin) ≤ max(end)
    # - Flag spans where earliest begin is after latest end
    # =========================================================================
    timespan_issues = find_timespan_inconsistencies(g, q["begin_props"], q["end_props"])

    # =========================================================================
    # SINGLE-VALUED PROPERTY VIOLATIONS
    # - For each configured predicate, flag subjects with multiplicity > 1
    # - Sample output to avoid oversized reports
    # =========================================================================
    sv_issues = find_multi_valued_props(g, q["single_valued_props"]) if q["single_valued_props"] else []

    # =========================================================================
    # REPORT EMISSION
    # - Summarize issue counts and persist samples (up to 50 per category)
    # - Exit code 0 if clean; 1 if any issue found; 2 if input graph missing
    # =========================================================================
    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "input_graph": merged_path,
        "summary": {
            "duplicate_iri_conflicts": len(bucket_conflicts),
            "stale_links": len(stale_links),
            "inconsistent_timespans": len(timespan_issues),
            "single_valued_violations": len(sv_issues),
        },
        "details": {
            "duplicate_iri_conflicts": bucket_conflicts[:50],
            "stale_links": stale_links[:50],
            "inconsistent_timespans": timespan_issues[:50],
            "single_valued_violations": sv_issues[:50],
        },
        "params": {
            "http_timeout": q["http_timeout"],
            "max_links": q["max_links"],
            "sample_size": q["sample_size"],
            "link_namespaces": q["link_namespaces"],
            "begin_props": q["begin_props"],
            "end_props": q["end_props"],
            "disjoint_buckets": {k: sorted(v) for k, v in q["disjoint_buckets"].items()},
        },
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"[quality] Report written: {report_path}")
    sys.exit(1 if any(report["summary"].values()) else 0)


if __name__ == "__main__":
    main()
