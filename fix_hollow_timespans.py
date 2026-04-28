"""
Fix per errori SHACL [7][8]: rimozione nodi E52_Time-Span "cavi"
(privi di P82a_begin_of_the_begin e P82b_end_of_the_end).

Dove inserire in main_process_demo.py:
  Dentro process_rdf_data(), subito dopo la chiamata a pair_subject_object():

    g = pair_subject_object(g, properties_in_triples_to_clean)
    g, n_idf = remove_hollow_identifiers(g)
    g, n_tsp = remove_hollow_timespans(g)     # ← NUOVO
    print(f"[fix] E52_Time-Span cavi rimossi: {n_tsp}")

  E in cima al file, con gli altri import:
    from fix_hollow_timespans import remove_hollow_timespans

Dove inserire in main_object_demo.py (se presenti tsp anche lì):
  Stesso punto del postprocessing, dopo pair_subject_object() e remove_hollow_identifiers().
"""

from rdflib import Graph, URIRef, RDF

CRM         = "http://www.cidoc-crm.org/cidoc-crm/"
E52_TIMESPAN = URIRef(CRM + "E52_Time-Span")
P82A_BEGIN   = URIRef(CRM + "P82a_begin_of_the_begin")
P82B_END     = URIRef(CRM + "P82b_end_of_the_end")
P4_HAS_TSP   = URIRef(CRM + "P4_has_time-span")


def remove_hollow_timespans(g: Graph) -> tuple[Graph, int]:
    """
    Rimuove dal grafo i nodi E52_Time-Span che non hanno né
    P82a_begin_of_the_begin né P82b_end_of_the_end.

    Per ciascun nodo cavo:
      1. Rimuove tutte le triple dove il nodo è SOGGETTO
         (es. <tsp/x> a crm:E52_Time-Span .)
      2. Rimuove tutte le triple dove il nodo è OGGETTO
         (es. <act/x> crm:P4_has_time-span <tsp/x> .)

    Returns:
        (grafo_pulito, numero_nodi_rimossi)
    """
    all_timespans = set(g.subjects(RDF.type, E52_TIMESPAN))

    # Un tsp è "cavo" se non ha né begin né end
    timespans_with_begin = set(g.subjects(P82A_BEGIN, None))
    timespans_with_end   = set(g.subjects(P82B_END,   None))
    timespans_with_dates = timespans_with_begin | timespans_with_end

    hollow = all_timespans - timespans_with_dates

    if not hollow:
        print("[fix] Nessun E52_Time-Span cavo trovato.")
        return g, 0

    print(f"[fix] Trovati {len(hollow)} nodi E52_Time-Span senza date → rimossi.")

    triples_removed = 0
    for node in hollow:
        # Triple dove il tsp è soggetto (es. dichiarazione del tipo)
        for triple in list(g.triples((node, None, None))):
            g.remove(triple)
            triples_removed += 1
        # Triple dove il tsp è oggetto (es. P4_has_time-span dall'attività)
        for triple in list(g.triples((None, None, node))):
            g.remove(triple)
            triples_removed += 1

    print(f"[fix] Triple eliminate in totale: {triples_removed}")
    return g, len(hollow)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python fix_hollow_timespans.py <file.ttl>")
        sys.exit(1)
    g = Graph()
    g.parse(sys.argv[1])
    print(f"Triple totali prima: {len(g)}")
    g, n = remove_hollow_timespans(g)
    print(f"Triple totali dopo:  {len(g)}")
    print(f"Nodi rimossi: {n}")