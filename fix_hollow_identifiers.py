"""
Fix per errore SHACL [3]: rimozione nodi E42_Identifier "cavi" (privi di P190_has_symbolic_content).

Questa funzione va aggiunta in main_object_demo.py (e opzionalmente in main_process_demo.py
per identifier_last_model) subito dopo la chiamata a pair_subject_object().

Logica:
  1. Trova tutti i nodi di tipo crm:E42_Identifier nel grafo
  2. Identifica quelli che NON hanno la tripla P190_has_symbolic_content
  3. Rimuove quelle istanze "cave" E TUTTE le triple che le puntano
     (in particolare crm:P1_is_identified_by dall'item)
"""

from rdflib import Graph, URIRef, RDF

CRM = "http://www.cidoc-crm.org/cidoc-crm/"
E42_IDENTIFIER      = URIRef(CRM + "E42_Identifier")
E41_APPELLATION     = URIRef(CRM + "E41_Appellation")
E35_TITLE           = URIRef(CRM + "E35_Title")
P190_SYMBOLIC       = URIRef(CRM + "P190_has_symbolic_content")
P1_IS_IDENTIFIED_BY = URIRef(CRM + "P1_is_identified_by")

# Tipi considerati "cavi" se privi di P190_has_symbolic_content
HOLLOW_TYPES = {E42_IDENTIFIER, E41_APPELLATION, E35_TITLE}


def remove_hollow_identifiers(g: Graph) -> tuple[Graph, int]:
    """
    Rimuove dal grafo i nodi E42_Identifier e E41_Appellation che non hanno
    P190_has_symbolic_content. Rimuove anche tutte le triple che puntano
    a quei nodi (es. P1_is_identified_by).

    Returns:
        (grafo_pulito, numero_nodi_rimossi)
    """
    # 1. Tutti i nodi di tipo E42_Identifier o E41_Appellation
    all_identifiers = set()
    for t in HOLLOW_TYPES:
        all_identifiers |= set(g.subjects(RDF.type, t))

    # 2. Quelli CON P190_has_symbolic_content
    identifiers_with_content = set(g.subjects(P190_SYMBOLIC, None))

    # 3. I "cavi": presenti nel grafo ma senza contenuto
    hollow = all_identifiers - identifiers_with_content

    if not hollow:
        print("[fix] Nessun nodo E42_Identifier/E41_Appellation/E35_Title cavo trovato.")
        return g, 0

    print(f"[fix] Trovati {len(hollow)} nodi E42_Identifier/E41_Appellation/E35_Title senza P190_has_symbolic_content → rimossi.")

    triples_removed = 0
    for node in hollow:
        # Rimuovi tutte le triple dove il nodo è SOGGETTO
        for triple in list(g.triples((node, None, None))):
            g.remove(triple)
            triples_removed += 1
        # Rimuovi tutte le triple dove il nodo è OGGETTO
        # (tipicamente: <item> crm:P1_is_identified_by <idf_cavo>)
        for triple in list(g.triples((None, None, node))):
            g.remove(triple)
            triples_removed += 1

    print(f"[fix] Triple eliminate in totale: {triples_removed}")
    return g, len(hollow)


# ─────────────────────────────────────────────────────────────
# DOVE INSERIRE IN main_object_demo.py
# ─────────────────────────────────────────────────────────────
#
# Attualmente il postprocessing è:
#
#   g = Graph()
#   g.parse(output_path)
#   g_clean = pair_subject_object(g, properties_in_triples_to_clean)
#   g_clean.serialize(destination=output_file_path, format="turtle")
#
# Diventa:
#
#   g = Graph()
#   g.parse(output_path)
#   g_clean = pair_subject_object(g, properties_in_triples_to_clean)
#   g_clean, n_removed = remove_hollow_identifiers(g_clean)   # ← NUOVO
#   print(f"[fix] E42_Identifier cavi rimossi: {n_removed}")
#   g_clean.serialize(destination=output_file_path, format="turtle")
#
# ─────────────────────────────────────────────────────────────
# NOTA per main_process_demo.py (errore su identifier_last_model):
# La stessa funzione copre anche il caso $(LINK) vuoto nel mapping acquisizione,
# perché identifier_last_model produce anch'esso un E42_Identifier senza P190
# quando la colonna LINK è assente.
# ─────────────────────────────────────────────────────────────


if __name__ == "__main__":
    # Test rapido su un TTL di esempio
    import sys
    if len(sys.argv) < 2:
        print("Uso: python fix_hollow_identifiers.py <file.ttl>")
        sys.exit(1)
    g = Graph()
    g.parse(sys.argv[1])
    print(f"Triple totali prima: {len(g)}")
    g, n = remove_hollow_identifiers(g)
    print(f"Triple totali dopo:  {len(g)}")
    print(f"Nodi rimossi: {n}")