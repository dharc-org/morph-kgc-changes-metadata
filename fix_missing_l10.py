"""
Fix per errore SHACL [12]: L10_had_input mancante su act di fase iniziale.
"""

import re
import pandas as pd
from rdflib import Graph, URIRef, RDF

BASE   = "https://w3id.org/changes/4/aldrovandi/"
CRM    = "http://www.cidoc-crm.org/cidoc-crm/"
CRMDIG = "http://www.cidoc-crm.org/extensions/crmdig/"

L10  = URIRef(CRMDIG + "L10_had_input")
L11  = URIRef(CRMDIG + "L11_had_output")

# Proprietà che indicano dati REALI (non statici del mapping)
MEANINGFUL_PROPS = {
    URIRef(CRM    + "P14_carried_out_by"),
    URIRef(CRM    + "P4_has_time-span"),
    URIRef(CRMDIG + "L23_used_software_or_firmware"),
    URIRef(CRMDIG + "L23_used_software_or_hardware"),
    URIRef(CRM    + "P11_had_participant"),
}


def _has_real_data(g: Graph, act_iri: URIRef) -> bool:
    """
    True se il nodo ha almeno una proprietà significativa
    (operatore, timespan, software). P2_has_type e L11 sono
    valori statici del mapping e non contano.
    """
    for _, p, _ in g.triples((act_iri, None, None)):
        if p in MEANINGFUL_PROPS:
            return True
    return False


def _get_oggetto_esistente(csv_path: str) -> set:
    """
    Restituisce gli NR (stringhe) con OGGETTO_ESISTENTE valorizzato.
    """
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="latin1")

    if "OGGETTO_ESISTENTE" not in df.columns:
        return set()

    mask = (df["OGGETTO_ESISTENTE"].notnull() &
            (df["OGGETTO_ESISTENTE"].astype(str).str.strip() != ""))
    return set(df.loc[mask, "NR"].astype(str).str.strip())


def add_missing_l10_for_first_phase(
    g: Graph,
    first_phase_dict: dict,
    csv_input: str
) -> tuple[Graph, int, int]:
    """
    Per ogni NR in first_phase_dict con first_phase > "00":

      - Se NR ha OGGETTO_ESISTENTE e act è un guscio vuoto:
          Rimuove act/NR/{first_phase}/1 e tutte le triple collegate.

      - Se NR NON ha OGGETTO_ESISTENTE:
          Aggiunge act/NR/{first_phase}/1 crmdig:L10_had_input
                   mdl/NR/{first_phase}/1 .

    Returns: (grafo, n_rimossi, n_aggiunti)
    """
    oggetto_esistente_nrs = _get_oggetto_esistente(csv_input)

    n_removed = 0
    n_added   = 0

    for nr, first_phase in first_phase_dict.items():
        if first_phase == "00":
            continue

        act_iri = URIRef(f"{BASE}act/{nr}/{first_phase}/1")
        mdl_iri = URIRef(f"{BASE}mdl/{nr}/{first_phase}/1")

        if not list(g.triples((act_iri, None, None))):
            continue  # nodo non esiste nel grafo

        if nr in oggetto_esistente_nrs:
            # Rimuovi solo se guscio vuoto (nessuna proprietà significativa)
            if not _has_real_data(g, act_iri):
                triples = (list(g.triples((act_iri, None, None))) +
                           list(g.triples((None, None, act_iri))))
                for t in triples:
                    g.remove(t)
                n_removed += 1
                print(f"[fix_l10] Rimosso act guscio: act/{nr}/{first_phase}/1")
            else:
                print(f"[fix_l10] act/{nr}/{first_phase}/1 ha dati reali, non rimosso")
        else:
            # Modellazione da zero: aggiungi L10 se mancante
            if not list(g.triples((act_iri, L10, None))):
                if list(g.triples((mdl_iri, None, None))):
                    g.add((act_iri, L10, mdl_iri))
                    n_added += 1
                    print(f"[fix_l10] Aggiunto L10: act/{nr}/{first_phase}/1 "
                          f"→ mdl/{nr}/{first_phase}/1")
                else:
                    print(f"[fix_l10] WARN: mdl/{nr}/{first_phase}/1 "
                          f"non trovato per NR={nr}")

    print(f"[fix_l10] Totale → rimossi: {n_removed} | L10 aggiunti: {n_added}")
    return g, n_removed, n_added


def add_missing_l10_from_graph(g: Graph) -> tuple[Graph, int]:
    """
    Fallback: scansiona il grafo cercando nodi act con fase numerica > 00
    che hanno dati reali (P14, P4, L23...) ma mancano di L10_had_input.
    Per ciascuno aggiunge L10_had_input → mdl/NR/phase/version.

    Copre i casi non catturati da first_phase_dict (es. oggetti senza
    OGGETTO_ESISTENTE ma che partono da una fase intermedia).
    """
    act_phase_re = re.compile(
        r"^https://w3id\.org/changes/4/aldrovandi/act/([^/]+)/(\d{2})/(\d+)$"
    )

    D10 = URIRef(CRMDIG + "D10_Software_Execution")

    n_added = 0

    for act_iri in list(g.subjects(RDF.type, D10)):
        m = act_phase_re.match(str(act_iri))
        if not m:
            continue
        nr, phase, version = m.group(1), m.group(2), m.group(3)

        if phase == "00":
            continue  # la prima fase non ha predecessore digitale

        # Ha già L10?
        if list(g.triples((act_iri, L10, None))):
            continue

        # Ha dati reali?
        if not _has_real_data(g, act_iri):
            continue

        # Cerca il mdl corrispondente
        mdl_iri = URIRef(f"{BASE}mdl/{nr}/{phase}/{version}")
        if list(g.triples((mdl_iri, None, None))):
            g.add((act_iri, L10, mdl_iri))
            n_added += 1
            print(f"[fix_l10_graph] Aggiunto L10: act/{nr}/{phase}/{version} "
                  f"→ mdl/{nr}/{phase}/{version}")
        else:
            print(f"[fix_l10_graph] WARN: mdl/{nr}/{phase}/{version} non trovato")

    print(f"[fix_l10_graph] L10 aggiunti da scansione grafo: {n_added}")
    return g, n_added


def remove_hollow_act_nodes(g: Graph) -> tuple[Graph, int]:
    """
    Scansiona il grafo cercando nodi act con fase numerica che non hanno
    dati reali (nessuna proprietà significativa come P14, P4, L23...).
    Li rimuove insieme a tutte le triple collegate.

    Copre i casi non catturati da first_phase_dict: oggetti senza
    OGGETTO_ESISTENTE ma la cui prima fase reale non è l'acquisizione
    (es. NR 85 che parte dalla modellazione).
    """
    act_phase_re = re.compile(
        r"^https://w3id\.org/changes/4/aldrovandi/act/([^/]+)/(\d{2})/(\d+)$"
    )

    D2  = URIRef(CRMDIG + "D2_Digitization_Process")
    D10 = URIRef(CRMDIG + "D10_Software_Execution")
    HOLLOW_TYPES = {D2, D10}

    n_removed = 0

    for act_type in HOLLOW_TYPES:
        for act_iri in list(g.subjects(RDF.type, act_type)):
            m = act_phase_re.match(str(act_iri))
            if not m:
                continue

            # Ha dati reali?
            if _has_real_data(g, act_iri):
                continue

            # È un guscio vuoto: rimuovi nodo e tutte le triple collegate
            triples = (list(g.triples((act_iri, None, None))) +
                       list(g.triples((None, None, act_iri))))
            for t in triples:
                g.remove(t)
            n_removed += 1
            nr, phase, ver = m.group(1), m.group(2), m.group(3)
            print(f"[fix_hollow_act] Rimosso act guscio: act/{nr}/{phase}/{ver}")

    print(f"[fix_hollow_act] Totale act guscio rimossi: {n_removed}")
    return g, n_removed