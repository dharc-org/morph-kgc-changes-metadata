from __future__ import annotations

# L'application profile resta remoto: e' un riferimento esterno/canonico, non
# qualcosa che questo repo produce.
DEFAULT_CHAD_AP_TTL_URL = (
    "https://raw.githubusercontent.com/dharc-org/chad-ap/main/docs/current/chad-ap.ttl"
)
# Come modulo interno, il target di validazione di default e' il grafo che
# questa stessa pipeline produce localmente (results/merged_graph_output.ttl,
# scritto da run_unified_pipeline.py), non piu' una copia remota su GitHub —
# evita un giro remoto per validare un file gia' presente in locale, e resta
# sempre allineato all'ultima run piuttosto che all'ultimo push.
# Il nome della costante resta invariato per non toccare i punti che la usano;
# accetta comunque anche un URL http(s) se passato esplicitamente via CLI.
DEFAULT_PUBLIC_GRAPH_URL = "results/merged_graph_output_aldrovandi.ttl"
DEFAULT_SHAPES_BASE = "https://w3id.org/skg-if/shapes/"
DC_DESCRIPTION = "http://purl.org/dc/elements/1.1/description"
PROPERTY_PATTERN = r"([\w:-]+) -\[(\d+|[*N])(\.\.)?(\d+|[*N])?]->\s+([\w:-]+)"
ROOT_CLASSES = {
    "agent": "http://xmlns.com/foaf/0.1/Agent",
    "data-source": "http://www.w3.org/ns/dcat#DataService",
    "grant": "http://purl.org/cerif/frapo/Grant",
    "research-product": "http://purl.org/spar/fabio/Work",
    "topic": "http://purl.org/spar/fabio/SubjectTerm",
    "venue": "http://purl.org/spar/fabio/ExpressionCollection",
}
