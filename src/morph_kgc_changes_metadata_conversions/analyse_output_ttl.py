from rdflib import Graph, Namespace, RDF


def get_crm_e21_person_triples(file_path):
    # Crea un grafo RDF
    g = Graph()
    # Carica il file TTL usando il percorso specificato
    g.parse(file_path, format="turtle")

    # Definisci il namespace per CRM
    crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")

    # Filtra le triple che hanno rdf:type crm:E21_Person
    person_triples = list(g.triples((None, RDF.type, crm.E21_Person)))
    return person_triples


def get_D9_Data_Object_triples(file_path):
    # Crea un grafo RDF
    g = Graph()
    # Carica il file TTL usando il percorso specificato
    g.parse(file_path, format="turtle")

    # Definisci il namespace per CRM DIG
    crmdig = Namespace("http://www.ics.forth.gr/isl/CRMdig/")

    # Filtra le triple che hanno rdf:type crmdig:D9_Data_Object

    object_triples = list(g.triples((None, RDF.type, crmdig.D9_Data_Object)))
    return object_triples


if __name__ == "__main__":
    # Specifica il percorso del tuo file TTL
    file_path = "src/morph_kgc_changes_metadata_conversions/output_dir/demo_marzo/ttl_output/process_dataset/knowledge-graph_pro_corretto.ttl"
    triples = get_D9_Data_Object_triples(file_path)

    # Stampa le triple filtrate
    for s, p, o in triples:
        print(s)