import pandas as pd
# import morph_kgc
from src.morph_kgc.__init__ import materialize
import configparser
from rdflib import Graph, Namespace, URIRef, BNode, Literal
import os
from ruamel.yaml import YAML
import re

import configparser


'''
Per questo script non è stata prevista la possibilità che avesse un input in cartella anziché csv'''

# Percorso del file di configurazione
config_path = "src/morph_kgc_changes_metadata_conversions/config.ini"

# Creazione di un oggetto ConfigParser
config = configparser.ConfigParser()

# Lettura del file di configurazione
config.read(config_path)

# Accesso al valore di file_path in DataSource1
csv_file_path = config["DataSource1"]["file_path"]

print(f"Il valore di file_path per DataSource1 è: {csv_file_path}")

# caricamento CSV
df = pd.read_csv(csv_file_path, delimiter=',', quotechar='"', encoding='utf-8')

# NaN sostituiti con stringhe vuote
df = df.fillna('')

# tutte le colonne in stringa
df = df.applymap(lambda x: str(x) if pd.notna(x) else '')

# sovrascrivere dataframe
df.to_csv(csv_file_path, index=False, quoting=1, encoding='utf-8')

config_path_tmp = config_path.split(".ini")[0] + "_tmp" + ".ini"

# Creazione di un oggetto ConfigParser
config_tmp = configparser.ConfigParser()

# Copia delle sezioni necessarie nel nuovo file
config_tmp["DataSource1"] = config["DataSource1"]
input_filepath = config_tmp["DataSource1"]["file_path"]
config_tmp["CONFIGURATION"] = config["CONFIGURATION"]
base_output_dir = config["CONFIGURATION"]["output_dir"]
sub_output_dir = os.path.join(base_output_dir, "object_dataset")
config_tmp["CONFIGURATION"]["output_dir"] = sub_output_dir

# Leggi il CSV senza specificare colonne per mantenere le intestazioni originali
df = pd.read_csv(input_filepath)
# Rimuovi tutti i caratteri di a capo (\n) dalle intestazioni delle colonne
df.columns = [col.replace('\n', ' ') for col in df.columns]
df.columns = [col.replace('  ', ' ') for col in df.columns]
df = df.fillna('')
df = df.applymap(lambda x: str(x) if pd.notna(x) else '')
# Sovrascrivi il file CSV con i nuovi nomi delle colonne
df.to_csv(input_filepath, index=False, quoting=1, encoding='utf-8')

if not os.path.exists(sub_output_dir):
    os.makedirs(sub_output_dir)

# Scrittura del nuovo file di configurazione
with open(config_path_tmp, "w") as file:
    config_tmp.write(file)

# materializzazione del grafo per questo dataset. (capire se sdoppiare le configurazioni o se aggiungere parametri o se cambiare i nomi in postprocessing)
try:
    graph = materialize(config_path_tmp)
except Exception as e:
    print(f"Errore durante la materializzazione: {e}")


###############

# file di output e il percorso di output
output_file = config_tmp['CONFIGURATION']['output_file']
output_dir = config_tmp['CONFIGURATION']['output_dir']
output_path = os.path.join(output_dir, output_file)

# grafo convertito in un oggetto rdflib.Graph se non lo è già
if not isinstance(graph, Graph):
    raise TypeError("Expected morph_kgc.materialize to return an rdflib.Graph object")

# mapping YARRRML in dizionario
yaml = YAML(typ='safe', pure=True)

map_file = config['DataSource1']['mappings']

with open(map_file, 'r', encoding='utf-8') as file:
    yarrrml_data = yaml.load(file)

# estrazione prefissi e aggiunta a grafo
prefixes = yarrrml_data.get('prefixes', {})
for prefix, uri in prefixes.items():
    graph.bind(prefix, Namespace(uri))

# il grafo in Turtle, su file output
serialization = config['CONFIGURATION']['output_serialization']

graph.serialize(destination=output_path, format=serialization)

# CORREZIONE (POSTPROCESSING)

# grafo per caricare rdf
g = Graph()
g.parse(output_path)


# lettura RDF come stringa
with open(output_path, 'r') as f:
    data = f.read()

# path del file di output
output_file_path = output_path.split(".")[0] + "_corretto.ttl"

# estrazione prefissi
def extract_prefixes(text):
    prfx = re.findall(r'@prefix\s([^\s:]+:)\s', text)
    return prfx

# regex generata sui prefissi
def generate_prefix_regex(prefixes):
    prefix_pattern = "|".join(re.escape(prefix) for prefix in prefixes)
    # pattern per match per ogni stringa tra virgolette che inizi per uno dei prefissi
    regex_pattern = rf'"({prefix_pattern})[^"]*"'
    return regex_pattern

# rimuovere le parentesi angolari da URI
def remove_angular_brackets(uri):
    return uri.strip('<>')# Funzione per rimuovere le parentesi angolari dalle URI

# rimuovere le parentesi angolari da URI
def remove_angular_apices_add_angular_brackets(uri):
    uri = remove_apices(uri)
    if uri.startswith("<") and uri.endswith(">"):
        return uri
    elif uri.startswith("<") and not uri.endswith(">"):
        return uri +">"
    elif not uri.startswith("<") and uri.endswith(">"):
        return "<"+uri
    else:
        return "<"+uri+">"

# rimuovere le virgolette da URI
def remove_apices(string):
    return string.strip('"')

# processare rdf in forma testuale
def process_rdf_data(data):
    prefixes = extract_prefixes(data)
    regex_pattern = generate_prefix_regex(prefixes)
    string_pattern = re.compile(regex_pattern)

    # matches = re.finditer(regex_pattern, data, re.MULTILINE)
    # matches_found = []
    # for matchNum, match in enumerate(matches, start=1):
    #     matches_found.append("{match}".format(matchNum=matchNum, start=match.start(), end=match.end(), match=match.group()))
    #     print("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum=matchNum, start=match.start(),
    #                                                                         end=match.end(), match=match.group()))
    # print("matches_found", matches_found)

    # regex x trovare URI
    uri_pattern = re.compile(r'<(?!http|https)([^>]+)>')
    bad_form_uri_pattern = re.compile(r"(?:[\"]<?)(https?:\/\/[^\s\"'<>\]]+)(?:>[\"]|[\">?])")

    # sostituzione uri con parentesi angolari con uri senza
    def replace_uri(match):
        return remove_angular_brackets(match.group(0))

    # sostituzione uri tra virgolette con uri in parentesi angolari
    def replace_badform_uri(match):
        return remove_angular_apices_add_angular_brackets(match.group(0))

    # sostituzione uri con virgolette con uri senza
    def replace_str(match):
        return remove_apices(match.group(0))

    # regex per sostituire tutti URI
    processed_data = uri_pattern.sub(replace_uri, data)

    # regex per sostituire tutti URI
    processed_data = string_pattern.sub(replace_str, processed_data)

    # regex per sostituire URI badformed
    processed_data = bad_form_uri_pattern.sub(replace_badform_uri, processed_data)


    return processed_data

# esecuzione trasformazione
processed_data = process_rdf_data(data)

# stampa dati RDF trasformati
print(processed_data)

# scrittura su file di dati RDF trasformati
with open(output_file_path, 'w') as f:
    f.write(processed_data)

if os.path.exists(config_path_tmp):
    os.remove(config_path_tmp)