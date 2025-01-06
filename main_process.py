import pandas as pd
# import morph_kgc
from src.morph_kgc.__init__ import materialize
import configparser
from rdflib import Graph, Namespace, URIRef, BNode, Literal
import os
from ruamel.yaml import YAML
import re

def get_stanza_number(filename: str) -> str:
    """
    Se nel nome del file è presente una stringa del tipo 'STANZA <numero>',
    estrae e restituisce il numero come stringa. Altrimenti, restituisce ''.
    Esempi di matching:
      - STANZA 3.csv  => "3"
      - stanza10.csv  => "10"
      - Test_STANZA 25.csv => "25"
    """
    # Pattern che cerca la parola 'stanza' (maiuscolo o minuscolo) seguita da
    # uno o più spazi facoltativi e poi un numero.
    pattern = re.compile(r"[Ss][Tt][Aa][Nn][Zz][Aa]\s*(\d+)")
    match = pattern.search(filename)
    if match:
        return match.group(1)
    return ""

def process_single_csv(csv_path: str):
    """
    Legge un singolo file CSV, controlla se esiste la colonna 'STANZA';
    se non esiste, la crea. Se dal nome del file si può estrarre un valore
    per la stanza, riempie la colonna con quel valore, altrimenti rimane vuota.
    Inoltre verifica se ci sono colonne completamente vuote e le segnala.
    Salva il risultato in un nuovo file con suffisso '_merged.csv'.
    """
    stanza_number = get_stanza_number(os.path.basename(csv_path))
    df = pd.read_csv(csv_path, delimiter=",", quotechar='"', encoding="utf-8")

    # 1. Controllo delle colonne completamente vuote
    empty_columns = []
    for col in df.columns:
        # Converte stringhe vuote in NaN, poi verifica se tutta la colonna è NaN
        if df[col].replace('', float('NaN')).isna().all():
            empty_columns.append(col)

    if empty_columns:
        print(f"[INFO] Nel file '{os.path.basename(csv_path)}' le colonne {empty_columns} sono completamente vuote.")
        # Se desideri rimuovere le colonne vuote, decommenta la riga seguente:
        # df.drop(columns=empty_columns, inplace=True)

    # 2. Gestione della colonna "STANZA"
    if "STANZA" not in df.columns:
        df["STANZA"] = ""
    if stanza_number:
        df["STANZA"] = stanza_number

    # 3. Salvataggio del file
    output_path = csv_path.split(".csv")[0] + "_merged.csv"
    df.to_csv(output_path, index=False, quoting=1, encoding="utf-8")
    print(f"[OK] Elaborato file singolo: {output_path}")

    return output_path, empty_columns

def process_folder(folder_path: str):
    """
    Legge tutti i file .csv in folder_path, per ognuno se trova 'STANZA <numero>'
    nel nome, aggiunge/riempie la colonna 'STANZA'. Infine concatena
    tutti i DataFrame in un unico CSV.
    """
    merged_filepath = folder_path + "_merged.csv"
    dfs = []
    for filename in os.listdir(folder_path):
        if filename.lower().endswith(".csv"):
            full_path = os.path.join(folder_path, filename)
            stanza_number = get_stanza_number(filename)

            df = pd.read_csv(full_path, delimiter=",", quotechar='"', encoding="utf-8")

            # Esempio di controllo colonne vuote:
            empty_columns = []
            for col in df.columns:
                # Sostituiamo le stringhe vuote con NaN, poi verifichiamo se tutta la colonna è NaN
                if df[col].replace('', float('NaN')).isna().all():
                    empty_columns.append(col)

            if empty_columns:
                print(f"[INFO] Nel file '{filename}' le colonne {empty_columns} sono completamente vuote.")

            # Se la colonna "STANZA" non esiste, la creiamo.
            if "STANZA" not in df.columns:
                df["STANZA"] = ""

            # Se abbiamo estratto un numero, riempiamo la colonna.
            if stanza_number:
                df["STANZA"] = stanza_number

            dfs.append(df)
            print(f"[OK] Elaborato file: {filename}")

    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df.to_csv(merged_filepath, index=False, quoting=1, encoding="utf-8")
        print(f"[OK] Generato file unico: {merged_filepath}")
    else:
        print("[WARN] Nessun file CSV trovato nella cartella.")

    return merged_filepath, empty_columns


def get_all_values(subdata):
    """
    Funzione di supporto ricorsiva:
    data in input (subdata) può essere:
      - un dict (dizionario)
      - una list (lista)
      - un valore semplice (stringa, numero, ecc.)
    Restituisce una lista di tutti i valori trovati.
    """
    values = []
    if isinstance(subdata, dict):
        # Se è un dizionario, scorre tutti i suoi valori
        for val in subdata.values():
            values.extend(get_all_values(val))
    elif isinstance(subdata, list):
        # Se è una lista, scorre tutti i suoi elementi
        for item in subdata:
            values.extend(get_all_values(item))
    else:
        # Caso base: è un valore semplice (stringa, numero, bool, ecc.)
        values.append(subdata)
    return values


def get_all_values_from_nested_dict(data):
    """
    Restituisce un dizionario con le chiavi di primo livello di 'data'
    e come valore di ciascuna chiave una lista contenente TUTTI i valori
    (di qualunque tipo) incontrati all'interno della sottostruttura corrispondente.

    Esempio:
      data = {
        "prefixes": {
          "aat": "http://vocab.getty.edu/page/aat/",
          "ex": "https://w3id.org/dharc/ontology/chad-ap/data/example/"
        },
        "mappings": {
          "responsible_group_acquisition": {
            "sources": ["sample_input_file.csv~csv"],
            "s": {
              "function": "idlab-fn:normalize_and_convert_to_iri",
              "parameters": [ ... ]
            }
          }
        }
      }

      Risultato atteso, per esempio:
      {
        "prefixes": [
          "http://vocab.getty.edu/page/aat/",
          "https://w3id.org/dharc/ontology/chad-ap/data/example/"
        ],
        "mappings": [
          "sample_input_file.csv~csv",
          "idlab-fn:normalize_and_convert_to_iri",
          ...
        ]
      }
    """
    result = {}
    for top_key, subdict in data.items():
        # Usa la funzione di supporto per "flattenare" tutti i valori
        flattened_values = get_all_values(subdict)
        result[top_key] = flattened_values
    return result

# Percorso del file di configurazione
config_path = "src/morph_kgc_changes_metadata_conversions/config.ini"

# Creazione di un oggetto ConfigParser
config = configparser.ConfigParser()

# Lettura del file di configurazione
config.read(config_path)

# Accesso al valore di file_path in DataSource2
csv_file_path = config["DataSource2"]["file_path"]

columns_with_no_values = []
if os.path.isdir(csv_file_path):
    print(f"[INFO] '{csv_file_path}' è una cartella. Inizio elaborazione multipla.")
    input_unified = process_folder(csv_file_path)[0]
    columns_with_no_values = process_folder(csv_file_path)[1]

elif os.path.isfile(csv_file_path) and csv_file_path.lower().endswith(".csv"):
    print(f"[INFO] '{csv_file_path}' è un singolo file CSV. Inizio elaborazione singola.")
    input_unified = process_single_csv(csv_file_path)[0]
    columns_with_no_values = process_single_csv(csv_file_path)[1]

else:
    print(f"[ERRORE] Il path '{csv_file_path}' non è una cartella né un file CSV valido.")


df = pd.read_csv(input_unified, delimiter=',', quotechar='"', encoding='utf-8')

# NaN sostituiti con stringhe vuote
df = df.fillna('')

# tutte le colonne in stringa
df = df.applymap(lambda x: str(x) if pd.notna(x) else '')

df.to_csv(input_unified, index=False, quoting=1, encoding='utf-8')

#  MODO PER INVISIBILIZZARE IL DATASET NON RICHIESTO (LEGGE CONFIGURAZIONI GENERALI, CREA UN FILE DI APPOGGIO CON LE INFO NECESSARIE E CANCELLA LE ALTRE POI CANCELLA IL FILE)
config_path_tmp = config_path.split(".ini")[0] + "_tmp" + ".ini"

# Creazione di un oggetto ConfigParser
config_tmp = configparser.ConfigParser()

temp_mapping_file = config["DataSource2"]["mappings"]

if columns_with_no_values:
    keys_to_remove_from_mapping = []
    temp_mapping_file = config["DataSource2"]["mappings"].split(".yaml")[0] + "_tmp.yaml"
    original_mapping_file = config["DataSource2"]["mappings"]

    with open(original_mapping_file, "r", encoding="utf-8") as f:
        yaml_mod = YAML()
        original_data_dict = yaml_mod.load(f)
        # Ora 'data_dict' è un dizionario Python che rispecchia la struttura del file YAML
        mapping_keys_simplified_dict = get_all_values_from_nested_dict(original_data_dict["mappings"])

        for k, v in mapping_keys_simplified_dict.items():
            for x in v:
                x_clean = x[2:] if x.startswith("$(") else x
                x_clean = x_clean[:-1] if x.endswith(")") else x_clean
                if x_clean in columns_with_no_values:
                    keys_to_remove_from_mapping.append(k)

        full_tmp_data_dict = dict()
        tmp_mappings_dict = dict()

        for c, w in original_data_dict["mappings"].items():
            if c not in keys_to_remove_from_mapping:
                tmp_mappings_dict[c] = w

        full_tmp_data_dict["prefixes"] = original_data_dict["prefixes"]
        full_tmp_data_dict["mappings"] = tmp_mappings_dict

    with open(temp_mapping_file, "w", encoding="utf-8") as f:
        yaml_tmp = YAML()

        # Stile a blocchi (anziché flow style)
        yaml_tmp.default_flow_style = False

        # Se vuoi disabilitare il riordino delle chiavi (sort)
        yaml_tmp.sort_base_mapping_type = False  # A partire da ruamel.yaml 0.17+
        # Oppure, per versioni meno recenti, potresti dover usare:
        # yaml_tmp.representer.sort_dicts = False

        # Consenti caratteri Unicode
        yaml_tmp.allow_unicode = True

        yaml_tmp.dump(full_tmp_data_dict, f)



# Copia delle sezioni necessarie nel nuovo file
config_tmp["DataSource2"] = config["DataSource2"]
config_tmp["DataSource2"]["file_path"] = input_unified
config_tmp["DataSource2"]["mappings"] = temp_mapping_file
config_tmp["CONFIGURATION"] = config["CONFIGURATION"]
base_output_dir = config["CONFIGURATION"]["output_dir"]
sub_output_dir = os.path.join(base_output_dir, "process_dataset")
config_tmp["CONFIGURATION"]["output_dir"] = sub_output_dir

# Leggi il CSV senza specificare colonne per mantenere le intestazioni originali
df = pd.read_csv(input_unified)
# Rimuovi tutti i caratteri di a capo (\n) dalle intestazioni delle colonne
df.columns = [col.replace('\n', ' ') for col in df.columns]
df.columns = [col.replace('  ', ' ') for col in df.columns]
df = df.fillna('')
df = df.applymap(lambda x: str(x) if pd.notna(x) else '')
# Sovrascrivi il file CSV con i nuovi nomi delle colonne
df.to_csv(input_unified, index=False, quoting=1, encoding='utf-8')

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

map_file = config['DataSource2']['mappings']

with open(map_file, 'r', encoding='utf-8') as file:
    yarrrml_data = yaml.load(file)

# estrazione prefissi e aggiunta a grafo
prefixes = yarrrml_data.get('prefixes', {})
prefixes_dict_from_yaml = dict()
for prefix, uri in prefixes.items():
    # @prefix aat: <http://vocab.getty.edu/page/aat/> .
    prefix_for_output_format = re.sub('http', '<http',  uri)
    prefix_for_output_format = prefix_for_output_format+"> ."
    prefixes_dict_from_yaml[prefix] = f'@prefix {prefix}: {prefix_for_output_format}'
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
def extract_prefixes(text, supplementary_dict):
    are_there_missing_prefixes = False
    prfx = re.findall(r'@prefix\s([^\s:]+:)\s', text)
    supplementary_prefixes = [x for x in supplementary_dict.keys() if x not in prfx]
    if supplementary_prefixes:
        are_there_missing_prefixes = True
    prfx = prfx + supplementary_prefixes
    return prfx, are_there_missing_prefixes

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

def process_rdf_data(data, extract_prefixes, supl_dict):
    """
    Processa i dati RDF in forma testuale, sostituendo URI malformati e gestendo i prefissi mancanti.

    Se `missing` è True, rimuove tutte le righe contenenti "@prefix" e aggiunge all'inizio
    del file tutte le righe del dizionario `supl_dict`.

    Parameters:
    - data (str): I dati RDF in formato testuale.
    - extract_prefixes (function): Funzione per estrarre i prefissi e verificare se mancano.
    - supl_dict (dict): Dizionario contenente le righe di prefissi da aggiungere se mancanti.

    Returns:
    - str: I dati RDF processati.
    """
    # Estrai i prefissi e verifica se mancano
    prefixes, missing = extract_prefixes(data, supl_dict)

    # Genera il pattern regex basato sui prefissi estratti
    regex_pattern = generate_prefix_regex(prefixes)
    string_pattern = re.compile(regex_pattern)

    # Definisci le espressioni regolari per trovare URI
    uri_pattern = re.compile(r'<(?!http|https)([^>]+)>')
    bad_form_uri_pattern = re.compile(r'(?:["]<?)(https?:\/\/[^\s"\'<>\\]]+)(?:>["]|[\">?])')
    bad_form_uri_pattern_url_extra = re.compile(r'"(<(https?:\/\/[^>]+)>)"')


    # Definisci le funzioni di sostituzione
    def replace_uri(match):
        return remove_angular_brackets(match.group(0))

    def replace_badform_uri(match):
        return remove_angular_apices_add_angular_brackets(match.group(0))

    def replace_str(match):
        return remove_apices(match.group(0))

    # Applica le sostituzioni
    processed_data = uri_pattern.sub(replace_uri, data)
    processed_data = string_pattern.sub(replace_str, processed_data)
    processed_data = bad_form_uri_pattern.sub(replace_badform_uri, processed_data)
    processed_data = bad_form_uri_pattern_url_extra.sub(replace_str, processed_data)

    if missing:
        # Dividi i dati in singole righe
        lines = processed_data.splitlines()

        # Rimuovi tutte le righe che contengono "@prefix"
        lines = [line for line in lines if "@prefix" not in line]

        # Estrai i valori dal dizionario `supl_dict` come righe
        supl_lines = [value for key, value in supl_dict.items()]

        # Combina le righe dei prefissi supplementari con le righe rimanenti
        # Le righe di `supl_dict` vengono aggiunte all'inizio
        processed_data = "\n".join(supl_lines + lines)

    return processed_data
# esecuzione trasformazione
processed_data = process_rdf_data(data, extract_prefixes, prefixes_dict_from_yaml)

# stampa dati RDF trasformati
#print(processed_data)

# scrittura su file di dati RDF trasformati
with open(output_file_path, 'w') as f:
    f.write(processed_data)

if os.path.exists(config_path_tmp):
    os.remove(config_path_tmp)
if os.path.exists(temp_mapping_file):
    os.remove(temp_mapping_file)


