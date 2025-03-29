import pandas as pd
# import morph_kgc
from src.morph_kgc.__init__ import materialize
import configparser
from rdflib import Graph, Namespace, URIRef, BNode, Literal
import os
from ruamel.yaml import YAML
import re
import datetime


# Trova eventuali colonne vuote nel csv
def get_empty_column_names(csv_path):
    """
    Legge un file CSV e restituisce una lista con i nomi delle colonne vuote (senza intestazione).

    Args:
        csv_path (str): Il percorso del file CSV.

    Returns:
        list: Una lista di stringhe che rappresentano i nomi delle colonne vuote.
    """
    df = pd.read_csv(csv_path, encoding='latin1')
    empty_columns = [col for col in df.columns if col.strip() == ""]
    return empty_columns

# verifica se ci sono degli ID mancanti
def compare_column_values(main_csv, secondary_csv, column_name):
    """
    Confronta i valori di una colonna specificata tra due file CSV.

    Args:
        main_csv (str): Percorso del file CSV principale.
        secondary_csv (str): Percorso del file CSV secondario.
        column_name (str): Nome della colonna da confrontare.

    Returns:
        dict: Dizionario con chiavi "missing_in_main" e "missing_in_secundary".
    """
    main_df = pd.read_csv(main_csv, encoding='latin1')
    secondary_df = pd.read_csv(secondary_csv, encoding='latin1')

    main_values = set(main_df[column_name].dropna().astype(str).str.strip())
    secondary_values = set(secondary_df[column_name].dropna().astype(str).str.strip())

    return {
        "missing_in_main": list(secondary_values - main_values),
        "missing_in_secundary": list(main_values - secondary_values)
    }


import os
import re
import datetime
import pandas as pd


def fix_person_name(cell):
    """
    Trasforma una stringa contenente uno o più nomi di persona nel formato:
      Cognome, Nome (eventuale id)  -->  Nome Cognome (eventuale id)
      I nomi multipli sono separati da ';'
      Se il nome è interamente racchiuso tra parentesi quadre, lo lascia invariato.
    """
    if pd.isna(cell):
        return cell
    s = str(cell).strip()
    if not s:
        return s
    # Se la cella contiene più nomi separati da ';'
    parts = s.split(';')
    fixed_parts = []
    # Pattern per nomi nel formato "Cognome, Nome" eventualmente seguito da uno spazio e da id tra parentesi tonde o quadre.
    pattern = re.compile(r'^\s*([^,]+),\s*([^\(\[]+)(\s*[\(\[].*[\)\]])?\s*$')
    for part in parts:
        part = part.strip()
        # Se il part è già del tipo "[...]", lascialo invariato
        if part.startswith('[') and part.endswith(']'):
            fixed_parts.append(part)
            continue
        m = pattern.match(part)
        if m:
            cognome = m.group(1).strip()
            nome = m.group(2).strip()
            id_str = m.group(3) or ""
            id_str = id_str.strip()
            # Costruisce "Nome Cognome" e, se presente, aggiunge l'id (con uno spazio prima)
            if id_str:
                fixed_parts.append(f"{nome} {cognome} {id_str}")
            else:
                fixed_parts.append(f"{nome} {cognome}")
        else:
            fixed_parts.append(part)
    return " ; ".join(fixed_parts)


def clean_value(value):
    if pd.isna(value):  # Gestisce i valori NaN
        return None
    value = str(value).strip()  # Rimuove spazi iniziali e finali
    value = re.sub(r'\s+', ' ', value)  # Sostituisce spazi multipli con uno solo
    value = re.sub(r'[\s\-_]+', '_', value)  # Sostituisce spazi, "-" e "_" con "_"
    value = value.lower()
    return value

def create_ready_csv(csv_filepath, columns_with_no_values, col_name, missing_ids, output_dir):
    """
    Elimina colonne vuote specificate e righe con valori specifici in una colonna,
    convertendo eventuali date dal formato DD/MM/YYYY in YYYY-MM-DD, applica una
    trasformazione sulle colonne relative ai nomi di persona, e salva il nuovo CSV.

    Le colonne da correggere sono:
      "Scopritore", "Autore", "Traduttore", "Disegnatore", "Incisore", "Editore",
      "Preparatore museale", "Committente"

    Args:
        csv_filepath (str): Il percorso del file CSV di input.
        columns_with_no_values (list): Lista dei nomi delle colonne da rimuovere.
        col_name (str): Nome della colonna su cui filtrare i valori da rimuovere.
        missing_ids (list): Lista dei valori nella colonna col_name da rimuovere.
        output_dir (str): Directory in cui salvare il file CSV modificato.

    Returns:
        str: Il percorso del file CSV salvato.
    """
    # Carica il dataset
    df = pd.read_csv(csv_filepath, encoding='latin1')

    # Funzione di supporto per convertire il formato data (DD/MM/YYYY -> YYYY-MM-DD)
    def convert_date(val):
        if isinstance(val, str):
            match = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", val)
            if match:
                day, month, year = match.groups()
                try:
                    dt = datetime.date(int(year), int(month), int(day))
                    return dt.isoformat()
                except Exception:
                    return val
        return val

    # Applica la conversione data a ogni cella del DataFrame
    df = df.applymap(convert_date)

    # Rimuovi le colonne specificate (se esistono)
    df = df.drop(columns=[col for col in columns_with_no_values if col in df.columns])

    # Rimuovi le righe in cui il valore della colonna col_name è presente in missing_ids
    df = df[~df[col_name].astype(str).isin([str(val) for val in missing_ids])]

    # Aggiungi qui la trasformazione dei nomi di persona nelle colonne specificate
    person_columns = ["Scopritore", "Autore", "Traduttore", "Disegnatore",
                      "Incisore", "Editore", "Preparatore museale", "Committente"]
    for col in person_columns:
        if col in df.columns:
            df[col] = df[col].apply(fix_person_name)

    # Applica la pulizia dei valori nella colonna "NR collegato"
    if "NR collegato" in df.columns:
        df["NR collegato"] = df["NR collegato"].apply(clean_value)

    # Crea il nome del nuovo file
    filename = os.path.basename(csv_filepath)
    output_path = os.path.join(output_dir, f"cleaned_{filename}")

    # Salva il nuovo file CSV
    df.to_csv(output_path, index=False, encoding='latin1')

    return output_path


# Esempio di utilizzo:
# csv_input = "path/to/your/input.csv"
# columns_to_remove = [ ... ]  # Lista delle colonne vuote da rimuovere
# missing_ids = [ ... ]  # Lista dei valori da rimuovere dalla colonna col_name
# ready_csv = create_ready_csv(csv_input, columns_to_remove, "NR", missing_ids, "path/to/output_dir")
# print(ready_csv)

# Funzione per la pulizia dinamica del file di mapping
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



# Percorso del file di configurazione
config_path = "src/morph_kgc_changes_metadata_conversions/config.ini"

# Creazione di un oggetto ConfigParser
config = configparser.ConfigParser()

# Lettura del file di configurazione
config.read(config_path)

# Accesso al valore di file_path in DataSource1
csv_file_path = config["DataSource1"]["file_path"]
coupled_csv_file_path = config["DataSource2"]["file_path"]
ready_input_dir =  config["DataSource1"]["ready_input_dir"]

columns_with_no_values = []
missing_ids = []

if os.path.isfile(csv_file_path) and csv_file_path.lower().endswith(".csv"):
    print(f"[INFO] '{csv_file_path}': Inizio elaborazione dell'input csv file.")
    columns_with_no_values = get_empty_column_names(csv_file_path)
    missing_ids = compare_column_values(csv_file_path, coupled_csv_file_path, "NR")["missing_in_main"]

else:
    print(f"[ERRORE] Il path '{csv_file_path}' non è un file CSV valido.")


ready_input = create_ready_csv(csv_file_path, columns_with_no_values, "NR", missing_ids, ready_input_dir)


df = pd.read_csv(ready_input, delimiter=',', quotechar='"', encoding='latin1')

# NaN sostituiti con stringhe vuote
df = df.fillna('')

# tutte le colonne in stringa
df = df.applymap(lambda x: str(x) if pd.notna(x) else '')

df.to_csv(ready_input, index=False, quoting=1, encoding='latin1')

#  MODO PER INVISIBILIZZARE IL DATASET NON RICHIESTO (LEGGE CONFIGURAZIONI GENERALI, CREA UN FILE DI APPOGGIO CON LE INFO NECESSARIE E CANCELLA LE ALTRE POI CANCELLA IL FILE)
config_path_tmp = config_path.split(".ini")[0] + "_tmp" + ".ini"

# Creazione di un oggetto ConfigParser
config_tmp = configparser.ConfigParser()

temp_mapping_file = config["DataSource1"]["mappings"]

if columns_with_no_values:
    keys_to_remove_from_mapping = []
    temp_mapping_file = config["DataSource1"]["mappings"].split(".yaml")[0] + "_tmp.yaml"
    original_mapping_file = config["DataSource1"]["mappings"]

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

        # per disabilitare il riordino delle chiavi (sort)
        yaml_tmp.sort_base_mapping_type = False  # A partire da ruamel.yaml 0.17+
        # Oppure, per versioni meno recenti: yaml_tmp.representer.sort_dicts = False

        # Consenti caratteri Unicode
        yaml_tmp.allow_unicode = True

        yaml_tmp.dump(full_tmp_data_dict, f)



# Copia delle sezioni necessarie nel nuovo file
config_tmp["DataSource1"] = config["DataSource1"]
config_tmp["DataSource1"]["file_path"] = ready_input
config_tmp["DataSource1"]["mappings"] = temp_mapping_file
config_tmp["CONFIGURATION"] = config["CONFIGURATION"]
base_output_dir = config["CONFIGURATION"]["output_dir"]
sub_output_dir = os.path.join(base_output_dir, "process_dataset")
config_tmp["CONFIGURATION"]["output_dir"] = sub_output_dir

# Leggi il CSV senza specificare colonne per mantenere le intestazioni originali
df = pd.read_csv(ready_input, encoding='latin1')
# Rimuovi tutti i caratteri di a capo (\n) dalle intestazioni delle colonne
df.columns = [col.replace('\n', ' ') for col in df.columns]
df.columns = [col.replace('  ', ' ') for col in df.columns]
df = df.fillna('')
df = df.applymap(lambda x: str(x) if pd.notna(x) else '')
# Sovrascrivi il file CSV con i nuovi nomi delle colonne
df.to_csv(ready_input, index=False, quoting=1, encoding='utf-8')

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
output_file = config_tmp["DataSource1"]['output_file']
#output_dir = config_tmp['CONFIGURATION']['output_dir']
output_path = os.path.join(sub_output_dir, output_file)


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
# print(processed_data)

# scrittura su file di dati RDF trasformati
with open(output_file_path, 'w') as f:
    f.write(processed_data)

if os.path.exists(config_path_tmp):
    os.remove(config_path_tmp)