import pandas as pd
from src.morph_kgc.__init__ import materialize
import configparser
from rdflib import Graph, Namespace, URIRef, BNode, Literal
from src.morph_kgc.fnml.built_in_functions import normalize_str_param
from src.morph_kgc.fnml.built_in_functions import multi_sep_string_split_explode  # importa la UDF
import os
from ruamel.yaml import YAML
import re
import datetime
from perf_monitor import PerfMonitor
import csv
import pprint
import glob

# NOTA : PROVVISORIAMENTE per il post processing si assume che il separatore INTERNO alle celle sia ;
# c'è anche un'euristica nella pulizia soggetti-oggetti fatti per le traduzioni
# che assume che se non si riesce a risalire a che tipo di caratteri introducono e chiudono la traduzione, allora "[" "]"
# --- rdflib: disabilita cast automatico di xsd:dateTime (e opzionalmente xsd:date) ---
try:
    from rdflib.term import _toPythonMapping
    from rdflib.namespace import XSD
    _toPythonMapping.pop(XSD.dateTime, None)
    _toPythonMapping.pop(XSD.date, None)  # se comparisse xsd:date
    print("[INFO] rdflib datetime cast disabled in this process")
except Exception as e:
    print(f"[WARN] rdflib datetime cast patch skipped: {e}")

def read_csv_safely(path):
    try:
        return pd.read_csv(path, encoding='utf-8')
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='latin1')

def sanitize_whitespace_df(df: pd.DataFrame) -> pd.DataFrame:
    """Trim testa/coda e rimozione di \t \r \n da TUTTE le celle stringa (vectorized)."""
    obj = df.select_dtypes(include="object").columns
    if len(obj):
        df[obj] = (
            df[obj]
            .replace({r'[\t\r\n]+': ' '}, regex=True)
            .apply(lambda col: col.str.strip())
        )
    return df


def sanitize_url_columns(df: pd.DataFrame) -> pd.DataFrame:
    url_cols = [c for c in df.columns if df[c].astype(str).str.contains(r'https?://', na=False).any()]
    for c in url_cols:
        s = df[c].astype(str)
        s = s.str.replace(r'^\s*<?\s*(https?://[^\s<>]+)\s*>?\s*$', r'\1', regex=True).str.strip()
        df[c] = s
    return df

# Trova colonne vuote nel csv
def get_empty_column_names(csv_path):
    """
    Legge un file CSV e restituisce una lista con i nomi delle colonne vuote (senza intestazione).

    Args:
        csv_path (str): Il percorso del file CSV.

    Returns:
        list: Una lista di stringhe che rappresentano i nomi delle colonne vuote.
    """
    df = read_csv_safely(csv_path)
    empty_columns = [col for col in df.columns if col.strip() == ""]
    return empty_columns

# verifica ID mancanti
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
    main_df = read_csv_safely(main_csv)
    secondary_df = read_csv_safely(secondary_csv)

    main_values = set(main_df[column_name].dropna().astype(str).str.strip())
    secondary_values = set(secondary_df[column_name].dropna().astype(str).str.strip())

    return {
        "missing_in_main": list(secondary_values - main_values),
        "missing_in_secundary": list(main_values - secondary_values)
    }



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
        # Se il part è già del tipo "[...]", invariato
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
    df = read_csv_safely(csv_filepath)

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
    df = df.apply(lambda col: col.map(convert_date))

    # Rimuove le colonne specificate (se esistono)
    df = df.drop(columns=[col for col in columns_with_no_values if col in df.columns])

    # Rimuove le righe in cui il valore della colonna col_name è presente in missing_ids
    df = df[~df[col_name].astype(str).isin([str(val) for val in missing_ids])]

    # Aggiunge trasformazione nomi di persona nelle colonne specificate
    person_columns = ["Scopritore", "Autore", "Traduttore", "Disegnatore",
                      "Incisore", "Editore", "Preparatore museale", "Committente"]
    for col in person_columns:
        if col in df.columns:
            df[col] = df[col].apply(fix_person_name)

    # Applica la pulizia dei valori nella colonna "NR collegato"
    if "NR collegato" in df.columns:
        df["NR collegato"] = df["NR collegato"].apply(clean_value)

    #  sanitize PRE-MATERIALIZZAZIONE
    df = sanitize_whitespace_df(df)
    df = sanitize_url_columns(df)

    # Crea il nome del nuovo file
    filename = os.path.basename(csv_filepath)
    output_path = os.path.join(output_dir, f"cleaned_{filename}")

    # Salvataggio il nuovo file CSV
    df.to_csv(output_path, index=False, encoding='utf-8')

    return output_path

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
        # Usa la funzione di supporto
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

# --- PERF MONITOR: setup dal config.ini ---
monitor_base = config["CONFIGURATION"].get("monitor_report", "results/monitor")
os.makedirs(monitor_base, exist_ok=True)
REPORT_PATH = os.path.join(monitor_base, "perf_report.json")
RUN_KEY = "objects"


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


df = read_csv_safely(ready_input)

# 1. sostituisci tutti i caratteri di spaziatura (TAB, CR, LF) con un singolo spazio
df.columns = df.columns.str.replace(r'\s+', ' ', regex=True)

# 2. elimina spazi a inizio/fine
df.columns = df.columns.str.strip()

# 3. rimuovi doppi spazi rimasti
df.columns = df.columns.str.replace('  ', ' ', regex=False)

df = df.fillna('')
df = sanitize_whitespace_df(df)
df = sanitize_url_columns(df)
df = df.apply(lambda col: col.map(lambda x: str(x) if pd.notna(x) else ''))
df.to_csv(ready_input, index=False, quoting=1, encoding='utf-8')

#INVISIBILIZZARE IL DATASET NON RICHIESTO (LEGGE CONFIGURAZIONI GENERALI, CREA UN FILE DI APPOGGIO CON LE INFO NECESSARIE E CANCELLA LE ALTRE POI CANCELLA IL FILE)
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
        # 'data_dict' = dizionario che rispecchia la struttura del file YAML
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
        yaml_tmp.sort_base_mapping_type = False

        # Consenti caratteri Unicode
        yaml_tmp.allow_unicode = True

        yaml_tmp.dump(full_tmp_data_dict, f)



# Copia delle sezioni necessarie nel nuovo file
config_tmp["DataSource1"] = config["DataSource1"]
config_tmp["DataSource1"]["file_path"] = ready_input
config_tmp["DataSource1"]["mappings"] = temp_mapping_file
config_tmp["CONFIGURATION"] = config["CONFIGURATION"]
base_output_dir = config["CONFIGURATION"]["output_dir"]
sub_output_dir = os.path.join(base_output_dir, "object_dataset")
config_tmp["CONFIGURATION"]["output_dir"] = sub_output_dir

# Leggi il CSV senza specificare colonne per mantenere le intestazioni originali
df = read_csv_safely(ready_input)
# Rimuovi tutti i caratteri di a capo (\n) dalle intestazioni delle colonne
df.columns = [col.replace('\n', ' ') for col in df.columns]
df.columns = [col.replace('  ', ' ') for col in df.columns]
df = df.fillna('')
df = df.apply(lambda col: col.map(lambda x: str(x) if pd.notna(x) else ''))
# Sovrascrivi il file CSV con i nuovi nomi delle colonne
df.to_csv(ready_input, index=False, quoting=1, encoding='utf-8')

if not os.path.exists(sub_output_dir):
    os.makedirs(sub_output_dir)

# Scrittura del nuovo file di configurazione
with open(config_path_tmp, "w") as file:
    config_tmp.write(file)

# materializzazione del grafo con monitor in context manager
graph = None
with PerfMonitor(label="objects") as mon:
    try:
        graph = materialize(config_path_tmp)
    except Exception as e:
        print(f"Errore durante la materializzazione: {e}")

if not isinstance(graph, Graph):
    # cleanup temporanei anche in caso di errore
    if os.path.exists(config_path_tmp): os.remove(config_path_tmp)
    if temp_mapping_file != config["DataSource1"]["mappings"] and os.path.exists(temp_mapping_file):
        os.remove(temp_mapping_file)
    raise SystemExit(1)
# rows_processed: numero di righe dell'input "ready" effettivamente usato
try:
    rows_processed = len(read_csv_safely(ready_input))
except Exception:
    rows_processed = None

# --- PERF MONITOR: calcolo metriche e update JSON ---
# rows_processed: numero di righe dell'input "ready" effettivamente usato

# iri_generated: conteggio IRI dal grafo restituito da materialize (se disponibile)
try:
    iris_generated = mon.count_iris_from_graph(graph, mode="subjects_unique")
except Exception:
    iris_generated = None


results = mon.report(rows_processed, iris_generated)

# Scrivi/aggiorna la sezione del JSON condiviso per questo run
PerfMonitor.update_json_report(
    REPORT_PATH,
    run_key=RUN_KEY,
    results=results,
    extra_fields={
        "config_used": config_path_tmp,
        "ready_input": ready_input,
        "output_dir": config["CONFIGURATION"].get("output_dir"),
        "report_dir": monitor_base,
    }
)

###############

# file di output e il percorso di output
output_file = config_tmp["DataSource1"]['output_file']
output_path = os.path.join(sub_output_dir, output_file)


# grafo convertito in un oggetto rdflib.Graph se non lo è già
if not isinstance(graph, Graph):
    raise TypeError("Expected morph_kgc.materialize to return an rdflib.Graph object")

# mapping YARRRML in dizionario
yaml = YAML(typ='safe', pure=True)

map_file = temp_mapping_file

with open(map_file, 'r', encoding='utf-8') as file:
    yarrrml_data = yaml.load(file)

# estrazione prefissi e aggiunta a grafo
prefixes = yarrrml_data.get('prefixes', {})
prefixes_dict_from_yaml = dict()
for prefix, uri in prefixes.items():
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


# ELIMINAZIONE OGGETTI NON APPAIATI CON SOGGETTI
def pair_subject_object(first_ver_graph: Graph, properties_list: list) -> Graph:
    '''
    1) leggere il grafo,
    2) identificare le triple con quei predicati della lista,
    3) verificare se la struttura della tripla rientra in una di queste due condizioni:
        a) soggetto è un iri e finisce con una struttura che finisce con "/sub/<string_variable>/<version_number>"  + predicate è crm:P1_is_identified_by + oggetto ha struttura "/apl/<string_variable>/<version_number>"
        b) soggetto è un iri e finisce con una struttura che finisce con "/apl/<string_variable>/<version_number>" + predicate è crm:P190_has_symbolic_content + oggetto è una stringa.
    4) nel caso a), identificare le <string_variable> di soggetto e oggetto e verificare che siano uguali. Nel caso in cui non fosse così, eliminare la tripla dal grafo. Nel caso b) identificare la <string_variable> del soggetto e verificare se è uguale al valore dell'oggetto stringa, normalizzato con la funzione normalize_str_param(str_param) importata dal file src/morph_kgc/fnml/built_in_functions.py. Se i due valori non sono uguali, eliminare la tripla.
    5) ritornare il grafo corretto
    '''

    # --- Lettura del path ready_input_dir da config.ini ---
    config_path = os.path.join("src", "morph_kgc_changes_metadata_conversions", "config.ini")
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")

    ready_input_dir = ""
    if config.has_section("DataSource1") and config.has_option("DataSource1", "ready_input_dir"):
        ready_input_dir = config.get("DataSource1", "ready_input_dir").strip()
    # ------------------------------------------------------

    # --- Costruzione iniziale del dizionario normalised_translations ---
    normalised_translations = {}
    if ready_input_dir and os.path.isdir(ready_input_dir):
        csv_paths = glob.glob(os.path.join(ready_input_dir, "**", "*.csv"), recursive=True)
        for path in csv_paths:
            try:
                with open(path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames:
                        continue
                    target_col = next(
                        (h for h in reader.fieldnames if (h or "").strip().lower() == "soggetti"),
                        None
                    )
                    if not target_col:
                        continue
                    for row in reader:
                        cell = row.get(target_col, "")
                        if not isinstance(cell, str) or not cell.strip():
                            continue

                        # Usa multi_sep_string_split_explode per separare solo al ";"
                        splitted_values = multi_sep_string_split_explode(
                            string=cell,
                            separators_list_str=";",
                            translation_container_chars=None,
                            consider_translation=False
                        )

                        # Ogni voce originale resta come chiave, con valore normalizzato
                        for value in splitted_values:
                            clean_key = value.strip()
                            if clean_key:
                                normalised_translations[clean_key] = {
                                    "orig": normalize_str_param(clean_key)
                                }

            except Exception as e:
                print(f"[WARN] Errore in {path}: {e}")
                continue
    # -------------------------------------------------------------------

    # --- Trasformazione in nuovo dizionario {norm_key: {"orig": ..., "tr": ...}} ---
    def _detect_translation_delims(keys):
        """
        Heuristica: >1/3 delle chiavi termina con ']' -> usa '[' ']';
                    altrimenti >1/3 termina con '>' -> usa '<' '>';
                    altrimenti >1/3 termina con ')' -> usa '(' ')';
                    default '[' ']'.
        """
        total = len(keys) or 1
        endings = {']': 0, '>': 0, ')': 0}
        for k in keys:
            s = (k or "").strip()
            if s.endswith(']'):
                endings[']'] += 1
            elif s.endswith('>'):
                endings['>'] += 1
            elif s.endswith(')'):
                endings[')'] += 1
        threshold = total / 3.0
        if endings[']'] > threshold:
            return '[', ']'
        if endings['>'] > threshold:
            return '<', '>'
        if endings[')'] > threshold:
            return '(', ')'
        return '[', ']'  # default

    def _extract_orig_tr(label: str, open_char: str, close_char: str):
        """
        Estrae (orig, tr) da una chiave tipo 'Orig [Tr]','Orig <<<Tr>>>','Orig (((Tr)))'.
        Supporta ripetizioni del delimitatore. Se assente/inefficace -> (label, None).
        """
        if not isinstance(label, str):
            return label, None
        s = label.strip()
        if not s:
            return s, None

        # conta ripetizioni consecutive del close_char in coda
        t = 0
        i = len(s) - 1
        while i >= 0 and s[i] == close_char:
            t += 1
            i -= 1
        if t == 0:
            return s, None  # nessuna traduzione rilevata

        close_sep = close_char * t
        open_sep = open_char * t

        end_inner = len(s) - t
        start = s.rfind(open_sep, 0, end_inner)
        if start == -1:
            return s, None  # apertura non trovata

        inner = s[start + len(open_sep): end_inner].strip()
        outer = s[:start].strip()

        orig = outer if outer else s
        tr = inner if inner else None
        return orig, tr

    open_char, close_char = _detect_translation_delims(list(normalised_translations.keys()))

    new_translations = {}
    for key, val in normalised_translations.items():
        norm_key = val.get("orig")
        if not norm_key:
            continue
        orig_label, tr_label = _extract_orig_tr(key, open_char, close_char)
        new_translations[norm_key] = {"orig": orig_label, "tr": tr_label}

    # Stampa solo il nuovo dizionario
    print(pprint.pformat(new_translations, width=120))
    # -------------------------------------------------------------------

    triples_to_remove = []

    for s, p, o in first_ver_graph:
        pred = str(p)

        if pred not in properties_list:
            continue

        subj_str = str(s)
        obj_str = str(o)

        # ----- CASO A -----
        if pred == "http://www.cidoc-crm.org/cidoc-crm/P1_is_identified_by":
            sub_match = re.match(r".*/(sub|acr|col)/([^/]+)/\d+$", subj_str)
            obj_match = re.match(r".*/apl/([^/]+)/\d+$", obj_str)
            if sub_match and obj_match:
                sub_value = sub_match.group(2)
                obj_value = obj_match.group(1)
                if sub_value != obj_value:
                    triples_to_remove.append((s, p, o))

        # ----- CASO B -----
        elif pred == "http://www.cidoc-crm.org/cidoc-crm/P190_has_symbolic_content":
            sub_match = re.match(r".*/apl/([^/]+)/\d+$", subj_str)
            if sub_match and isinstance(o, Literal):
                sub_value = sub_match.group(1)
                normalized_obj_value = normalize_str_param(str(o))
                if sub_value != normalized_obj_value:

                    translations_from_dict = new_translations.get(sub_value)
                    if not translations_from_dict:
                        print(sub_value, " not in ", sorted(new_translations.keys()))
                        # RIMUOVERE DIRETTAMENTE TRIPLA
                        triples_to_remove.append((s, p, o))

                    else: # tentare di verificare se si tratta di una traduzione
                        original = translations_from_dict.get("or")
                        translation = translations_from_dict.get("tr")
                        normalized_obj_value_TRSL = normalize_str_param(str(translation))
                        normalized_obj_value_ORIG = normalize_str_param(str(original))

                        if normalized_obj_value != normalized_obj_value_TRSL:
                            #print(sub_value, "!=", normalized_obj_value, "AND", normalized_obj_value, "!=", normalized_obj_value_TRSL)
                            # TRIPLA DA RIMUOVERE: NON è Né STESSO SOGGETTO nella stessa lingia Né UNA SUA TRADUZIONE
                            triples_to_remove.append((s, p, o))

                        else: # normalized_obj_value == normalized_obj_value_TRSL, quindi è una traduzione
                            print(sub_value, "!=", normalized_obj_value, "!!!!!!  BUT !!!!!!!", normalized_obj_value, "==", normalized_obj_value_TRSL)


    for triple in triples_to_remove:
        first_ver_graph.remove(triple)

    return first_ver_graph


properties_in_triples_to_clean = [
    "http://www.cidoc-crm.org/cidoc-crm/P1_is_identified_by",
    "http://www.cidoc-crm.org/cidoc-crm/P190_has_symbolic_content"
]

# Applica la pulizia sul grafo
g_clean = pair_subject_object(g, properties_in_triples_to_clean)

# Scrivi su file il grafo corretto in TTL
output_file_path = output_path.split(".")[0] + "_corretto.ttl"
g_clean.serialize(destination=output_file_path, format="turtle")

# Lettura RDF corretto come stringa
with open(output_file_path, 'r') as f:
    data = f.read()

def extract_prefixes(text):
    prfx = re.findall(r'@prefix\s([^\s:]+:)\s', text)
    return prfx

def generate_prefix_regex(prefixes):
    prefix_pattern = "|".join(re.escape(prefix) for prefix in prefixes)
    regex_pattern = rf'"({prefix_pattern})[^"]*"'
    return regex_pattern

def remove_angular_brackets(uri):
    return uri.strip('<>')

def remove_apices(string):
    return string.strip('"')

def remove_angular_apices_add_angular_brackets(uri):
    uri = remove_apices(uri)
    if uri.startswith("<") and uri.endswith(">"):
        return uri
    elif uri.startswith("<") and not uri.endswith(">"):
        return uri + ">"
    elif not uri.startswith("<") and uri.endswith(">"):
        return "<" + uri
    else:
        return "<" + uri + ">"

def process_rdf_data(data):
    prefixes = extract_prefixes(data)
    regex_pattern = generate_prefix_regex(prefixes)
    string_pattern = re.compile(regex_pattern)

    uri_pattern = re.compile(r'<(?!http|https)([^>]+)>')
    bad_form_uri_pattern = re.compile(r"(?:[\"]<?)(https?:\/\/[^\s\"'<>\]]+)(?:>[\"]|[\">?])")

    def replace_uri(match):
        return remove_angular_brackets(match.group(0))

    def replace_badform_uri(match):
        return remove_angular_apices_add_angular_brackets(match.group(0))

    def replace_str(match):
        return remove_apices(match.group(0))

    processed_data = uri_pattern.sub(replace_uri, data)
    processed_data = string_pattern.sub(replace_str, processed_data)
    processed_data = bad_form_uri_pattern.sub(replace_badform_uri, processed_data)
    processed_data = re.sub(r'(?<!<)(https?://[^\s<>"]+)(?!>)', r'<\1>', processed_data)

    return processed_data

# Esecuzione postprocessing testuale
processed_data = process_rdf_data(data)

# Scrittura su file del risultato finale
with open(output_file_path, 'w') as f:
    f.write(processed_data)

# Rimozione file temporaneo config
if os.path.exists(config_path_tmp):
    os.remove(config_path_tmp)

if temp_mapping_file != config["DataSource1"]["mappings"] and os.path.exists(temp_mapping_file):
    os.remove(temp_mapping_file)
