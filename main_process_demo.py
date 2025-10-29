import pandas as pd
from src.morph_kgc.__init__ import materialize
import configparser
from rdflib import Graph, Namespace, URIRef, BNode, Literal
import os
from ruamel.yaml import YAML
import re
import datetime
from perf_monitor import PerfMonitor
from rdflib.namespace import XSD


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
    """
    Individua colonne con URL e rimuove spazi/<> attaccati: '< https://.. >' -> 'https://..'
    """
    url_cols = [c for c in df.columns if df[c].astype(str).str.contains(r'https?://', na=False).any()]
    for c in url_cols:
        s = df[c].astype(str)
        s = s.str.replace(r'^\s*<?\s*(https?://[^\s<>]+)\s*>?\s*$', r'\1', regex=True).str.strip()
        df[c] = s
    return df


# Trova eventuali colonne vuote nel csv
def get_empty_column_names(csv_path):
    """
    Legge un file CSV e restituisce una lista con i nomi delle colonne vuote (senza intestazione).

    Args:
        csv_path (str): Il percorso del file CSV.

    Returns:
        list: Una lista di stringhe che rappresentano i nomi delle colonne vuote.
    """
    df = read_csv_safely(csv_path)
    return [col for col in df.columns if col.strip() == ""]

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
    main_df = read_csv_safely(main_csv)
    secondary_df = read_csv_safely(secondary_csv)

    main_values = set(main_df[column_name].dropna().astype(str).str.strip())
    secondary_values = set(secondary_df[column_name].dropna().astype(str).str.strip())

    return {
        "missing_in_main": list(secondary_values - main_values),
        "missing_in_secundary": list(main_values - secondary_values)
    }

# Prepara il nuovo csv preprocessato, senza colonne vuote e senza row disallineate tra i due dataset
def create_ready_csv(csv_filepath, columns_with_no_values, col_name, missing_ids, output_dir):
    """
    Elimina colonne vuote specificate e righe con valori specifici in una colonna,
    convertendo eventuali date al formato ISO YYYY-MM-DD, poi salva il nuovo CSV.
    """
    df = read_csv_safely(csv_filepath)

    def convert_date(val):
        if not isinstance(val, str):
            return val
        s = val.strip()
        if not s:
            return s

        # già ISO YYYY-MM-DD ?
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
        if m:
            return s

        # DD/MM/YYYY o MM/DD/YYYY
        m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s)
        if m:
            a, b, year = m.groups()
            d1, d2 = int(a), int(b)
            if d1 > 12 and d2 <= 12:
                day, month = d1, d2  # DD/MM/YYYY
            elif d2 > 12 and d1 <= 12:
                day, month = d2, d1  # MM/DD/YYYY
            else:
                day, month = d1, d2  # default: DD/MM/YYYY
            try:
                return datetime.date(int(year), int(month), int(day)).isoformat()
            except ValueError:
                return s

        for fmt in ("%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d"):
            try:
                return datetime.datetime.strptime(s, fmt).date().isoformat()
            except Exception:
                pass

        return s

    # Applicazioni
    df = df.apply(lambda col: col.map(convert_date))
    df = df.drop(columns=[col for col in columns_with_no_values if col in df.columns])
    df = df[~df[col_name].astype(str).isin([str(val) for val in missing_ids])]

    # Sanitizzazione uniforme
    df = sanitize_whitespace_df(df)
    df = sanitize_url_columns(df)

    # Normalizza intestazioni
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True).str.strip()
    df.columns = df.columns.str.replace('  ', ' ', regex=False)

    # Cast stringhe coerente
    df = df.fillna('')
    df = df.apply(lambda col: col.map(lambda x: str(x) if pd.notna(x) else ''))

    # Output sempre utf-8
    filename = os.path.basename(csv_filepath)
    output_path = os.path.join(output_dir, f"cleaned_{filename}")
    df.to_csv(output_path, index=False, quoting=1, encoding='utf-8')
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

# --- PERF MONITOR: setup dal config.ini ---
monitor_base = config["CONFIGURATION"].get("monitor_report", "results/monitor")  # es: "results/monitor"
os.makedirs(monitor_base, exist_ok=True)
REPORT_PATH = os.path.join(monitor_base, "perf_report.json")
RUN_KEY = "process"


# Accesso al valore di file_path in DataSource2
csv_file_path = config["DataSource2"]["file_path"]
coupled_csv_file_path = config["DataSource1"]["file_path"]
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
df = df.fillna('')
df = sanitize_whitespace_df(df)
df = sanitize_url_columns(df)
df = df.apply(lambda col: col.map(lambda x: str(x) if pd.notna(x) else ''))
df.to_csv(ready_input, index=False, quoting=1, encoding='utf-8')

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

        # per disabilitare il riordino delle chiavi (sort)
        yaml_tmp.sort_base_mapping_type = False

        # Consenti caratteri Unicode
        yaml_tmp.allow_unicode = True

        yaml_tmp.dump(full_tmp_data_dict, f)



# Copia delle sezioni necessarie nel nuovo file
config_tmp["DataSource2"] = config["DataSource2"]
config_tmp["DataSource2"]["file_path"] = ready_input
config_tmp["DataSource2"]["mappings"] = temp_mapping_file
config_tmp["CONFIGURATION"] = config["CONFIGURATION"]
base_output_dir = config["CONFIGURATION"]["output_dir"]
sub_output_dir = os.path.join(base_output_dir, "process_dataset")
config_tmp["CONFIGURATION"]["output_dir"] = sub_output_dir

df = read_csv_safely(ready_input)
df.columns = [col.replace('\n', ' ') for col in df.columns]
df.columns = [col.replace('  ', ' ') for col in df.columns]
df = df.fillna('')
df = df.apply(lambda col: col.map(lambda x: str(x) if pd.notna(x) else ''))
df.to_csv(ready_input, index=False, quoting=1, encoding='utf-8')

if not os.path.exists(sub_output_dir):
    os.makedirs(sub_output_dir)

# Scrittura del nuovo file di configurazione
with open(config_path_tmp, "w") as file:
    config_tmp.write(file)

# materializzazione del grafo per questo dataset. (capire se sdoppiare le configurazioni o se aggiungere parametri o se cambiare i nomi in postprocessing)

graph = None
with PerfMonitor(label="process") as mon:
    try:
        graph = materialize(config_path_tmp)
    except Exception as e:
        print(f"Errore durante la materializzazione: {e}")

if not isinstance(graph, Graph):
    # cleanup temporanei anche in caso di errore
    if os.path.exists(config_path_tmp):
        os.remove(config_path_tmp)
    if temp_mapping_file != config["DataSource2"]["mappings"] and os.path.exists(temp_mapping_file):
        os.remove(temp_mapping_file)
    raise SystemExit(1)


# --- PERF MONITOR: calcolo metriche e update JSON ---
# rows_processed: numero di righe dell'input ready usato
try:
    rows_processed = len(read_csv_safely(ready_input))
except Exception:
    rows_processed = None


# iri_generated: conteggio IRI dal grafo restituito da materialize (se disponibile)
try:
    iris_generated = mon.count_iris_from_graph(graph, mode="subjects_unique") if isinstance(graph, Graph) else None
except Exception:
    iris_generated = None

# N.B. la tua versione di report non accetta keyword args: usa chiamata posizionale
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
output_file = config_tmp["DataSource2"]['output_file']
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

# rimuovere parentesi angolari
def remove_angular_brackets(uri):
    return uri.strip('<>')

# rimuovere le virgolette
def remove_apices(string):
    return string.strip('"')

# rimuovere apici da uri delimitati da parentesi angolari
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

def pair_subject_object(graph, properties=None):
    """
    Rimuove triple con P1_is_identified_by o P190_has_symbolic_content
    incoerenti, ma conserva:
      - Actor-like:  .../(sub|acr|col)/<val>/<n>  ↔  .../apl/<val>/<n>
      - DigitalDevice: .../dev/<val>/<n>         ↔  .../apl/<val>/<n>
      - Software: .../sfw/<val>/<n>              ↔  .../apl/<val>/<n>
      - Modello:  .../mdl/<nr>/<idx>/<n>         ↔  .../apl/<nr>/<idx>/<n>,
                 literal: "Modello Digitale <idx> oggetto <nr>"
      - Licenze modello: literal tipo "by-nc 4.0"/"cc by ..." anche se l’IRI è .../apl/<nr>/<idx>/<n>
      - Licenze modello con IRI .../apl-lic/<nr>/<idx>/<n>
      - Licenze (lic/<nr>/<idx>/<n>) ↔ apl-lic/<nr>/<idx>/<n>
    """
    def norm_text(s: str) -> str:
        s = str(s)
        s = s.replace("_", " ").replace("-", " ")
        s = re.sub(r"\s+", " ", s)
        return s.strip().lower()

    ACTOR_SUBJ_RE   = re.compile(r".*/(sub|acr|col)/([^/]+)/\d+$")
    DEV_SUBJ_RE     = re.compile(r".*/dev/([^/]+)/\d+$")
    SFW_SUBJ_RE     = re.compile(r".*/sfw/([^/]+)/\d+$")
    APL_VAL_RE      = re.compile(r".*/apl/([^/]+)/\d+$")

    MDL_SUBJ_RE     = re.compile(r".*/mdl/(?P<nr>[^/]+)/(?P<idx>\d{2})/\d+$")
    APL_MDL_RE      = re.compile(r".*/apl/(?P<nr>[^/]+)/(?P<idx>\d{2})/\d+$")
    LIC_SUBJ_RE     = re.compile(r".*/lic/(?P<nr>[^/]+)/(?P<idx>\d{2})/\d+$")
    APL_LIC_MDL_RE  = re.compile(r".*/apl-lic/(?P<nr>[^/]+)/(?P<idx>\d{2})/\d+$")
    IDF_MDL_RE = re.compile(r".*/idf/(?P<nr>[^/]+)/mdl/(?P<idx>\d{2})/\d+$")

    MDL_LIT_RE      = re.compile(
        r"^\s*Modello\s+Digitale\s+(?P<idx>\d{2})\s+oggetto\s+(?P<nr>.+?)\s*$",
        flags=re.IGNORECASE
    )

    triples_to_remove = []
    triples_to_add = []

    for s, p, o in graph.triples((None, None, None)):
        pred = str(p)
        subj_str = str(s)

        # ===== P1_is_identified_by =====
        if pred == "http://www.cidoc-crm.org/cidoc-crm/P1_is_identified_by":
            m_obj_apl = APL_VAL_RE.match(str(o)) or APL_LIC_MDL_RE.match(str(o))

            m_sub_actor = ACTOR_SUBJ_RE.match(subj_str)
            if m_sub_actor and m_obj_apl:
                if norm_text(m_sub_actor.group(2)) != norm_text(m_obj_apl.group(1)):
                    triples_to_remove.append((s, p, o))
                continue

            m_sub_dev = DEV_SUBJ_RE.match(subj_str)
            if m_sub_dev and m_obj_apl:
                if norm_text(m_sub_dev.group(1)) != norm_text(m_obj_apl.group(1)):
                    triples_to_remove.append((s, p, o))
                continue

            m_sub_sfw = SFW_SUBJ_RE.match(subj_str)
            if m_sub_sfw and m_obj_apl:
                if norm_text(m_sub_sfw.group(1)) != norm_text(m_obj_apl.group(1)):
                    triples_to_remove.append((s, p, o))
                continue

            m_sub_mdl = MDL_SUBJ_RE.match(subj_str)
            m_obj_apl_mdl = APL_MDL_RE.match(str(o)) or APL_LIC_MDL_RE.match(str(o))
            if m_sub_mdl and m_obj_apl_mdl:
                if (m_sub_mdl.group("nr") != m_obj_apl_mdl.group("nr") or
                        m_sub_mdl.group("idx") != m_obj_apl_mdl.group("idx")):
                    triples_to_remove.append((s, p, o))
                continue

            # BLOCCO x IDF dell'ultimo
            m_obj_idf_mdl = IDF_MDL_RE.match(str(o))
            if m_sub_mdl and m_obj_idf_mdl:
                if (m_sub_mdl.group("nr") != m_obj_idf_mdl.group("nr") or
                        m_sub_mdl.group("idx") != m_obj_idf_mdl.group("idx")):
                    triples_to_remove.append((s, p, o))
                continue

            # Licenze (lic  apl-lic)
            m_sub_lic = LIC_SUBJ_RE.match(subj_str)
            m_obj_apl_lic = APL_LIC_MDL_RE.match(str(o))
            if m_sub_lic and m_obj_apl_lic:
                if (m_sub_lic.group("nr") != m_obj_apl_lic.group("nr") or
                        m_sub_lic.group("idx") != m_obj_apl_lic.group("idx")):
                    triples_to_remove.append((s, p, o))
                continue

            # default: rimuovi
            triples_to_remove.append((s, p, o))
            continue

        # ===== P190_has_symbolic_content =====
        if pred == "http://www.cidoc-crm.org/cidoc-crm/P190_has_symbolic_content":
            # NORMALIZZAZIONE: no <> e verifica che sia Literal
            if isinstance(o, Literal):
                raw = str(o)
                cleaned = raw.strip().strip('<>')
                if cleaned != raw:
                    new_o = Literal(cleaned, datatype=o.datatype or XSD.string, lang=o.language)
                    triples_to_remove.append((s, p, o))
                    triples_to_add.append((s, p, new_o))
                    o = new_o
            else:
                o_str = str(o).strip()
                if re.match(r'^\s*https?://', o_str, flags=re.IGNORECASE):
                    cleaned = o_str.strip('<>')
                    new_o = Literal(cleaned, datatype=XSD.string)
                    triples_to_remove.append((s, p, o))
                    triples_to_add.append((s, p, new_o))
                    o = new_o
                else:
                    triples_to_remove.append((s, p, o))
                    continue

            # su literal pulito
            lit_raw = str(o)
            lit_clean = lit_raw.strip().strip('<>')
            lit_norm = norm_text(lit_clean)

            # Modelli con apl/<nr>/<idx>/<n>
            m_apl_mdl = APL_MDL_RE.match(subj_str)
            if m_apl_mdl:
                if (lit_norm.startswith("by nc")
                        or lit_norm.startswith("by nc sa")
                        or lit_norm.startswith("cc by")):
                    continue
                m_lit = MDL_LIT_RE.match(lit_clean)
                if not m_lit:
                    triples_to_remove.append((s, p, o))
                else:
                    if (m_lit.group("nr").strip() != m_apl_mdl.group("nr")
                            or f"{int(m_lit.group('idx')):02d}" != m_apl_mdl.group("idx")):
                        triples_to_remove.append((s, p, o))
                continue

            # Modelli con apl-lic/<nr>/<idx>/<n>
            m_apl_lic_mdl = APL_LIC_MDL_RE.match(subj_str)
            if m_apl_lic_mdl:
                if (lit_norm.startswith("by nc")
                        or lit_norm.startswith("by nc sa")
                        or lit_norm.startswith("cc by")):
                    continue
                triples_to_remove.append((s, p, o))
                continue

            # IDF del modello: accetta link http/https (ATON, ecc.)
            m_idf_mdl = IDF_MDL_RE.match(subj_str)
            if m_idf_mdl:
                if not re.match(r'^\s*https?://', lit_clean, flags=re.IGNORECASE):
                    triples_to_remove.append((s, p, o))
                continue

            # Appellation generiche apl/<val>/<n>
            m_apl_val = APL_VAL_RE.match(subj_str)
            if m_apl_val:
                iri_val_norm = norm_text(m_apl_val.group(1))
                if (lit_norm.startswith("by nc")
                        or lit_norm.startswith("by nc sa")
                        or lit_norm.startswith("cc by")):
                    continue
                if lit_norm != iri_val_norm:
                    triples_to_remove.append((s, p, o))
                continue

            # default
            triples_to_remove.append((s, p, o))
            continue

    # applicazione delle modifiche fuori dal loop
    for t in triples_to_remove:
        graph.remove(t)
    for t in triples_to_add:
        graph.add(t)

    return graph

def determine_first_phase(csv_path):
    """
    Per ogni riga del CSV in cui il campo "OGGETTO_ESISTENTE" NON è vuoto,
    identifica la prima fase svolta. Le fasi sono definite in ordine:

      - ACQUISIZIONE: "00"
      - PROCESSAMENTO: "01"
      - MODELLAZIONE: "02"
      - OTTIMIZZAZIONE: "03"
      - ESPORTAZIONE: "04"
      - METADATAZIONE: "05"
      - CARICAMENTO: "06"

    Per ogni fase si considerano tutte le colonne il cui nome inizia con il nome
    della fase (ignorando il case). Se tutte le colonne di una fase sono vuote,
    quella fase non è stata svolta. La prima fase in cui almeno un valore è presente
    viene considerata come la prima fase svolta.

    Restituisce un dizionario in cui:
      - le chiavi sono i valori della colonna "NR" (come stringhe)
      - i valori sono il codice numerico (stringa) della prima fase svolta.

    Esempio di output: {"26": "01", "27a": "02", ...}
    """
    # Leggi il CSV (modifica l'encoding se necessario)
    df = pd.read_csv(csv_path, encoding="latin1")

    # Filtra solo le righe in cui "OGGETTO_ESISTENTE" non è nullo e non è vuoto
    df = df[df["OGGETTO_ESISTENTE"].notnull()]
    df = df[df["OGGETTO_ESISTENTE"].astype(str).str.strip() != ""]

    # Definizione delle fasi (nome, codice)
    phases = [
        ("ACQUISIZIONE", "00"),
        ("PROCESSAMENTO", "01"),
        ("MODELLAZIONE", "02"),
        ("OTTIMIZZAZIONE", "03"),
        ("ESPORTAZIONE", "04"),
        ("METADATAZIONE", "05"),
        ("CARICAMENTO", "06")
    ]

    first_phase_dict = {}

    # Itera sulle righe filtrate
    for _, row in df.iterrows():
        nr = str(row["NR"]).strip()
        # Controlla le fasi in ordine
        for phase_name, phase_code in phases:
            # Trova tutte le colonne che iniziano per il nome della fase (case-insensitive)
            phase_columns = [col for col in df.columns if col.upper().startswith(phase_name.upper())]
            if not phase_columns:
                continue  # Se non ci sono colonne per quella fase, passa alla successiva
            # Estrai i valori relativi a queste colonne per la riga corrente, rimuovendo spazi
            values = row[phase_columns].apply(lambda x: "" if pd.isna(x) else str(x).strip())
            # Se almeno un valore non è vuoto, considera questa fase come la prima svolta
            if any(values != ""):
                first_phase_dict[nr] = phase_code
                break  # Passa alla riga successiva una volta trovata la prima fase svolta

    return first_phase_dict


def process_rdf_data(data, extract_prefixes, supl_dict, csv_input):
    """
    Processa i dati RDF in forma testuale, sostituendo URI malformati e gestendo i prefissi mancanti.

    Inoltre:
      - Prima di ulteriori post-elaborazioni, rimuove dal grafo tutte le triple che hanno
        come soggetto o oggetto un IRI relativo a un'entità (NR) la cui parte fase (in formato a due cifre)
        è inferiore al valore della prima fase svolta (determinata dal CSV).
      - Sostituisce tutte le occorrenze di IRIs con placeholder "0x" (es. <.../mdl/{id}/0x/{version}>)
        con l'IRI reale avente il valore numerico (a due cifre) più basso per quello {id} e {version}.
      - Per le triple con predicato crm:P130_shows_features_of, converte l'oggetto (se URI) in minuscolo.

    Parameters:
      - data (str): I dati RDF in formato testuale.
      - extract_prefixes (function): Funzione per estrarre i prefissi e verificare se mancano.
      - supl_dict (dict): Dizionario contenente le righe di prefissi da aggiungere se mancanti.
      - csv_input (str): Il percorso del CSV di input, per determinare la prima fase svolta.

    Returns:
      - str: I dati RDF processati.
    """
    # --- Sostituzioni testuali di base ---
    prefixes, missing = extract_prefixes(data, supl_dict)
    regex_pattern = generate_prefix_regex(prefixes)
    string_pattern = re.compile(regex_pattern)
    uri_pattern = re.compile(r'<(?!http|https)([^>]+)>')
    bad_form_uri_pattern = re.compile(r'(?:["]<?)(https?:\/\/[^\s"\'<>\\]]+)(?:>["]|[\">?])')
    bad_form_uri_pattern_url_extra = re.compile(r'"(<(https?:\/\/[^>]+)>)"')

    def replace_uri(match):
        return remove_angular_brackets(match.group(0))

    def replace_badform_uri(match):
        return remove_angular_apices_add_angular_brackets(match.group(0))

    def replace_str(match):
        return remove_apices(match.group(0))

    processed_data = uri_pattern.sub(replace_uri, data)
    processed_data = string_pattern.sub(replace_str, processed_data)
    processed_data = bad_form_uri_pattern.sub(replace_badform_uri, processed_data)
    processed_data = bad_form_uri_pattern_url_extra.sub(replace_str, processed_data)

    if missing:
        lines = processed_data.splitlines()
        lines = [line for line in lines if "@prefix" not in line]
        supl_lines = [value for key, value in supl_dict.items()]
        processed_data = "\n".join(supl_lines + lines)

    def enclose_bare_urls(text):
        # lavora solo fuori dalle stringhe: splitta per virgolette e opera sulle sole sezioni pari (fuori literal)
        parts = re.split(r'(".*?")', text, flags=re.DOTALL)
        for i in range(0, len(parts), 2):  # solo fuori dalle virgolette
            parts[i] = re.sub(r'(?<!<)(https?://[^\s<>"]+)(?!>)', r'<\1>', parts[i], flags=re.IGNORECASE)
        return ''.join(parts)

    processed_data = enclose_bare_urls(processed_data)

    # --- Carica il testo RDF processato in un grafo rdflib ---
    g = Graph()
    g.parse(data=processed_data, format="turtle")

    # --- STEP PRELIMINARE: Rimuovi dal grafo le triple relative a entità con fase inferiore ---
    # Determina il dizionario delle prime fasi svolte dal CSV
    first_phase_dict = determine_first_phase(csv_input)
    # print("First phase dictionary:", first_phase_dict)

    # Pattern per riconoscere IRIs con struttura: https://w3id.org/changes/4/aldrovandi/(itm|lic|mdl)/{NR}/{phase}/{version}
    pattern_phase = re.compile(r"^https://w3id.org/changes/4/aldrovandi/(?:itm|lic|mdl)/([^/]+)/(\d{2})/([^/]+)$")

    for triple in list(g.triples((None, None, None))):
        s, p, o = triple
        remove_triple = False
        # Verifica il soggetto
        if isinstance(s, URIRef):
            m = pattern_phase.match(str(s))
            if m:
                nr = m.group(1)
                phase_str = m.group(2)
                if nr in first_phase_dict:
                    if int(phase_str) < int(first_phase_dict[nr]):
                        remove_triple = True
        # Verifica l'oggetto se non già deciso
        if not remove_triple and isinstance(o, URIRef):
            m = pattern_phase.match(str(o))
            if m:
                nr = m.group(1)
                phase_str = m.group(2)
                if nr in first_phase_dict:
                    if int(phase_str) < int(first_phase_dict[nr]):
                        remove_triple = True
        if remove_triple:
            g.remove(triple)
            # print(f"Removed triple: {s} {p} {o}")

    # --- Sostituzione dei placeholder "0x" nei soggetti ---
    pattern_placeholder = re.compile(r"^https://w3id.org/changes/4/aldrovandi/mdl/([^/]+)/0x/([^/]+)$")
    for s in list(g.subjects()):
        s_str = str(s)
        m = pattern_placeholder.match(s_str)
        if m:
            id_part = m.group(1)
            version = m.group(2)
            numeric_pattern = re.compile(
                rf"^https://w3id.org/changes/4/aldrovandi/mdl/{re.escape(id_part)}/(\d{{2}})/{re.escape(version)}$"
            )
            numeric_values = []
            for term in g.all_nodes():
                if isinstance(term, URIRef):
                    term_str = str(term)
                    m_num = numeric_pattern.match(term_str)
                    if m_num:
                        try:
                            num_val = int(m_num.group(1))
                            numeric_values.append(num_val)
                        except ValueError:
                            continue
            if numeric_values:
                min_val = min(numeric_values)
                new_s_str = f"https://w3id.org/changes/4/aldrovandi/mdl/{id_part}/{min_val:02d}/{version}"
                new_s = URIRef(new_s_str)
                for p, o in list(g.predicate_objects(subject=s)):
                    g.remove((s, p, o))
                    g.add((new_s, p, o))
                # print(f"Replaced subject {s_str} with {new_s_str}")

    # --- Sostituzione dei placeholder "0x" negli oggetti ---
    for o in list(g.objects()):
        o_str = str(o)
        m = pattern_placeholder.match(o_str)
        if m:
            id_part = m.group(1)
            version = m.group(2)
            numeric_pattern = re.compile(
                rf"^https://w3id.org/changes/4/aldrovandi/mdl/{re.escape(id_part)}/(\d{{2}})/{re.escape(version)}$"
            )
            numeric_values = []
            for term in g.all_nodes():
                if isinstance(term, URIRef):
                    term_str = str(term)
                    m_num = numeric_pattern.match(term_str)
                    if m_num:
                        try:
                            num_val = int(m_num.group(1))
                            numeric_values.append(num_val)
                        except ValueError:
                            continue
            if numeric_values:
                min_val = min(numeric_values)
                new_o_str = f"https://w3id.org/changes/4/aldrovandi/mdl/{id_part}/{min_val:02d}/{version}"
                new_o = URIRef(new_o_str)
                for s, p in list(g.subject_predicates(object=o)):
                    g.remove((s, p, o))
                    g.add((s, p, new_o))
                # print(f"Replaced object {o_str} with {new_o_str}")

    # --- Lowercase per oggetti di crm:P130_shows_features_of ---
    crm = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
    for s, p, o in list(g.triples((None, crm.P130_shows_features_of, None))):
        if isinstance(o, URIRef):
            lower_o_str = str(o).lower()
            if lower_o_str != str(o):
                new_o = URIRef(lower_o_str)
                g.remove((s, p, o))
                g.add((s, p, new_o))
                # print(f"Lowercased object for predicate crm:P130_shows_features_of: {str(o)} -> {lower_o_str}")

    # --- Pulizia appellations come nel dataset oggetto ---
    properties_in_triples_to_clean = [
        "http://www.cidoc-crm.org/cidoc-crm/P1_is_identified_by",
        "http://www.cidoc-crm.org/cidoc-crm/P190_has_symbolic_content"
    ]

    g = pair_subject_object(g, properties_in_triples_to_clean)

    processed_data = g.serialize(format="turtle")
    return processed_data


processed_data = process_rdf_data(data, extract_prefixes, prefixes_dict_from_yaml, ready_input)

with open(output_file_path, 'w') as f:
    f.write(processed_data)

if os.path.exists(config_path_tmp):
    os.remove(config_path_tmp)

if temp_mapping_file != config["DataSource2"]["mappings"] and os.path.exists(temp_mapping_file):
    os.remove(temp_mapping_file)