import pandas as pd
import os
import re
import json
from pprint import pprint

# Funzione di pulizia per i valori NR
def clean_value(value):
    if pd.isna(value):
        return None
    value = str(value).strip()              # Rimuove spazi iniziali e finali
    value = re.sub(r'\s+', ' ', value)        # Sostituisce spazi multipli con uno solo
    value = re.sub(r'[\s\-_]+', '_', value)   # Sostituisce spazi, "-" e "_" con "_"
    value = value.lower()
    return value

# Costanti per costruire gli IRI
iri_str_pt1 = "<https://w3id.org/changes/4/aldrovandi/itm/"
iri_str_pt2 = "/ob00/1>"

# Percorsi dei dataset (APRILE)
path_obj = 'src/morph_kgc_changes_metadata_conversions/output_dir/demo_april/input/aldrovandi_obj(Worksheet).csv'
path_pro = 'src/morph_kgc_changes_metadata_conversions/output_dir/demo_april/input/aldrovandi_pro(Worksheet) (1).csv'

# Cartella dei CSV delle stanze (ATON dataset)
stanza_dir = '/Users/ariannamorettj/Desktop/dati_marcello'

# Cartella di output per il JSON finale
output_dir = 'src/morph_kgc_changes_metadata_conversions/output_dir/demo_april/output'

# --- PARTE 1: Confronto tra dataset OBJ e PRO ---

# Carica i dataset OBJ e PRO
df_obj = pd.read_csv(path_obj, encoding='latin1')
df_pro = pd.read_csv(path_pro, encoding='latin1')

# Estrai la colonna "NR" (come stringhe, non pulite)
nr_obj = df_obj['NR'].dropna().astype(str).tolist()
nr_pro = df_pro['NR'].dropna().astype(str).tolist()

# Trova gli ID presenti in OBJ ma non in PRO e viceversa
missing_in_pro = set(nr_obj) - set(nr_pro)
missing_in_obj = set(nr_pro) - set(nr_obj)

print("Valori 'NR' in OBJ non presenti in PRO:")
pprint(missing_in_pro)
print("\nValori 'NR' in PRO non presenti in OBJ:")
pprint(missing_in_obj)

# Lista completa degli ID presenti in PRO (raw)
nr_pro_ids = set(nr_pro)
print("\nLista completa degli ID (NR) presenti in PRO:")
pprint(nr_pro_ids)

# --- PARTE 2: Raccolta dati dai CSV delle stanze (ATON dataset) ---
# Insieme per raccogliere i NR (puliti) dai file delle stanze
all_cleaned_from_stanze = set()

# Dizionario per i CSV delle stanze:
# chiave = numero stanza (estratto dal nome del file)
# valore = dizionario in cui:
#         - le chiavi sono i valori "NR" raw (così come appaiono nel CSV)
#         - i valori sono gli IRI, costruiti usando la versione pulita del NR
dizionario_stanze = {}

for filename in os.listdir(stanza_dir):
    if filename.endswith('.csv'):
        # Esempio: ALDROVANDI_tracking-rototraslazione(STANZA 1).csv
        match = re.search(r'\(STANZA\s*(\d+)\)', filename.upper())
        if match:
            stanza_num = match.group(1)
            filepath = os.path.join(stanza_dir, filename)
            df_stanza = pd.read_csv(filepath, encoding='latin1')
            stanza_dict = {}
            for _, row in df_stanza.iterrows():
                raw_nr = row.get("NR")
                if raw_nr is not None:
                    raw_nr = str(raw_nr).strip()
                    cleaned_nr = clean_value(raw_nr)
                    if cleaned_nr is not None:
                        all_cleaned_from_stanze.add(cleaned_nr)
                        iri = iri_str_pt1 + cleaned_nr + iri_str_pt2
                        # Usa il valore raw come chiave
                        stanza_dict[raw_nr] = iri
            dizionario_stanze[stanza_num] = stanza_dict

# --- PARTE 3: Confronto tra NR in PRO e dati dalle stanze ---
# Calcola l'insieme degli ID PRO in versione pulita
cleaned_pro_ids = {clean_value(nr) for nr in nr_pro_ids if clean_value(nr) is not None}

# Extra: NR presenti nei CSV delle stanze (puliti) ma non in PRO
extra_in_rooms = all_cleaned_from_stanze - cleaned_pro_ids
print("\nValori (puliti) di NR presenti nei CSV delle stanze NON presenti in PRO:")
pprint(extra_in_rooms)

# Extra: NR presenti in PRO ma non trovati in nessun CSV delle stanze
missing_in_rooms = cleaned_pro_ids - all_cleaned_from_stanze
print("\nValori (puliti) di NR in PRO NON trovati nei CSV delle stanze:")
pprint(missing_in_rooms)

# --- PARTE 4: Riorganizzazione degli extra provenienti dal CSV delle stanze ---
# Questi extra rappresentano gli ID che sono presenti nei CSV (ATON dataset) ma non in PROCESS.
# Li salviamo in extra_not_in_PROCESS_dataset.
extra_not_in_PROCESS_dataset = {}

for room, room_dict in dizionario_stanze.items():
    keys_to_remove = []
    for raw_nr, iri in room_dict.items():
        cleaned = clean_value(raw_nr)
        if cleaned not in cleaned_pro_ids:
            keys_to_remove.append(raw_nr)
            if room not in extra_not_in_PROCESS_dataset:
                extra_not_in_PROCESS_dataset[room] = {}
            extra_not_in_PROCESS_dataset[room][raw_nr] = {
                "iri": iri,
                "room": room
            }
    for key in keys_to_remove:
        del room_dict[key]

# --- PARTE 5: Extra per gli ID in PROCESS non trovati nei CSV delle stanze ---
# Questi extra rappresentano gli ID (puliti) che sono presenti nel PROCESS dataset
# ma non sono stati trovati nei CSV (ATON dataset).
extra_not_in_ATON_dataset = {}
for nr in missing_in_rooms:
    extra_not_in_ATON_dataset[nr] = {
        "iri": iri_str_pt1 + nr + iri_str_pt2,
        "room": "not_found"
    }

# --- PARTE 6: Costruzione del dizionario finale e salvataggio in JSON ---
# Il dizionario finale conterrà:
#   - le stanze (con i loro NR, chiave raw, filtrati per quelli presenti anche in PROCESS)
#   - una sezione "extra" con due chiavi:
#         "extra_not_in_ATON_dataset": extra provenienti dal PROCESS dataset (ID in PRO non trovati nei CSV)
#         "extra_not_in_PROCESS_dataset": extra provenienti dai CSV delle stanze (ID presenti nei CSV ma non in PROCESS)
dizionario_finale = dict(dizionario_stanze)  # copia dei dizionari per ciascuna stanza
dizionario_finale["extra"] = {
    "extra_not_in_ATON_dataset": extra_not_in_ATON_dataset,
    "extra_not_in_PROCESS_dataset": extra_not_in_PROCESS_dataset
}

os.makedirs(output_dir, exist_ok=True)
json_output_path = os.path.join(output_dir, 'output_stanze.json')

with open(json_output_path, 'w', encoding='utf-8') as f:
    json.dump(dizionario_finale, f, indent=4, ensure_ascii=False)

print(f"\n✅ Dizionario finale salvato in: {json_output_path}")