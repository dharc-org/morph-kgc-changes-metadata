import pandas as pd
import json
import os

# Carica i CSV in DataFrame (sostituisci 'tabella1.csv' e 'tabella2.csv' con i nomi dei tuoi file)
df1 = pd.read_csv('src/morph_kgc_changes_metadata_conversions/dataset/worksheets_ald_finali/aldrovandi_obj(Worksheet).csv', encoding='latin1')
df2 = pd.read_csv('src/morph_kgc_changes_metadata_conversions/dataset/worksheets_ald_finali/aldrovandi_pro(Worksheet).csv', encoding='latin1')

# Estrai la colonna "nr" (rimuovendo eventuali valori mancanti)
nr1 = df1['NR'].dropna().tolist()
nr2 = df2['NR'].dropna().tolist()

# Trova i valori presenti in tabella1 che non sono in tabella2
missing_in_table2 = set(nr1) - set(nr2)
# Trova i valori presenti in tabella2 che non sono in tabella1
missing_in_table1 = set(nr2) - set(nr1)

print("Valori 'nr' in OBJ non presenti in PRO:", missing_in_table2)
print("Valori 'nr' in PRO non presenti in OBJ:", missing_in_table1)


from pprint import pprint

# Leggi la tabella CSV (adatta il percorso e l'encoding se necessario)
df = pd.read_csv('src/morph_kgc_changes_metadata_conversions/dataset/worksheets_ald_finali/aldrovandi_obj(Worksheet).csv', encoding='latin1')

# Lista dei valori da cercare nella colonna "NR"
lista_valori = [x for x in missing_in_table2]

# Filtra le righe in cui il valore di "NR" è presente nella lista
df_filtrato = df[df['NR'].isin(lista_valori)]

# Crea un dizionario: chiave = valore "NR", valore = valore corrispondente in "Didascalia"
dizionario = dict(zip(df_filtrato['NR'], df_filtrato['Didascalia']))

# Stampa il dizionario in maniera formattata
pprint(dizionario)


### GENERAZIONE DEGLI IRI X ATON https://w3id/changes/4/aldrovandi/<<<ID>>>/mdl/07

iri_str_pt1 = "<https://w3id/changes/4/aldrovandi/"
iri_str_pt2 = "/mdl/06>"
df2 = pd.read_csv('src/morph_kgc_changes_metadata_conversions/dataset/worksheets_ald_finali/aldrovandi_pro(Worksheet).csv', encoding='latin1')
output_dir = "src/morph_kgc_changes_metadata_conversions/output_dir/demo_marzo/iri"


# Crea il dizionario per il JSON
dizionario = {}
for idx, row in df2.iterrows():
    nr = str(row["NR"])            # Assicurati che "NR" sia trattato come stringa
    didascalia = row["DIDASCALIA"]   # Assicurati del nome esatto della colonna
    iri = iri_str_pt1 + nr + iri_str_pt2
    dizionario[nr] = {"iri": iri, "DIDASCALIA": didascalia}

# Salva il dizionario in un file JSON
json_filepath = os.path.join(output_dir, "output.json")
with open(json_filepath, "w", encoding="utf-8") as json_file:
    json.dump(dizionario, json_file, indent=4, ensure_ascii=False)

# Crea un DataFrame per il CSV con le colonne IRI, NR, Didascalia
df_csv = pd.DataFrame({
    "IRI": [iri_str_pt1 + str(nr) + iri_str_pt2 for nr in df2["NR"]],
    "NR": df2["NR"],
    "DIDASCALIA": df2["DIDASCALIA"]
})

# Salva il DataFrame in un file CSV
csv_filepath = os.path.join(output_dir, "output.csv")
df_csv.to_csv(csv_filepath, index=False, encoding="utf-8")

print("File generati nella cartella:", output_dir)