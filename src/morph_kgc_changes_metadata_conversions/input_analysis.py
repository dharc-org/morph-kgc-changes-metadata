import pandas as pd
import csv
import re


def unique_values_from_csv(file_path, column_name):
    # csv to dataframe
    df = pd.read_csv(file_path)

    # does column name exist in dataframe
    if column_name not in df.columns:
        raise ValueError(f"La colonna '{column_name}' non esiste nel file CSV.")

    # obtain a set of all values of that column
    unique_values = df[column_name].dropna().unique()

    # stor unique values in a list
    unique_values_sorted = sorted(unique_values)

    return unique_values_sorted


# input file and column name
csv_file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
column_name = 'Consistenza'

# use the function
try:
    unique_values = unique_values_from_csv(csv_file_path, column_name)
    print("Valori unici nella colonna '{}':".format(column_name))
    for value in unique_values:
        print(value)
except Exception as e:
    print(f"Errore: {e}")


def check_relation_and_nr_collegato(file_path):
    # Leggi il file CSV in un dataframe
    df = pd.read_csv(file_path)

    # Verifica che le colonne 'Relazione' e 'NR collegato' esistano
    if 'Relazione' not in df.columns or 'NR collegato' not in df.columns:
        raise ValueError(f"Una delle colonne richieste ('Relazione', 'NR collegato') non esiste nel file CSV.")

    # Filtra le righe dove 'Relazione' non è vuoto
    filtered_df = df[df['Relazione'].notna()]

    # Controlla che in queste righe 'NR collegato' non sia vuoto
    missing_nr_collegato = filtered_df[filtered_df['NR collegato'].isna()]

    # Se ci sono righe dove 'Relazione' ha valore ma 'NR collegato' è vuoto
    if not missing_nr_collegato.empty:
        print(f"Ci sono {len(missing_nr_collegato)} righe dove 'Relazione' ha un valore ma 'NR collegato' è vuoto.")
        print(missing_nr_collegato[['Relazione', 'NR collegato']])
    else:
        print("Tutti i valori di 'Relazione' hanno 'NR collegato' non vuoto.")



def extract_languages_from_titles(file_path, column_name):
    languages = set()

    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            title = row[column_name].strip()
            match = re.search(r'@(\w+)$', title)  # looking for pattern "@xx" at the end of the string
            if match:
                language = match.group(1)
                languages.add(language)

    return languages

file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
column_name = 'Titolo originale'
languages_set = extract_languages_from_titles(file_path, column_name)
print("Linguaggi trovati:", languages_set)


def extract_docu_types(file_path, column_name):
    docu_types = set()

    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            title = row[column_name].strip()
            docu_types.add(title)

    return docu_types

file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
column_name = 'Tipologia documentaria'
column_name2 = 'Tipologia opera parente'

td_set2 = extract_docu_types(file_path, column_name2)
td_set1 = extract_docu_types(file_path, column_name)
print("docu_types_column_name2 trovati:", td_set2)
print("docu_types_column_name1 trovati:", td_set1)

print("in Tipologia opera parente but not in Tipologia documentaria", [x for x in td_set2 if x not in td_set1])
print("in Tipologia documentaria but not in Tipologia opera parente", [x for x in td_set2 if x not in td_set1])

# Percorso al file CSV
csv_file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'

# Esegui la funzione
check_relation_and_nr_collegato(csv_file_path)