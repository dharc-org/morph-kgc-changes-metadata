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


# # input file and column name
# csv_file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
# column_name = 'Consistenza'
#
# # use the function
# try:
#     unique_values = unique_values_from_csv(csv_file_path, column_name)
#     print("Valori unici nella colonna '{}':".format(column_name))
#     for value in unique_values:
#         print(value)
# except Exception as e:
#     print(f"Errore: {e}")
#
#
# def check_relation_and_nr_collegato(file_path):
#     # Leggi il file CSV in un dataframe
#     df = pd.read_csv(file_path)
#
#     # Verifica che le colonne 'Relazione' e 'NR collegato' esistano
#     if 'Relazione' not in df.columns or 'NR collegato' not in df.columns:
#         raise ValueError(f"Una delle colonne richieste ('Relazione', 'NR collegato') non esiste nel file CSV.")
#
#     # Filtra le righe dove 'Relazione' non è vuoto
#     filtered_df = df[df['Relazione'].notna()]
#
#     # Controlla che in queste righe 'NR collegato' non sia vuoto
#     missing_nr_collegato = filtered_df[filtered_df['NR collegato'].isna()]
#
#     # Se ci sono righe dove 'Relazione' ha valore ma 'NR collegato' è vuoto
#     if not missing_nr_collegato.empty:
#         print(f"Ci sono {len(missing_nr_collegato)} righe dove 'Relazione' ha un valore ma 'NR collegato' è vuoto.")
#         print(missing_nr_collegato[['Relazione', 'NR collegato']])
#     else:
#         print("Tutti i valori di 'Relazione' hanno 'NR collegato' non vuoto.")
#
#
#
# def extract_languages_from_titles(file_path, column_name):
#     languages = set()
#
#     with open(file_path, 'r', encoding='utf-8') as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             title = row[column_name].strip()
#             match = re.search(r'@(\w+)$', title)  # looking for pattern "@xx" at the end of the string
#             if match:
#                 language = match.group(1)
#                 languages.add(language)
#
#     return languages
#
# file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
# column_name = 'Titolo originale'
# languages_set = extract_languages_from_titles(file_path, column_name)
# print("Linguaggi trovati:", languages_set)
#
#
# def extract_docu_types(file_path, column_name):
#     docu_types = set()
#
#     with open(file_path, 'r', encoding='utf-8') as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             title = row[column_name].strip()
#             docu_types.add(title)
#
#     return docu_types
#
# file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
# column_name = 'Tipologia documentaria'
# column_name2 = 'Tipologia opera parente'
#
# td_set2 = extract_docu_types(file_path, column_name2)
# td_set1 = extract_docu_types(file_path, column_name)
# print("docu_types_column_name2 trovati:", td_set2)
# print("docu_types_column_name1 trovati:", td_set1)
#
# print("in Tipologia opera parente but not in Tipologia documentaria", [x for x in td_set2 if x not in td_set1])
# print("in Tipologia documentaria but not in Tipologia opera parente", [x for x in td_set2 if x not in td_set1])
#
# # Percorso al file CSV
# csv_file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
#
# # Esegui la funzione
# check_relation_and_nr_collegato(csv_file_path)

file_unico_merged = "src/morph_kgc_changes_metadata_conversions/output_dir/demo_marzo/ready_csv_to_convert/cleaned_aldrovandi_pro(Worksheet).csv"

print("VALORI TROVATI IN ACQUISIZIONE_Istituto_responsabile_acquisizione ", unique_values_from_csv(file_unico_merged, "ACQUISIZIONE_Istituto_responsabile_acquisizione"))
print("VALORI TROVATI IN PROCESSAMENTO_Istituto_responsabile_processamento ", unique_values_from_csv(file_unico_merged, "PROCESSAMENTO_Istituto_responsabile_processamento"))
print("VALORI TROVATI IN MODELLAZIONE_Istituto_responsabile_modellazione ", unique_values_from_csv(file_unico_merged, "MODELLAZIONE_Istituto_responsabile_modellazione"))
print("VALORI TROVATI IN OTTIMIZZAZIONE_Istituto_responsabile_ottimizzazione ", unique_values_from_csv(file_unico_merged, "OTTIMIZZAZIONE_Istituto_responsabile_ottimizzazione"))
print("VALORI TROVATI IN ESPORTAZIONE_Istituto_responsabile_esportazione ", unique_values_from_csv(file_unico_merged, "ESPORTAZIONE_Istituto_responsabile_esportazione"))
print("VALORI TROVATI IN METADATAZIONE_Istituto_responsabile_metadatazione ", unique_values_from_csv(file_unico_merged, "METADATAZIONE_Istituto_responsabile_metadatazione"))
print("VALORI TROVATI IN CARICAMENTO_SU_ATON_Istituto_responsabile_caricamento ", unique_values_from_csv(file_unico_merged, "CARICAMENTO_SU_ATON_Istituto_responsabile_caricamento"))

print("VALORI TROVATI IN ACQUISIZIONE_Persone_responsabili_acquisizione", unique_values_from_csv(file_unico_merged, "ACQUISIZIONE_Persone_responsabili_acquisizione"))
print("VALORI TROVATI IN PROCESSAMENTO_Persone_responsabili_processamento", unique_values_from_csv(file_unico_merged, "PROCESSAMENTO_Persone_responsabili_processamento"))
print("VALORI TROVATI IN MODELLAZIONE_Persone_responsabili_modellazione", unique_values_from_csv(file_unico_merged, "MODELLAZIONE_Persone_responsabili_modellazione"))
print("VALORI TROVATI IN OTTIMIZZAZIONE_Persone_responsabili_ottimizzazione ", unique_values_from_csv(file_unico_merged, "OTTIMIZZAZIONE_Persone_responsabili_ottimizzazione"))
print("VALORI TROVATI IN ESPORTAZIONE_Persone_responsabili_esportazione ", unique_values_from_csv(file_unico_merged, "ESPORTAZIONE_Persone_responsabili_esportazione"))
print("VALORI TROVATI IN METADATAZIONE_Persone_responsabili_metadatazione ", unique_values_from_csv(file_unico_merged, "METADATAZIONE_Persone_responsabili_metadatazione"))
print("VALORI TROVATI IN CARICAMENTO_SU_ATON_Persone_responsabili_caricamento", unique_values_from_csv(file_unico_merged, "CARICAMENTO_SU_ATON_Persone_responsabili_caricamento"))

print("VALORI TROVATI IN ACQUISIZIONE_Strumentazione_di_acquisizione", unique_values_from_csv(file_unico_merged, "ACQUISIZIONE_Strumentazione_di_acquisizione"))
print("VALORI TROVATI IN PROCESSAMENTO_Strumentazione_di_processamento", unique_values_from_csv(file_unico_merged, "PROCESSAMENTO_Strumentazione_di_processamento"))
print("VALORI TROVATI IN MODELLAZIONE_Strumentazione_di_modellazione", unique_values_from_csv(file_unico_merged, "MODELLAZIONE_Strumentazione_di_modellazione"))
print("VALORI TROVATI IN OTTIMIZZAZIONE_Strumentazione_di_ottimizzazione", unique_values_from_csv(file_unico_merged, "OTTIMIZZAZIONE_Strumentazione_di_ottimizzazione"))
print("VALORI TROVATI IN ESPORTAZIONE_Strumentazione_di_esportazione", unique_values_from_csv(file_unico_merged, "ESPORTAZIONE_Strumentazione_di_esportazione"))
print("VALORI TROVATI IN METADATAZIONE_Strumentazione_di_metadatazione", unique_values_from_csv(file_unico_merged, "METADATAZIONE_Strumentazione_di_metadatazione"))
print("VALORI TROVATI IN CARICAMENTO_SU_ATON_Strumentazione_di_caricamento", unique_values_from_csv(file_unico_merged, "CARICAMENTO_SU_ATON_Strumentazione_di_caricamento"))

print("VALORI TROVATI IN ACQUISIZIONE_Tecnica_di_acquisizione", unique_values_from_csv(file_unico_merged, "ACQUISIZIONE_Tecnica_di_acquisizione"))


contributors_columns = ["ACQUISIZIONE_Persone_responsabili_acquisizione", "PROCESSAMENTO_Persone_responsabili_processamento", "MODELLAZIONE_Persone_responsabili_modellazione", "OTTIMIZZAZIONE_Persone_responsabili_ottimizzazione","ESPORTAZIONE_Persone_responsabili_esportazione","METADATAZIONE_Persone_responsabili_metadatazione", "CARICAMENTO_SU_ATON_Persone_responsabili_caricamento"]

