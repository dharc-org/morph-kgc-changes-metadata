import csv
import pandas as pd
import os
import re
from datetime import datetime
import argparse
import chardet


# Funzione per normalizzare i nomi delle colonne
def normalize_column_names(df):
    """
    Normalizza i nomi delle colonne rimuovendo gli a capo e sostituendo spazi multipli.

    Parametri:
    df (pd.DataFrame): Il DataFrame di pandas da cui normalizzare i nomi delle colonne.

    Ritorna:
    pd.DataFrame: Il DataFrame con i nomi delle colonne normalizzati.
    """
    df = df.copy()
    df.columns = df.columns.str.replace('\n', ' ', regex=False)  # Sostituisce '\n' con spazio
    df.columns = df.columns.str.strip()  # Rimuove spazi iniziali e finali
    df.columns = df.columns.str.replace(r'\s+', ' ', regex=True)  # Sostituisce spazi multipli con uno spazio
    df.columns = df.columns.str.replace(' ', '_', regex=False)  # Sostituisce spazi con underscore
    return df


# Funzione per rilevare la codifica del file
def detect_encoding(file_path):
    """
    Rileva la codifica di un file usando chardet.

    Parametri:
    file_path (str): Il percorso al file da analizzare.

    Ritorna:
    str: La codifica rilevata.
    """
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(100000))  # Legge i primi 100KB
    return result['encoding']


# Funzione per leggere un CSV con la codifica rilevata
def read_csv_with_encoding(file_path):
    """
    Legge un file CSV con la codifica rilevata.

    Parametri:
    file_path (str): Il percorso al file CSV.

    Ritorna:
    pd.DataFrame: Il DataFrame pandas letto dal CSV.
    """
    encoding = detect_encoding(file_path)
    try:
        df = pd.read_csv(file_path, encoding=encoding)
        print(f"File '{file_path}' letto con successo usando la codifica: {encoding}")
    except UnicodeDecodeError:
        print(f"Errore di decodifica con la codifica: {encoding}. Provando con 'latin1'.")
        df = pd.read_csv(file_path, encoding='latin1')
    return df


# Funzione per rimuovere le colonne 'Unnamed'
def drop_unnamed_columns(df):
    """
    Rimuove le colonne che contengono 'Unnamed' nel loro nome.

    Parametri:
    df (pd.DataFrame): Il DataFrame di pandas da cui rimuovere le colonne.

    Ritorna:
    pd.DataFrame: Il DataFrame senza le colonne 'Unnamed'.
    """
    unnamed_cols = [col for col in df.columns if 'Unnamed' in col]
    if unnamed_cols:
        print(f"Dropping Unnamed columns: {unnamed_cols}")
        df = df.drop(columns=unnamed_cols)
    return df


# Funzione per combinare i dataset
def combine_datasets(file1, file2, output_file):
    """
    Combina due dataset CSV in uno unico, assicurando che ogni colonna abbia almeno una cella non vuota.
    Ordina le colonne in base al dataset con più colonne e rimuove le colonne 'Unnamed'.

    Parametri:
    file1 (str): Percorso al primo file CSV.
    file2 (str): Percorso al secondo file CSV.
    output_file (str): Percorso del file CSV di output combinato.

    Ritorna:
    pd.DataFrame: Il DataFrame combinato.
    """
    # Leggi entrambi i file CSV con la codifica corretta
    df1 = read_csv_with_encoding(file1)
    df2 = read_csv_with_encoding(file2)

    # Rimuovi le colonne 'Unnamed' da entrambi i DataFrame
    df1 = drop_unnamed_columns(df1)
    df2 = drop_unnamed_columns(df2)

    # Normalizza i nomi delle colonne
    df1 = normalize_column_names(df1)
    df2 = normalize_column_names(df2)

    # Identifica tutte le colonne uniche da entrambi i DataFrame
    all_columns = set(df1.columns).union(set(df2.columns))

    # Determina quale DataFrame ha più colonne per stabilire l'ordine delle colonne
    if len(df1.columns) >= len(df2.columns):
        column_order = list(df1.columns)
    else:
        column_order = list(df2.columns)

    # Aggiungi eventuali colonne mancanti all'ordine delle colonne
    for col in all_columns:
        if col not in column_order:
            column_order.append(col)

    # Inizializza un set per tenere traccia dei campi riempiti
    filled_fields = set()

    # Inizializza una lista per raccogliere le righe selezionate
    selected_rows = []

    # Combina entrambi i DataFrame per un'elaborazione unificata
    combined_df = pd.concat([df1, df2], ignore_index=True)

    # Itera attraverso ogni riga nel DataFrame combinato
    for index, row in combined_df.iterrows():
        # Determina i nuovi campi che questa riga può coprire
        new_fields = set()
        for col in all_columns:
            if pd.notnull(row.get(col)) and col not in filled_fields:
                new_fields.add(col)

        # Se la riga contribuisce a nuovi campi, selezionala
        if new_fields:
            selected_rows.append(row)
            filled_fields.update(new_fields)
            print(f"Selected row {index}: covers fields {new_fields}")

            # Controlla se tutti i campi sono riempiti
            if len(filled_fields) == len(all_columns):
                print("All columns have been covered.")
                break

    # Dopo la selezione iniziale, verifica se ci sono colonne mancanti
    missing_fields = all_columns - filled_fields
    if missing_fields:
        print(f"Columns missing coverage: {missing_fields}")
        # Tenta di trovare righe che possono coprire i campi mancanti
        for index, row in combined_df.iterrows():
            # Salta le righe già selezionate
            if row in selected_rows:
                continue

            # Identifica se la riga può coprire uno dei campi mancanti
            row_missing_fields = set()
            for col in missing_fields:
                if pd.notnull(row.get(col)):
                    row_missing_fields.add(col)

            if row_missing_fields:
                selected_rows.append(row)
                filled_fields.update(row_missing_fields)
                missing_fields -= row_missing_fields
                print(f"Selected additional row {index}: covers fields {row_missing_fields}")

                # Se tutti i campi mancanti sono coperti, interrompi
                if not missing_fields:
                    break

    # Verifica finale
    if len(filled_fields) != len(all_columns):
        print(f"Warning: The following columns could not be covered: {missing_fields}")

    # Crea un nuovo DataFrame con le righe selezionate
    combined_sample_df = pd.DataFrame(selected_rows)

    # Aggiungi eventuali colonne mancanti con valori NaN
    for col in all_columns:
        if col not in combined_sample_df.columns:
            combined_sample_df[col] = pd.NA

    # Riordina le colonne secondo l'ordine stabilito
    combined_sample_df = combined_sample_df[column_order]

    # Salva il DataFrame combinato in un file CSV
    combined_sample_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Combined dataset saved to {output_file}")

    return combined_sample_df


# Funzione per creare il dataset unico
def create_unique_dataset(file1, file2, output_dir):
    """
    Crea un dataset unico da due file CSV di input, assicurando che ogni colonna abbia almeno una cella non vuota.

    Parametri:
    file1 (str): Percorso al primo file CSV.
    file2 (str): Percorso al secondo file CSV.
    output_dir (str): Directory in cui salvare il file CSV di output combinato.

    Ritorna:
    pd.DataFrame: Il DataFrame combinato.
    """
    # Assicurati che la directory di output esista
    os.makedirs(output_dir, exist_ok=True)

    # Definisci il percorso del file di output
    output_file_path = os.path.join(output_dir, 'unique_combined_dataset.csv')

    # Combina i dataset
    combined_sample_df = combine_datasets(file1, file2, output_file_path)

    return combined_sample_df


# Esempio di utilizzo
if __name__ == "__main__":
    # Configurazione degli argomenti da linea di comando
    parser = argparse.ArgumentParser(
        description="Combine two CSV datasets ensuring all columns have at least one non-empty cell.")
    parser.add_argument('-file1', type=str, required=True, help='Path to the first input CSV file.')
    parser.add_argument('-file2', type=str, required=True, help='Path to the second input CSV file.')
    parser.add_argument('-output_dir', type=str, required=True, help='Directory to save the combined output CSV file.')

    args = parser.parse_args()

    # Crea il dataset unico combinato
    create_unique_dataset(args.file1, args.file2, args.output_dir)