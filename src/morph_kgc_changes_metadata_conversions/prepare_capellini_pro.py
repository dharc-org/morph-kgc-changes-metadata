import os
import shutil
import pandas as pd

from clean_and_reshape_acquisition_table import (
    read_and_clean_csv,
    reorganize_table_cells,
    process_and_save_csv_files,
)


SOURCE_CSV = "src/morph_kgc_changes_metadata_conversions/dataset/dataset_acquisizione_capellini/acquisizione_capellini.csv"
TMP_FIXED_DIR = "src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_capellini_fixed_typos"
FLAT_OUT_DIR = "src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_capellini_clean"
FINAL_OUT = "input/ready_to_convert/cleaned_capellini_pro.csv"
MAPPING_FILE = "src/morph_kgc_changes_metadata_conversions/mapping_file_acquisition.yaml"

# Il file sorgente (export UniBO piu' recente) e' UTF-8, a differenza dei vecchi
# file ISO-8859-1 di Aldrovandi su cui sono state scritte le funzioni condivise
# di clean_and_reshape_acquisition_table.py: qui le richiamiamo con encoding='utf-8'
# invece di fare un round-trip lossy in Latin-1 (il file contiene caratteri come
# l'en-dash "-" non rappresentabili in Latin-1).
ENCODING = 'utf-8'

# Refusi presenti nel template di acquisizione Capellini (righe di intestazione 2-3),
# corretti per allinearsi ai nomi colonna attesi da mapping_file_acquisition.yaml.
# NB: "aquisizione"->"acquisizione" e' stato corretto a monte, nella versione
# piu' recente del file esportata da UniBO (metadata-acquisizioni), quindi qui
# resta solo il refuso "sepecificare" (ancora presente in quella versione).
HEADER_TYPO_FIXES = {
    "Tecnica di aquisizione": "Tecnica di acquisizione",
    "Strumentazione di aquisizione": "Strumentazione di acquisizione",
    "Tempi di aquisizione": "Tempi di acquisizione",
    "Data inizio (sepecificare data mm-dd)": "Data inizio (specificare data mm-dd)",
    "Data fine (sepecificare data mm-dd)": "Data fine (specificare data mm-dd)",
}


def fix_header_typos():
    os.makedirs(TMP_FIXED_DIR, exist_ok=True)
    df = pd.read_csv(SOURCE_CSV, encoding='utf-8-sig', header=None, dtype=str)
    df.fillna('', inplace=True)
    for i in range(3):
        df.iloc[i] = df.iloc[i].apply(lambda v: HEADER_TYPO_FIXES.get(v, v))
    fixed_path = os.path.join(TMP_FIXED_DIR, os.path.basename(SOURCE_CSV))
    df.to_csv(fixed_path, index=False, header=False, encoding=ENCODING)
    return fixed_path


def mapping_referenced_columns():
    """Estrae tutti i riferimenti $(colonna) usati dal mapping YARRRML."""
    cols = set()
    with open(MAPPING_FILE, encoding='utf-8') as f:
        for line in f:
            if '$(' in line:
                start = line.index('$(') + 2
                end = line.rindex(')')
                if end > start:
                    cols.add(line[start:end])
    cols.discard('o')
    return cols


def add_missing_mapped_columns(df):
    """Aggiunge come colonne vuote quelle referenziate dal mapping ma assenti nel
    template Capellini (es. i campi 'Licenza' e 'OGGETTO_ESISTENTE', mai raccolti
    per questa collezione): senza di esse morph-kgc fallisce in lettura (usecols),
    anche se il dato resta semplicemente assente/nullo."""
    expected = mapping_referenced_columns()
    missing = sorted(c for c in expected if c not in df.columns)
    for c in missing:
        df[c] = ''
    return df, missing


def main():
    fixed_path = fix_header_typos()

    hierarchy, first_level_titles, n_cols = read_and_clean_csv(fixed_path, encoding=ENCODING)
    new_headers = reorganize_table_cells(hierarchy, first_level_titles, n_cols)
    if new_headers is None:
        raise ValueError("Il numero di celle ricostruite non combacia con le colonne del file sorgente.")

    process_and_save_csv_files(TMP_FIXED_DIR, new_headers, FLAT_OUT_DIR, encoding=ENCODING)

    flat_file = os.path.join(FLAT_OUT_DIR, os.path.basename(SOURCE_CSV))
    df = pd.read_csv(flat_file, encoding=ENCODING, dtype=str)
    df.fillna('', inplace=True)
    df, missing_cols = add_missing_mapped_columns(df)
    df.to_csv(FINAL_OUT, index=False, encoding='utf-8')

    shutil.rmtree(TMP_FIXED_DIR)

    print(f"Righe scritte: {len(df)}")
    if missing_cols:
        print(f"Colonne aggiunte vuote (assenti nel template Capellini): {missing_cols}")
    print(f"File pulito per morph-kgc: {FINAL_OUT}")


if __name__ == '__main__':
    main()
