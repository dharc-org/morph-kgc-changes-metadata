import csv
import re


SOURCE_CSV = "src/morph_kgc_changes_metadata_conversions/dataset/dataset_oggetto_capellini/dataset_oggetto_capellini.csv"
RAW_COPY_OUT = "input/capellini_obj.csv"
CLEANED_OUT = "input/ready_to_convert/cleaned_capellini_obj.csv"

# Stesso schema/ordine di input/ready_to_convert/cleaned_aldrovandi_obj.csv,
# cosi' il mapping YARRRML esistente (sample_mapping_file.yaml) funziona invariato.
TARGET_COLUMNS = [
    'NR', 'NR collegato', 'Relazione', 'Sala mostra', 'Didascalia',
    'Riproduzione digitale', 'Consistenza', 'Tipologia documentaria', 'Tecnica',
    'Tipologia riprod. in mostra', 'Soggetti', 'Titolo originale', 'Titolo museale',
    'Titolo @en', 'Data', 'Scopritore', 'Autore', 'Traduttore', 'Disegnatore',
    'Incisore', 'Editore', 'Luogo editore', 'Preparatore museale', 'Committente',
    'Tipologia opera parente', 'Titolo opera parente', 'Volume', 'Collezione',
    'Ente conservatore', 'Luogo conservazione', 'Collocazione', 'Fonte',
    'Immagine digitale', 'Iconografia',
]


def normalize_header(h):
    return re.sub(r'\s+', ' ', h.replace('\n', ' ')).strip()


def main():
    with open(SOURCE_CSV, encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        raw_header = next(reader)
        rows = [row for row in reader if any(c.strip() for c in row)]

    # copia grezza rinominata, stesso contenuto/header del file sorgente
    with open(RAW_COPY_OUT, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(raw_header)
        writer.writerows(rows)

    normalized_header = [normalize_header(h) for h in raw_header]
    col_index = {h: i for i, h in enumerate(normalized_header) if h}

    missing = [c for c in TARGET_COLUMNS if c != 'Riproduzione digitale' and c not in col_index]
    if missing:
        raise ValueError(f"Colonne attese ma non trovate nel sorgente Capellini: {missing}")

    with open(CLEANED_OUT, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(TARGET_COLUMNS)
        for row in rows:
            out_row = []
            for col in TARGET_COLUMNS:
                if col == 'Riproduzione digitale':
                    # non presente come colonna a se' nel sorgente Capellini:
                    # duplica "Immagine digitale", come fatto per Aldrovandi
                    idx = col_index.get('Immagine digitale')
                else:
                    idx = col_index[col]
                out_row.append(row[idx] if idx is not None and idx < len(row) else '')
            writer.writerow(out_row)

    print(f"Righe scritte: {len(rows)}")
    print(f"Copia grezza: {RAW_COPY_OUT}")
    print(f"File pulito per morph-kgc: {CLEANED_OUT}")


if __name__ == '__main__':
    main()
