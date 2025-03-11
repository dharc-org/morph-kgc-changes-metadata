import csv
import json

def confronta_tabelle(file1, file2, outfile_report):
    """
    Confronta due tabelle (liste di dizionari) e restituisce un dizionario con i campi:

    1) id_nella_tabella_x_assenti_in_y:
       Lista di stringhe <NR>_<OGGETTO> presenti in tabella1 (x) ma assenti in tabella2 (y).
    2) id_nella_tabella_y_assenti_in_x:
       Lista di stringhe <NR>_<OGGETTO> presenti in tabella2 (y) ma assenti in tabella1 (x).
    3) missing_data:
       Lista di stringhe <NR>_<OGGETTO> degli elementi trovati in tabella2
       che hanno *solo* i campi (NR, OGGETTO, STANZA) compilati (DIDASCALIA
       può essere compilato o meno, ma non influisce) e tutti gli altri vuoti.
    4) repeated_ids_tab1:
       Lista di stringhe <NR>_<OGGETTO> corrispondenti ai valori di NR duplicati in tabella1.
    5) repeated_ids_tab2:
       Lista di stringhe <NR>_<OGGETTO> corrispondenti ai valori di NR duplicati in tabella2.
    6) missing_ids_tab1:
       Lista di stringhe <NR>_<OGGETTO> di tutte le entità in tabella1
       che hanno il campo NR vuoto oppure con un valore che inizia con 'EXT'.
    7) missing_ids_tab2:
       Lista di stringhe <NR>_<OGGETTO> di tutte le entità in tabella2
       che hanno il campo NR vuoto oppure con un valore che inizia con 'EXT'.
    """

    # =============================================================================
    # 1) id_nella_tabella_x_assenti_in_y
    # 2) id_nella_tabella_y_assenti_in_x
    # =============================================================================

    tabella1 = []
    with open(file1, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tabella1.append(dict(row))

    tabella2 = []
    with open(file2, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tabella2.append(dict(row))


    # 1) Creiamo i set di NR per ciascuna tabella
    set_tab1_NR = {str(row.get("NR", "")).strip() for row in tabella1}
    set_tab2_NR = {str(row.get("NR", "")).strip() for row in tabella2}

    # 2) Determiniamo gli NR mancanti
    nr_mancanti_in_tab2 = set_tab1_NR - set_tab2_NR  # Quelli presenti in tab1 ma assenti in tab2
    nr_mancanti_in_tab1 = set_tab2_NR - set_tab1_NR  # Quelli presenti in tab2 ma assenti in tab1

    # Determiniamo nr comuni
    nr_comuni = [x for x in set_tab1_NR if x in set_tab2_NR]

    # 3) Recuperiamo la lista NR_OGGETTO per NR mancanti
    id_nella_tabella_x_assenti_in_y = []
    for row in tabella1:
        nr = str(row.get("NR", "")).strip()
        oggetto = str(row.get("OGGETTO", "")).strip()
        if nr in nr_mancanti_in_tab2:
            id_nella_tabella_x_assenti_in_y.append(f"{nr} : {oggetto}")

    id_nella_tabella_y_assenti_in_x = []
    for row in tabella2:
        nr = str(row.get("NR", "")).strip()
        oggetto = str(row.get("OGGETTO", "")).strip()
        if nr in nr_mancanti_in_tab1:
            id_nella_tabella_y_assenti_in_x.append(f"{nr} : {oggetto}")

    # Eventualmente ordiniamo le liste
    id_nella_tabella_x_assenti_in_y.sort()
    id_nella_tabella_y_assenti_in_x.sort()

    # =============================================================================
    # 3) missing_data
    #    - in tabella2, vanno considerati solo i record che abbiano NR, OGGETTO, STANZA
    #      compilati (o almeno uno di essi), e DIDASCALIA può essere sia vuoto sia pieno,
    #      ma non è rilevante. Tutti gli altri campi devono essere vuoti.
    # =============================================================================

    campi_obbligatori = {"NR", "OGGETTO", "STANZA"}  # DIDASCALIA non influenza
    campi_opzionali = {"DIDASCALIA"}  # Non consideriamo DIDASCALIA né come obbligatorio né come extra

    missing_data = []

    for row in tabella2:
        nr = str(row.get("NR", "")).strip()
        oggetto = str(row.get("OGGETTO", "")).strip()

        # Campi compilati (non vuoti) effettivamente presenti nella riga
        campi_compilati = {
            chiave
            for chiave, valore in row.items()
            if valore is not None and str(valore).strip() != ""
        }

        if len([x for x in campi_compilati if x in campi_obbligatori]) == 3 and len(campi_compilati) ==3:
            missing_data.append(f"{nr} : {oggetto}")

        elif len(campi_compilati) == 4:
            if len([x for x in campi_compilati if x in campi_opzionali]) == 1 and len([x for x in campi_compilati if x in campi_obbligatori]) == 3:
                missing_data.append(f"{nr} : {oggetto}")

    # =============================================================================
    # 4) repeated_ids_tab1: NR duplicati in tabella1
    # 5) repeated_ids_tab2: NR duplicati in tabella2
    # =============================================================================

    #  FINO A QUI
    from collections import Counter

    # Ripetizioni in tabella1
    counter_tab1 = Counter(str(row.get("NR", "")).strip() for row in tabella1)
    nr_duplicati_tab1 = {nr for nr, count in counter_tab1.items() if count > 1}

    repeated_ids_tab1 = set()
    for row in tabella1:
        nr = str(row.get("NR", "")).strip()
        oggetto = str(row.get("OGGETTO", "")).strip()
        if nr in nr_duplicati_tab1:
            repeated_ids_tab1.add(f"{nr} : {oggetto}")
    repeated_ids_tab1 = sorted(repeated_ids_tab1)

    # Ripetizioni in tabella2
    counter_tab2 = Counter(str(row.get("NR", "")).strip() for row in tabella2)
    nr_duplicati_tab2 = {nr for nr, count in counter_tab2.items() if count > 1}

    repeated_ids_tab2 = set()
    for row in tabella2:
        nr = str(row.get("NR", "")).strip()
        oggetto = str(row.get("OGGETTO", "")).strip()
        if nr in nr_duplicati_tab2:
            repeated_ids_tab2.add(f"{nr} : {oggetto}")
    repeated_ids_tab2 = sorted(repeated_ids_tab2)

    # =============================================================================
    # 6) missing_ids_tab1: NR vuoto o che inizia con "EXT" in tabella1
    # 7) missing_ids_tab2: NR vuoto o che inizia con "EXT" in tabella2
    # =============================================================================
    missing_ids_tab1 = []
    for row in tabella1:
        nr = str(row.get("NR", "")).strip()
        oggetto = str(row.get("OGGETTO", "")).strip()
        if not nr or nr.startswith("EXT"):
            missing_ids_tab1.append(f"{nr}_{oggetto}")

    missing_ids_tab2 = []
    for row in tabella2:
        nr = str(row.get("NR", "")).strip()
        oggetto = str(row.get("OGGETTO", "")).strip()
        if not nr or nr.startswith("EXT"):
            missing_ids_tab2.append(f"{nr}_{oggetto}")

    # Ritorno il dizionario con tutti i campi richiesti
    id_nella_tabella_x_assenti_in_y = [x for x in set(id_nella_tabella_x_assenti_in_y)]
    id_nella_tabella_y_assenti_in_x = [x for x in set(id_nella_tabella_y_assenti_in_x)]
    nr_comuni = [x for x in set(nr_comuni)]
    missing_data = [x for x in set(missing_data)]
    repeated_ids_tab1 = [x for x in set(repeated_ids_tab1)]
    repeated_ids_tab2 = [x for x in set(repeated_ids_tab2)]
    missing_ids_tab1 = [x for x in set(missing_ids_tab1)]
    missing_ids_tab2 = [x for x in set(missing_ids_tab2)]

    analysis_result = {
        "id_nella_tabella_1_assenti_in_2": id_nella_tabella_x_assenti_in_y,
        "id_nella_tabella_2_assenti_in_1": id_nella_tabella_y_assenti_in_x,
        "ID_comuni_alle_tabelle": nr_comuni,
        "rows_con_solo_missing_data_eccetto_nr_oggetto_e_stanza": missing_data,

        "ids_ripetuti_in_tab1": repeated_ids_tab1,
        "ids_ripetuti_in_tab2": repeated_ids_tab2,
        "ids_generati_da_me_perche_mancanti_in_tab1": missing_ids_tab1,
        "ids_generati_da_me_perche_mancanti_in_tab2": missing_ids_tab2
    }

    # Stampa a schermo in formato JSON con indentazione
    print(json.dumps(analysis_result, indent=4, ensure_ascii=False))

    # Salvataggio su file JSON
    with open(outfile_report, "w", encoding="utf-8") as f:
        json.dump(analysis_result, f, indent=4, ensure_ascii=False)

    return analysis_result


t1 = "src/morph_kgc_changes_metadata_conversions/dataset/dataset_oggetto_aldrovandi/dataset_oggetto_aldrovandi.csv"
t2 = "src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_aldrovandi_clean_restructured_final_merged.csv"
outp = "src/morph_kgc_changes_metadata_conversions/output_dir/id_analysis.json"
confronta_tabelle(t1, t2, outp)