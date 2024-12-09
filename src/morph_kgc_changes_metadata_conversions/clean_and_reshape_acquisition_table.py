import pandas as pd
import numpy as np
from pprint import PrettyPrinter
from collections import OrderedDict
import os
import csv
import re
from datetime import datetime, date

def clean_entity(e):
    if isinstance(e, dict):
        return {k: clean_entity(v) for k, v in e.items()}
    elif isinstance(e, datetime):
        return e.isoformat()
    else:
        return clean_value(e)

def clean_value(value):
    if isinstance(value, str):
        value = value.strip()
        value = re.sub(r'[^\x00-\x7F]+', '', value)
        value = value.replace('"', "'")
    return value

def contains_number(input_string):
    return bool(re.search(r'\d', input_string))


def remove_newlines(text):
    return text.replace('\n', '')


def remove_whitespace(text):
    return text.replace(' ', '_')


def read_and_clean_csv(filepath):
    pp = PrettyPrinter(indent=4)

    # Leggi il CSV senza considerare le prime righe come intestazioni
    df = pd.read_csv(filepath, encoding='ISO-8859-1', header=None)

    # Sostituire i valori nulli (NaN) con una stringa vuota
    df.fillna('', inplace=True)

    # Rimuovere i caratteri di nuova linea (\n) da tutte le celle del DataFrame
    df = df.applymap(remove_newlines)

    titles_lists = []
    for i in range(3):  # Itera sulle prime 3 righe
        titles_lists.append(df.iloc[i].tolist())

    st_level_titles = [remove_newlines(x) for x in titles_lists[0]]
    nd_level_titles = [remove_newlines(x) for x in titles_lists[1]]
    rd_level_titles = [remove_newlines(x) for x in titles_lists[2]]
    hierarchy_titles = OrderedDict()

    first_level_values_in_order = []

    for p,v in enumerate(st_level_titles):
        if v:
            first_level_values_in_order.append(v)
            if st_level_titles[p+1]:
                # significa che è una colonna senza sotto valori
                hierarchy_titles[v] = ""
            else:
                hierarchy_titles[v] =[]
                expected_child_values = 1
                moving_p = p+1
                while moving_p<len(st_level_titles) and not st_level_titles[moving_p]:
                    expected_child_values +=1
                    moving_p +=1
                last_tree_position = p + expected_child_values -1

                for e, ch in enumerate(nd_level_titles[p:last_tree_position]):
                    if ch:
                        if nd_level_titles[p+e+1]:
                            hierarchy_titles[v].append(ch)
                        else:
                            moving_e = p+e+1
                            expected_child_values_nd = 1
                            while moving_e < len(st_level_titles) and not nd_level_titles[moving_e]:
                                expected_child_values_nd += 1
                                moving_e += 1
                            last_tree_position_nd = p+e + expected_child_values_nd - 1
                            init_tree_position_nd = p+e
                            ch_dict = {}
                            #rimozione dei valori vuoti in caso lo spreadsheet ecceda nel numero di colonne vuote
                            clean_list = [x for x in rd_level_titles[init_tree_position_nd:last_tree_position_nd+1] if x]
                            ch_dict[ch] = clean_list
                            hierarchy_titles[v].append(ch_dict)

    pp.pprint(hierarchy_titles)

    # eliminare eventuali colonne in eccesso dal conteggio
    numero_titoli_senza_sottotitolo = len([x for x,v in hierarchy_titles.items() if not v])
    numero_sottotitoli_row_nd =  0
    numero_sottotitoli_row_rd =  0
    for k,v in  hierarchy_titles.items():
        if type(v) is list:
            for el in v:
                if el and type(el) is str:
                    numero_sottotitoli_row_nd +=1
                elif el and type(el) is dict:
                    for c, w in el.items():
                        for z in w:
                            if z:
                                numero_sottotitoli_row_rd += 1
    numero_totale_colonne = numero_titoli_senza_sottotitolo + numero_sottotitoli_row_nd + numero_sottotitoli_row_rd



    return hierarchy_titles, first_level_values_in_order, numero_totale_colonne


def reorganize_table_cells(structure_tree, ordered_first_level_titles, number_of_columns):
    new_list_of_cells = []
    for title in ordered_first_level_titles:
        list_of_sub_titles = structure_tree[title]

        # se il titolo non ha sottotitoli:
        if not list_of_sub_titles:
            new_list_of_cells.append(title)
        else:
            # se ha sottotitoli
            prefix = title
            for subt in list_of_sub_titles:
                if type(subt) is str:
                    # se il sottotitolo non ha ulteriori diramazioni
                    col_name = prefix + "_" + subt
                    new_list_of_cells.append(col_name)
                elif type(subt) is dict:
                    #se il sottotitolo ha ulteriori sottotitoli
                    for k, v in subt.items():
                        # c'è solo una coppia chiave valore dove la chiave è il nome della cella di secondo livello e il valore è una lista con i valori di terzo livello
                        for x in v:
                            col_name = prefix + "_" + k + "_" + x
                            new_list_of_cells.append(col_name)

    print("lunghezza totale delle colonne nuova tabella", len(new_list_of_cells))
    print("\n".join(new_list_of_cells))

    if len(new_list_of_cells) == number_of_columns:
        print("parità numero di celle", len(new_list_of_cells), number_of_columns)

        return new_list_of_cells
    else:
        print("discrepanza nel numero di celle", len(new_list_of_cells), number_of_columns)


def process_and_save_csv_files(input_folder, new_headers, output_folder):
    new_headers = [remove_whitespace(x) for x in new_headers]
    # Assicurati che la cartella di output esista
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Itera su tutti i file nella cartella specificata
    for filename in os.listdir(input_folder):
        # Verifica se il file è un CSV
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            output_file_path = os.path.join(output_folder, filename)

            # Leggi il CSV senza intestazioni
            df = pd.read_csv(file_path, encoding='ISO-8859-1', header=None)

            # Sostituire i valori nulli (NaN) con una stringa vuota
            df.fillna('', inplace=True)

            # Elimina le colonne completamente vuote (sia NaN che stringhe vuote)
            df = df.loc[:, (df != '').any(axis=0)]  # Mantieni solo le colonne non vuote


            # Verifica che la lista degli header sia lunga quanto il numero di colonne
            if len(new_headers) == len(df.columns):
                # Rimuovi le prime 3 righe
                df = df.iloc[3:].reset_index(drop=True)

                # Sostituire la prima riga con la lista degli header
                df.columns = new_headers

                # Salva il CSV modificato nel folder di output
                df.to_csv(output_file_path, index=False, encoding='ISO-8859-1')

                print(f'File {filename} è stato processato e salvato in {output_folder}')
            else:
                print(f"Errore: il numero di intestazioni ({len(new_headers)}) non corrisponde al numero di colonne nel file {filename} ({len(df.columns)} colonne).")
                continue  # Salta questo file se c'è un errore


def convert_to_date(input_date):
    input_date = [x for x in input_date if x]
    if len(input_date) == 0:
        return ""

    """
    Converte una data fornita come lista o stringa in una stringa data standardizzata (YYYY-MM-DD).

    Formati supportati:
        - Lista: ['d', 'm', 'yy']
        - Stringa: 'd/m/yy', 'd.m.yy', 'd-m-yy'

    Args:
        input_date (list o str): La data da convertire.

    Returns:
        str: La data nel formato 'YYYY-MM-DD'.

    Raises:
        ValueError: Se il formato della data non è riconosciuto o è invalido.
    """
    if type(input_date) is datetime:
        input_date = input_date.strftime("%d-%m-%Y")
    elif type(input_date) is list:
        if len(input_date)<3:
            # Ottieni l'anno corrente
            anno_corrente = datetime.now().year

            # Calcola l'anno precedente
            anno_precedente = anno_corrente - 1

            # Converti l'anno precedente in formato a due cifre (YY)
            anno_precedente_yy = f"{anno_precedente % 100:02}"
            input_date.append(anno_precedente_yy)


    # Definisci i possibili separatori
    separatori = ['/', '.', '-']

    # Funzione per gestire la lista
    if isinstance(input_date, list):
        if len(input_date) != 3:
            if not input_date:
                return ""

            #inserire meccanismo di pulizia dei caratteri alfabetici, tipo  + la giornata del
            print("La lista deve contenere esattamente tre elementi: [giorno, mese, anno]. invece contiene", input_date)
            return ""

        giorno, mese, anno = input_date
        if len(giorno) >2:
            anno, mese, giorno = input_date
        # Converti in interi
        try:
            giorno = int(giorno)
            mese = int(mese)
            anno = int(anno)
            if mese>12:
                mese, giorno, anno = input_date
                giorno = int(giorno)
                mese = int(mese)
                anno = int(anno)



        except ValueError:
            raise ValueError("Gli elementi della lista devono essere numerici.")

        # Gestisci anni a due cifre
        if anno < 100:
            anno += 2000  # Ad esempio, '23' diventa '2023'

        try:
            data_obj = date(anno, mese, giorno)
            return data_obj.strftime("%Y-%m-%d")
        except ValueError as ve:
            print(f" {input_date} Data non valida: {ve}")
            return ""
            #raise ValueError(f" {input_date} Data non valida: {ve}")

    # Se è una stringa
    elif isinstance(input_date, str):
        # Identifica quale separatore viene usato
        pattern = '|'.join(map(re.escape, separatori))
        separatore_trovato = re.search(pattern, input_date)

        if separatore_trovato:
            sep = separatore_trovato.group()
        else:
            raise ValueError("Separatore non riconosciuto. Usa '/', '.', o '-'.")

        # Suddividi la stringa in parti
        parti = input_date.split(sep)

        if len(parti) != 3:
            raise ValueError("Formato data non valido. Usa 'd/m/yy', 'd.m.yy', o 'd-m-yy'.")

        giorno, mese, anno = parti
        parti_correct = []
        for x in parti:
            if x.startswith("0"):
                parti_correct.append(x[1:])
            else:
                parti_correct.append(x)


        # Converti in interi
        try:
            giorno = int(parti_correct[0])
            mese = int(parti_correct[1])
            anno = int(parti_correct[2])
        except ValueError:
            raise ValueError("I componenti della data devono essere numerici.")

        # Gestisci anni a due cifre
        if anno < 100:
            anno += 2000  # Ad esempio, '23' diventa '2023'

        try:
            data_obj = date(anno, mese, giorno)
            return data_obj.strftime("%Y-%m-%d")
        except ValueError as ve:
            raise ValueError(f"{input_date} Data non valida: {ve}")

    else:
        raise TypeError("Il tipo di input deve essere una lista o una stringa.", type(input_date), input_date)


# Funzione per il parsing delle date, inclusi i casi speciali
def parse_date(date_str):
    date_str = str(date_str).strip().lower()
    mon = ""
    months = {
        1: ["jan", "january", "gen", "gennaio"],
        2: ["feb", "february", "feb", "febbraio"],
        3: ["mar", "march", "mar", "marzo"],
        4: ["apr", "april", "apr", "aprile"],
        5: ["may", "maggio", "mag", "maggio"],
        6: ["jun", "june", "giu", "giugno"],
        7: ["jul", "july", "lug", "luglio"],
        8: ["aug", "august", "ago", "agosto"],
        9: ["sep", "september", "set", "settembre"],
        10: ["oct", "october", "ott", "ottobre"],
        11: ["nov", "november", "nov", "novembre"],
        12: ["dec", "december", "dic", "dicembre"]
    }
    for k, v in months.items():
        for m in v:
            if m in date_str:
                mon = k
                break

    date_list = []
    date_strip = date_str.strip(" ")
    if "-" in date_strip:
        date_list = date_strip.split("-")
    elif "." in date_strip:
        date_list = date_strip.split(".")
    elif "/" in date_strip:
        date_list = date_strip.split("/")
    elif "\\" in date_strip:
        date_list = date_strip.split("\\")
    else:
        date_list.append(date_strip)

    date_list_num = []
    if mon:
        if len(date_list) == 1:
            date_list_num.append("1")
            date_list_num.append(mon)
        elif len(date_list) == 2:
            date_list_num.append(date_list[0])
            date_list_num.append(mon)
        elif len(date_list) == 3:
            date_list_num.append(date_list[0])
            date_list_num.append(mon)
            date_list_num.append(date_list[2])
        else:
            raise ValueError("Check why there are more than 3 date parts in:", date_list)
    else:
        date_list_num = [x for x in date_list]

    date_converted = convert_to_date(date_list_num)

    return date_converted


def get_min_max_dates(date_list):
    # Definisce il formato delle date nel caso siano stringhe
    date_format = "%Y-%m-%d"  # Formato: anno-mese-giorno (puoi modificarlo se necessario)

    # Converte le stringhe in oggetti datetime
    dates = [datetime.strptime(date, date_format) for date in date_list]

    # Trova la data minima e massima
    min_date = min(dates)
    max_date = max(dates)

    # Restituisce le date nel formato richiesto
    return min_date.strftime(date_format), max_date.strftime(date_format)


def modify_csv(input_file, output_dir, columns_to_remove, new_column_data):
    input_filename = input_file.split("/")[-1]
    output_file = os.path.join(output_dir, input_filename)
    # Leggi il file CSV originale
    with open(input_file, mode='r', newline='', encoding='ISO-8859-1') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames

        # Rimuovi le colonne da eliminare
        fieldnames = [field for field in fieldnames if field not in columns_to_remove]

        # Aggiungi le nuove colonne
        fieldnames.extend(['ACQUISIZIONE_Tempi_di_acquisizione_Data_fine_(specificare_data_mm-dd)',
                           'ACQUISIZIONE_Tempi_di_acquisizione_Data_inizio_(specificare_data_mm-dd)'])

        # Scrivi il nuovo file CSV
        with open(output_file, mode='w', newline='', encoding='ISO-8859-1') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            # Itera su ogni riga del CSV originale
            for index, row in enumerate(reader):
                # Aggiungi i valori delle nuove colonne per ogni riga
                new_data = new_column_data.get(index, {})

                # Aggiungi i valori delle nuove colonne alla riga
                row.update(new_data)

                # Rimuovi le colonne specificate
                for col in columns_to_remove:
                    if col in row:
                        del row[col]

                # Scrivi la riga modificata nel nuovo file CSV
                writer.writerow(row)


def dates_post_process(output_directory):
    # for each csv file in the directory, read the headers
    # check if the headers of the list below are in the file
    # if not, return
    headers_to_change = ['ACQUISIZIONE_Tempi_di_acquisizione_Aprile_23_o_prima_(specificare_data_mm-dd)',
     'ACQUISIZIONE_Tempi_di_acquisizione_17.04.23',
     'ACQUISIZIONE_Tempi_di_acquisizione_24.04.23',
     'ACQUISIZIONE_Tempi_di_acquisizione_08.05.23',
     'ACQUISIZIONE_Tempi_di_acquisizione_15.05.23',
     'ACQUISIZIONE_Tempi_di_acquisizione_22.05.23',
     'ACQUISIZIONE_Tempi_di_acquisizione_29.05.23',
     'ACQUISIZIONE_Tempi_di_acquisizione_Giugno_23_o_dopo_(specificare_data_mm-dd)']

    #dizionario di mapping stringhe - date
    mapping_string_dates ={
        'ACQUISIZIONE_Tempi_di_acquisizione_17.04.23': '17.04.23',
        'ACQUISIZIONE_Tempi_di_acquisizione_24.04.23': '24.04.23',
        'ACQUISIZIONE_Tempi_di_acquisizione_08.05.23': '08.05.23',
        'ACQUISIZIONE_Tempi_di_acquisizione_15.05.23': '15.05.23',
        'ACQUISIZIONE_Tempi_di_acquisizione_22.05.23': '22.05.23',
        'ACQUISIZIONE_Tempi_di_acquisizione_29.05.23': '29.05.23',
    }

    # Verifica se la directory esiste
    if not os.path.exists(output_directory):
        print(f"Errore: La directory {output_directory} non esiste.")
        return

    # Itera attraverso ogni file nella directory
    for filename in os.listdir(output_directory):
        file_path = os.path.join(output_directory, filename)

        # Controlla se il file è un CSV
        if filename.endswith('.csv'):
            with open(file_path, mode='r', newline='', encoding='ISO-8859-1') as file:
                reader = csv.DictReader(file)

                # Verifica se almeno una delle intestazioni desiderate è presente nel file
                if not any(header in reader.fieldnames for header in headers_to_change):
                    print(f"Intestazioni mancanti nel file: {filename}")
                    return

                rows_values_to_update = {}

                for index, row in enumerate(reader):
                    gathered_values = []
                    for el in headers_to_change:
                        if row.get(el):
                            if len(str(row[el]).strip()) == 1:
                                gathered_values.append(parse_date(mapping_string_dates[el]))

                            else:
                                if contains_number(row.get(el)):
                                    gathered_values.append(parse_date(row.get(el)))

                    rows_values_to_update[index] = gathered_values


                updated_columns = dict()
                for k,v in rows_values_to_update.items():
                    if len(v) == 0:
                        updated_columns[k] = {
                            "ACQUISIZIONE_Tempi_di_acquisizione_Data_inizio_(specificare_data_mm-dd)": "",
                            "ACQUISIZIONE_Tempi_di_acquisizione_Data_fine_(specificare_data_mm-dd)": ""
                        }
                    elif len(v) ==1:
                        updated_columns[k] = {
                            "ACQUISIZIONE_Tempi_di_acquisizione_Data_inizio_(specificare_data_mm-dd)": v[0],
                            "ACQUISIZIONE_Tempi_di_acquisizione_Data_fine_(specificare_data_mm-dd)": v[0]
                        }
                    else:
                        start_date, end_date = get_min_max_dates(v)
                        updated_columns[k] = {
                            "ACQUISIZIONE_Tempi_di_acquisizione_Data_inizio_(specificare_data_mm-dd)": start_date,
                            "ACQUISIZIONE_Tempi_di_acquisizione_Data_fine_(specificare_data_mm-dd)": end_date
                        }
                pp = PrettyPrinter(indent=4)
                #pp.pprint(updated_columns)

                file_dir_out = output_directory + "_restructured"
                if not os.path.exists(file_dir_out):
                    os.makedirs(file_dir_out)

                modify_csv(file_path, file_dir_out, headers_to_change, updated_columns)


def final_postprocess(input_directory):
    # Crea una nuova directory per i file finali
    output_directory = input_directory + "_final"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Itera su tutti i file nella cartella di input
    for filename in os.listdir(input_directory):
        file_path = os.path.join(input_directory, filename)

        # Se il file è un CSV, processalo
        if filename.endswith('.csv'):
            # Estrai il numero della stanza dal nome del file (assumendo che "STANZA X" sia nel nome del file)
            match = re.search(r"STANZA (\d+)", filename)
            if match:
                room_number = match.group(1)
            else:
                room_number = "UNKNOWN"  # Se non trovi la stanza, metti un valore di default

            with open(file_path, mode='r', newline='', encoding='ISO-8859-1') as infile:
                reader = csv.DictReader(infile)
                fieldnames = reader.fieldnames

                # Crea il file di output nella nuova directory
                output_file_path = os.path.join(output_directory, filename)
                with open(output_file_path, mode='w', newline='', encoding='ISO-8859-1') as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()

                    # Introduci una variabile per il numero progressivo
                    progressivo = 1

                    # Itera su ogni riga del CSV
                    for row in reader:
                        # Rimuovi gli a capo da tutte le celle e pulisci il contenuto
                        for key in row:
                            row[key] = clean_entity(row[key])

                        # Correggi le colonne che contengono 'specificare_data'
                        for key in row:
                            if "specificare_data" in key:
                                row[key] = parse_date(row[key])

                        # Controlla la colonna NR, se è vuota, riempila con la stringa "EXT_<numero_stanza>_<numero_progressivo>"
                        if not row.get('NR'):  # Se la colonna NR è vuota
                            row['NR'] = f"EXT_{room_number}_{progressivo}"

                        # Incrementa il numero progressivo per la prossima riga
                        progressivo += 1

                        # Scrivi la riga nel nuovo file
                        writer.writerow(row)

    print(f"File CSV corretti salvati nella cartella: {output_directory}")


if __name__ == '__main__':

    # Percorso del file CSV (modifica con il tuo percorso)
    csv_file_path = 'src/morph_kgc_changes_metadata_conversions/dataset/dataset_acquisizione_aldrovandi/oggetti_mostra_chi_quando(STANZA 1).csv'

    # Chiama la funzione per leggere e stampare le prime tre righe come liste
    gerarchia, lista_colonne_primo_livello, numero_colonne =  read_and_clean_csv(csv_file_path)
    new_cells_names = reorganize_table_cells(gerarchia, lista_colonne_primo_livello, numero_colonne)

    # Percorso della cartella contenente i file CSV
    input_folder = 'src/morph_kgc_changes_metadata_conversions/dataset/dataset_acquisizione_aldrovandi'
    # Percorso della cartella di destinazione per i nuovi file
    output_folder = 'src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_aldrovandi_clean'
    # La nuova lista di intestazioni da usare

    # Chiama la funzione per processare i file
    process_and_save_csv_files(input_folder, new_cells_names, output_folder)


    # SECONDO ROUND PER LE ALTRE TABELLE (PERCHé DALLA STANZA 2 C'è UNA DATA DI ACQUISIZIONE IN PIù)
    # Percorso del file CSV (modifica con il tuo percorso)
    csv_file_path = 'src/morph_kgc_changes_metadata_conversions/dataset/dataset_acquisizione_aldrovandi/oggetti_mostra_chi_quando(STANZA 2).csv'

    # Chiama la funzione per leggere e stampare le prime tre righe come liste
    gerarchia, lista_colonne_primo_livello, numero_colonne =  read_and_clean_csv(csv_file_path)
    new_cells_names = reorganize_table_cells(gerarchia, lista_colonne_primo_livello, numero_colonne)

    # Percorso della cartella contenente i file CSV
    input_folder = 'src/morph_kgc_changes_metadata_conversions/dataset/dataset_acquisizione_aldrovandi'
    # Percorso della cartella di destinazione per i nuovi file
    output_folder = 'src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_aldrovandi_clean'
    # La nuova lista di intestazioni da usare

    # Chiama la funzione per processare i file
    process_and_save_csv_files(input_folder, new_cells_names, output_folder)

    # NON NECESSARIO PER CAPPELLINI!!!
    dates_post_process("src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_aldrovandi_clean")

    # pulizia finale
    final_postprocess("src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_aldrovandi_clean_restructured")