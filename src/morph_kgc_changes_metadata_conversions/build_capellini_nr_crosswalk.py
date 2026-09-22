import csv
import json
import re
import os


OBJ_SOURCE = "src/morph_kgc_changes_metadata_conversions/dataset/dataset_oggetto_capellini/dataset_oggetto_capellini.csv"
ACQ_CLEANED = "input/ready_to_convert/cleaned_capellini_pro.csv"
ACQ_CLEANED_REMAPPED = "input/ready_to_convert/cleaned_capellini_pro.csv"
CROSSWALK_OUT = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed/nr_crosswalk_capellini.json"
UNMATCHED_OUT = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed/nr_unmatched_capellini.json"

# Nel dataset oggetto Capellini "di riferimento" (nuovo), NR e' un ID del
# sistema Changes (es. 699690...); la colonna "nome Changes" conserva pero'
# la vecchia identificazione (es. "13 Rudista"), lo stesso schema usato come
# NR nel dataset di acquisizione/processo (es. NR=13, OGGETTO="Rudista").
# Il token iniziale di "nome Changes" e' quindi la chiave di join verso il
# vecchio NR dell'acquisizione: costruiamo qui quel crosswalk vecchio->nuovo
# e lo applichiamo al file di acquisizione gia' pulito, cosi' che entrambi i
# dataset condividano lo stesso NR (e quindi la stessa IRI item) in fase di
# materializzazione RML.


def build_crosswalk():
    with open(OBJ_SOURCE, encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        header = [h.replace('\n', ' ').strip() for h in next(reader)]
        rows = [r for r in reader if any(c.strip() for c in r)]

    idx = {h: i for i, h in enumerate(header)}
    nr_i, nome_changes_i = idx['NR'], idx['nome Changes']

    crosswalk = {}
    for r in rows:
        new_nr = r[nr_i].strip()
        nome_changes = r[nome_changes_i].strip()
        m = re.match(r'^(\S+)\s+(.*)$', nome_changes)
        if not m:
            continue
        old_nr = m.group(1)
        crosswalk[old_nr] = new_nr

    return crosswalk


def apply_crosswalk(crosswalk):
    with open(ACQ_CLEANED, encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    matched, unmatched = 0, []
    for row in rows:
        old_nr = row['NR'].strip()
        if old_nr in crosswalk:
            row['NR'] = crosswalk[old_nr]
            matched += 1
        elif old_nr:
            unmatched.append(old_nr)

    with open(ACQ_CLEANED_REMAPPED, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return matched, unmatched, len(rows)


def main():
    os.makedirs(os.path.dirname(CROSSWALK_OUT), exist_ok=True)

    crosswalk = build_crosswalk()
    with open(CROSSWALK_OUT, 'w', encoding='utf-8') as f:
        json.dump(crosswalk, f, ensure_ascii=False, indent=2)

    matched, unmatched, total = apply_crosswalk(crosswalk)
    with open(UNMATCHED_OUT, 'w', encoding='utf-8') as f:
        json.dump(sorted(set(unmatched)), f, ensure_ascii=False, indent=2)

    print(f"Crosswalk: {len(crosswalk)} coppie vecchio_NR -> nuovo_NR (salvato in {CROSSWALK_OUT})")
    print(f"Acquisizione: {matched}/{total} righe rimappate al nuovo NR")
    print(f"Acquisizione: {len(set(unmatched))} NR senza corrispondenza nel dataset oggetto (nessun record metadati ancora disponibile), salvati in {UNMATCHED_OUT}")
    print(f"File aggiornato: {ACQ_CLEANED_REMAPPED}")


if __name__ == '__main__':
    main()
