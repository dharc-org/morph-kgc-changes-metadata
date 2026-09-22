# shacl_validator

Modulo interno di validazione SHACL per i grafi prodotti da questa pipeline
(`run_unified_pipeline.py`). Importato e adattato da
[`morph_kgchad_shacl_validator`](https://github.com/ariannamorettj/morph_kgchad_shacl_validator)
(versione piu' aggiornata/pubblicata rispetto al parallelo
`chad_kg_shacl_validator`, che ne e' uno snapshot precedente dello stesso
codice, `isolated_chad_kg_validation`).

## Cosa fa

1. **Estrazione SHACL**: genera shape SHACL da un'ontologia/application
   profile (di default CHAD-AP) documentata con annotazioni `dc:description`
   strutturate.
2. **Validazione**: applica le shape a un grafo RDF con `pySHACL`.
3. **Analisi del report**: deduplica le migliaia di violazioni ripetute a
   livello di istanza in un numero ristretto di "famiglie di problemi"
   indipendenti, per renderle effettivamente leggibili.

## Differenze rispetto al repo di origine

- `config.DEFAULT_PUBLIC_GRAPH_URL` punta di default al grafo locale
  `results/merged_graph_output_aldrovandi.ttl` (prodotto da `run_unified_pipeline.py`)
  invece che a una copia remota su GitHub raw — e' un modulo *interno*, non
  ha senso ripescare da remoto un file che la pipeline ha appena scritto in
  locale. Resta comunque possibile passare esplicitamente un URL http(s) o un
  path diverso (es. `results/merged_graph_output_capellini.ttl`) via
  `--data-source`.
- Il caricamento del file di alias (`resources/chad_ap_aliases.json`) usa un
  path relativo a `__file__` invece di `importlib.resources` con un nome di
  pacchetto punteggiato fisso, perche' in questo repo i moduli non vengono
  sempre importati con lo stesso percorso qualificato.
- `extractor.py` include una piccola tabella di prefissi noti
  (`WELL_KNOWN_PREFIXES`, per ora solo `edtf`) usata come ultima risorsa
  quando un prefisso usato in una `dc:description` di CHAD-AP non e'
  dichiarato ne' nel testo ne' altrove nel grafo dell'ontologia — senza
  questa aggiunta l'estrazione delle shape falliva su CHAD-AP nella sua
  versione corrente.
- Non importato: `fuseki_queries/` (tool separato per testare query SPARQL
  contro un endpoint Fuseki locale, non parte del percorso di validazione
  SHACL) — recuperabile dal repo di origine se serve in futuro.

## Uso rapido

Dalla root del progetto:

```bash
poetry run python3 -c "
import sys; sys.path.insert(0, 'src')
from morph_kgc_changes_metadata_conversions.shacl_validator.cli import main
sys.argv = ['chadkg', 'validate',
            '--data-source', 'results/merged_graph_output_aldrovandi.ttl',
            '--summary-json', 'results/quality/shacl_summary.json',
            '--text-report', 'results/quality/shacl_report.txt']
main()
"
```

oppure per Capellini, passando `--data-source results/merged_graph_output_capellini.ttl`.

Il comando esce con codice 0 se il grafo e' conforme, 1 se ci sono violazioni
(non e' un errore del modulo: `conforms: false` e' un risultato valido).

## Dipendenza

Aggiunge `pyshacl` (>=0.30.1,<0.31.0, richiede Python >=3.9) alle dipendenze
del progetto in `pyproject.toml`. **Nota**: `poetry lock` fallisce al momento
per un bug preesistente indipendente da questa modifica (Poetry/`pkginfo`
installati via Homebrew non riconoscono `Metadata-Version: 2.4` usato da un
pacchetto recente nella catena di dipendenze) — `pyshacl` e le sue dipendenze
sono comunque installate nel virtualenv del progetto; `poetry.lock` va
rigenerato quando l'ambiente Poetry globale viene aggiornato.
