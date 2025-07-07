
# ASK-KG
## A morph-kgc plug-in 
Ask-KG automatically generates YARRRML mapping rules from answers to a Limesurvey questionnaire, exported as JSON file

input: Answers exported in JSON
output: YARRRML mapping file + ini configuration file

Once ASK-KG output is generated, the two files can be used in the Morph-KGC (in particular, the CHANGES extension) conversion process as mapping and configuration files. 

**The four sections of the questionnaire have the following purposes:**
1) To set up a configuration file
2) To make the internal filling conventions of a data structure into mapping rules, by linking the mapping necessities to existing conversion functions
3) To help formalising the model extension necessities #CRITICO
4) To translate them into YARRRML rules

## Set of questions to be converted in config.ini

## 1- `[CONFIGURATION]`

---

### `na_values`

**Question**: Which values should be treated as missing/null in the input data? 
- Default:
```
,,#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,nan,None,""
```
- Possible values:
Any comma-separated list of null representations (e.g., `"NULL"`, `"n/a"`, etc.)
---

### `output_dir`

**Question**: Where should the output RDF files be saved?
- Default:
`output_dir`
- Possible values:
Any output directory path
---

### `output_format`

**Question**: What RDF output format should be used?
- Default: `N-TRIPLES`
- Possible values:

  * `N-TRIPLES`
  * `RDF/XML`
  * `JSON-LD`
  * `TRIG`
  * `N3`

---

### `output_serialization`

**Question**: What RDF serialization format should be used?
- Default: `turtle`
- Possible values:

  * `turtle`
  * `xml`
  * `json-ld`
  * `trig`
  * `n3`

---

### `only_printable_characters`

**Question**: Should only printable characters be allowed in output?
- Default: `no`
- Possible values:

  * `yes`
  * `no`
---

### `safe_percent_encoding`

**Question**: Should safe percent-encoding of IRIs be enabled?
- Default: *(empty)* (disabled)
- Possible values:

  * `true`
  * `false`
  * *(empty = disabled)*
---
### `mapping_partitioning`

**Question**: What kind of mapping partitioning should be used?
- Default: `PARTIAL-AGGREGATIONS`
- Possible values:

  * `NONE`
  * `PARTIAL-AGGREGATIONS`
  * `FULL`
---

### `infer_sql_datatypes`

**Question**: Should SQL datatypes be inferred from the data?
- Default: `no`
- Possible values:

  * `yes`
  * `no`
---
### `logging_level`

**Question**: What logging level should be used during execution?
- Default: `INFO`
- Possible values:
  * `DEBUG`
  * `INFO`
  * `WARNING`
  * `ERROR`
  * `CRITICAL`

---

### `logs_file`

**Question**: Where should logs be written?
- Default: *(empty = output to stdout)*
---

### `oracle_client_lib_dir`

**Question**: Path to the Oracle client libraries (if using Oracle DB)?
- Default: *(empty)*

---

### `oracle_client_config_dir`

**Question**: Path to the Oracle client configuration files (if using Oracle DB)?
- Default: *(empty)*

---

### `project_iri_base`

**Question**: What is the base IRI for the generated URIs?
- Default: `https://w3id.org/changes/4/aldrovandi/`

---

### `versione`

**Question**: What version of the project should be declared?
- Default: `1`
- Possible values:
Any string or integer.

---

##2- `[DataSourceX]` sections (e.g., `[DataSource1]`, `[DataSource2]`...)

---
Repeat the following questions for each data source.

---
### `mappings`

**Question**: Path to the mapping file used for this data source?
- Default (example):
`mapping_file.yaml`
- Possible values:
Any filepath to a valid yaml mapping file.

---

### `mapping_format`

**Question**: What format is the mapping file written in?
- Default: `YARRRML`
- Possible values:

  * `YARRRML`
  * `RML`

---

### `file_path`

**Question**: Path to the JSON/CSV/TSV file to be converted?
- Default (example):
`input.csv`
- Possible values:
Any filepath to a valid json/tsv/csv input file.
---

### `ready_input_dir`

**Question**: Directory where preprocessed input files are stored?
- Default (example):
`input_dir`
- Possible values:
Any dirpath to a directory where the input file(s) is/are stored. 
---

### `output_file`

**Question**: Filename for the generated RDF output?
- Default (example):
`output_dir`
- Possible values:
Any dirpath to a directory where the output rdf file(s) is/are to be stored. 
---

### `delimiter`

**Question**: What character separates fields in the input file?
- Default: `,`
- Possible values:

  * `,` (comma)
  * `;` (semicolon)
  * `\t` (tab)
  * `|` (pipe)

---

### `quotechar`

**Question**: What character is used to quote field values in the CSV file?
- Default: `"`
- Other possible valuses: "'"
---

### `encoding`

**Question**: What encoding is used in the input file?
- Default: `utf-8`
- Possible values:

  * `utf-8`
  * `latin1`
  * `iso-8859-1`
  * `utf-16`



## Set of questions to be converted in mapping.yaml


## Application in CHANGES case studies
- Each case study comes with specific necessities 
- To leave the users a degree of freedom to interveen on the template if necessary 
- To allow field expert withoit semantic web background to formalise domain expertise 

