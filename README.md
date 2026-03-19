# Morph-KGChad

**Morph-KGChad** (**Morph-KGC CHANGES Metadata**) is an open-source extension of **Morph-KGC** for the materialisation of **CHANGES-aligned cultural heritage metadata** into RDF. It implements a reproducible **CSV-to-RDF pipeline** for datasets structured according to the **CHAD-AP** (**Cultural Heritage Acquisition and Digitisation Application Profile**), enabling the generation of semantically structured knowledge graphs from tabular metadata and digitisation paradata.

The software was developed in the context of the CHANGES project and documented primarily through the CHAD-KG case study on the Aldrovandi Digital Twin. Conceptually, the pipeline is designed to support the conversion of two complementary tabular datasets:

- **cultural heritage object metadata**
- **acquisition and digitisation paradata**

These are converted into a unified RDF graph aligned with CHAD-AP.

A key goal of Morph-KGChad is to provide a **transparent, reusable, and reproducible** working configuration. The repository encapsulates a frozen version of the current software stack, including the versions of Morph-KGC, the adopted mapping rules, and the function library used in the documented experiments.

---

## Software metadata

- **Title:** Morph-KGChad (Morph-KGC CHANGES Metadata)
- **Authors:** Arianna Moretti; Sebastian Barzaghi
- **Repository:** https://github.com/dharc-org/morph-kgc-changes-metadata
- **Secondary dependency:** CHAD-AP (Cultural Heritage Acquisition and Digitisation Application Profile)  
  https://dharc-org.github.io/chad-ap/current/chad-ap.html

### Documented in

- **Primary reference:**  
  Barzaghi, Sebastian, Arianna Moretti, Ivan Heibi, and Silvio Peroni.  
  *CHAD-KG: A Knowledge Graph for Representing Cultural Heritage Objects and Digitisation Paradata.*  
  arXiv preprint, 19 May 2025.  
  https://doi.org/10.48550/arXiv.2505.13276

- **Secondary reference:**  
  Barzaghi, Sebastian, Alice Bordignon, Bianca Gualandi, et al.  
  *A Proposal for a FAIR Management of 3D Data in Cultural Heritage: The Aldrovandi Digital Twin Case.*  
  *Data Intelligence*, ahead of print, 31 December 2024.  
  https://doi.org/10.3724/2096-7004.di.2024.0061

---

## Overview

Morph-KGChad extends Morph-KGC to support the semantic materialisation of cultural heritage data collected in collaborative spreadsheet environments and exported as CSV files. It was designed for cases in which:

- metadata are gathered through tabular templates;
- the target output must comply with a formal semantic model;
- mapping rules need to remain inspectable and reusable;
- input values may require project-specific transformations beyond Morph-KGC built-in functions;
- pre-processing and post-processing steps are needed to handle heterogeneous or imperfect tabular data.

The pipeline combines:

- **YARRRML mapping files**
- **Morph-KGC configuration files**
- **RML-FNML built-in functions**
- **project-specific user-defined functions**
- **a launch/orchestrator script**
- **optional monitoring and quality-checking components**

The resulting output is an RDF graph serialised in a format supported by Morph-KGC and configured through the `.ini` file (for example, **N-TRIPLES** or **Turtle**).

---

## Conceptual model

Morph-KGChad materialises data according to **CHAD-AP**, an OWL application profile for representing:

1. **Cultural Heritage Objects (CHOs)** through an **Object Module**, grounded in:
   - CIDOC-CRM
   - LRMoo

2. **Acquisition and digitisation workflows** through a **Process Module**, grounded mainly in:
   - CRMdig

The model also reuses controlled vocabularies such as **Getty AAT** for the stabilisation of terms related to object types, activities, tools, techniques, and roles.

At a high level, this means the software can be used to express:

- object-level descriptive metadata;
- contextual entities such as actors, places, titles, and subjects;
- acquisition activities;
- software-based processing, modelling, optimisation, and related workflow steps;
- tools, devices, software, agents, and time spans involved in the production of digital surrogates.

---

## Current repository usage

### Preprocessing

(optional)
Run:

```bash
python src/morph_kgc_changes_metadata_conversions/clean_csv.py
````

Notes:

* manually change the input and output paths in the script;
* the input file is the raw CSV;
* the output file is the cleaned/post-processed CSV;
* in the current execution setup, the file may be overwritten.

### Triples production and post-processing

Run:

```bash
python run_unified_pipeline.py
```
This script orchestrate the joint use of 
* main_object_demo.py
* main_process_demo.py 

Notes:

* manually change input, output, mapping, and configuration paths if needed;
* this script executes the conversion workflow;
* it also post-processes the generated RDF and fixes issues related to subject and object datatypes where required.

---

## Current mapping file path

```text
src/morph_kgc_changes_metadata_conversions/sample_mapping_file.yaml
src/morph_kgc_changes_metadata_conversions/mapping_file_acquisition.yaml
```

This repository currently exposes a sample YARRRML mapping file. More broadly, the Morph-KGChad architecture is designed to support separate mapping files for different input datasets or modules, such as object metadata and acquisition/digitisation paradata.

---

## Current sample input structure

The code currently accepts CSV input tables structured like the sample file at:

```text
src/morph_kgc_changes_metadata_conversions/sample_input_3_entries.csv
```

This sample illustrates the expected column semantics for the conversion process.

---

## Configuration file

```text
src/morph_kgc_changes_metadata_conversions/config.ini
```

Morph-KGChad relies on a Morph-KGC `.ini` configuration file to declare conversion parameters.

This includes:

* output file name;
* output directory;
* output RDF serialisation;
* input file path;
* mapping file path;
* CSV parsing parameters such as delimiter, quote character, and encoding.
* in [QUALITY] parameters for quality controls on the genrated graph

A typical configuration looks like this:

```ini
[CONFIGURATION]
na_values = ,,#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,nan,None,""
output_dir = results
monitor_report = results/monitor
quality_report = results/quality
output_format = N-TRIPLES
output_serialization = turtle
only_printable_characters = no
safe_percent_encoding = 
mapping_partitioning = PARTIAL-AGGREGATIONS
infer_sql_datatypes = no
logging_level = INFO
logs_file = 
oracle_client_lib_dir = 
oracle_client_config_dir =
project_iri_base = https://w3id.org/changes/4/aldrovandi/
versione = 1

[DataSource1]
mappings = src/morph_kgc_changes_metadata_conversions/sample_mapping_file.yaml
mapping_format = YARRRML
file_path = input/aldrovandi_obj.csv
ready_input_dir = input/ready_to_convert
output_file = knowledge-graph_obj.ttl
delimiter = ,
quotechar = "
encoding = utf-8

[DataSource2]
mappings = src/morph_kgc_changes_metadata_conversions/mapping_file_acquisition.yaml
mapping_format = YARRRML
file_path = input/aldrovandi_pro.csv
ready_input_dir = input/ready_to_convert
output_file = knowledge-graph_pro.ttl
delimiter = ,
quotechar = "
encoding = utf-8

[QUALITY]
http_timeout = 5
max_links = 200
sample_size = 25
link_namespaces = vocab.getty.edu,viaf.org

# bucket disgiunti: se lo stesso IRI ha tipi in bucket diversi si considera un conflitto
disjoint_buckets =
    person=http://www.cidoc-crm.org/cidoc-crm/E21_Person;
    group=http://www.cidoc-crm.org/cidoc-crm/E74_Group;
    appellation=http://www.cidoc-crm.org/cidoc-crm/E41_Appellation;
    item=http://iflastandards.info/ns/lrm/lrmoo/F5_Item;
    data_object=http://www.ics.forth.gr/isl/CRMdig/D9_Data_Object;
    device=http://www.ics.forth.gr/isl/CRMdig/D8_Digital_Device;
    software=http://www.ics.forth.gr/isl/CRMdig/D14_Software;
    activity=http://www.ics.forth.gr/isl/CRMdig/D2_Digitization_Process|http://www.ics.forth.gr/isl/CRMdig/D10_Software_Execution;
    timespan=http://www.cidoc-crm.org/cidoc-crm/E52_Time-Span

# proprietà attese single-valued
single_valued_props = http://purl.org/dc/terms/identifier,http://www.w3.org/2000/01/rdf-schema#label,http://xmlns.com/foaf/0.1/name

# begin/end
begin_props = http://www.cidoc-crm.org/cidoc-crm/P82a_begin_of_the_begin
end_props   = http://www.cidoc-crm.org/cidoc-crm/P82b_end_of_the_end



```

---

## Main software components

### 1. Mapping files

Morph-KGChad uses **YARRRML** mapping files to define the conversion rules from CSV columns to RDF triples.

These mappings:

* translate tabular fields into RDF according to **CHAD-AP classes and properties**;
* are driven by the structure of the input template rather than by individual content values;
* are therefore reusable across datasets that follow the same template logic.

In the documented CHAD-KG workflow, separate mappings were used for the different tabular inputs.

### 2. Configuration file

The `.ini` configuration file specifies:

* where input data is located;
* which mappings should be used;
* how output should be serialised;
* where results should be written.

This supports reproducible execution across runs and datasets.

### 3. User-defined functions

Morph-KGChad extends Morph-KGC through **RML-FNML-compatible user-defined functions**. These address conversion needs that are not fully covered by the built-in function set.

The implemented transformations fall into four main groups:

#### a. IRI generation

* `conditional_normalize_and_convert_to_iri`
* `normalize_and_convert_to_iri`

#### b. Controlled-value alignment

* `assess_aat_tool_type`
* `convert_to_aat`
* `convert_documentary_type_to_aat`

#### c. Complex value parsing and extraction

* `multiple_separator_split_explode`
* `extract_title`
* `extract_documented_in_iri`

#### d. Date handling

* `date_to_xs_datetime`
* `split_year_range_to_dates`

These functions are particularly useful for normalising project-specific strings, aligning free-text values to controlled vocabularies, managing multi-value cells, and transforming tabular date expressions into RDF-compatible values.

### 4. Launch/orchestrator script

The launch script coordinates the end-to-end workflow.

Beyond triggering the conversion itself, it can also perform:

* input normalisation;
* cleaning and reshaping of tabular data;
* handling of missing or exceeding columns;
* pre-processing before materialisation;
* post-processing after RDF generation.

A key design goal is robustness. The orchestration logic was conceived to reduce blocking failures caused by partial or structurally inconsistent inputs. In the documented workflow, the software was designed so that, if only one of the expected datasets is available, it should still be possible to generate a well-formed RDF graph from that single input.

### 5. Monitoring and performance instrumentation

The extended pipeline includes a monitoring component that records empirical execution data, such as:

* materialisation time per thousand rows;
* peak memory usage.

These results are serialised into a JSON report for regression tracking and performance evaluation.

### 6. Data quality checks

Morph-KGChad also includes a quality-checking layer for structural and semantic sanity checks on the generated RDF graph.

The documented checks include:

* duplicate IRIs and disjoint-type conflicts;
* stale or non-dereferenceable sampled external authority links;
* inconsistent time-span intervals;
* violations of single-valued property expectations.

Results are exported to a JSON report including anomaly counts, sampled problematic cases, and execution parameters.

---

## Input model and workflow logic

The broader Morph-KGChad workflow was originally designed around two tabular inputs exported from collaborative spreadsheet environments:

1. **Exhibited objects metadata**
2. **Digitisation paradata**

Conceptually, the pipeline works as follows:

### Input

* one or more CSV files;
* one or more YARRRML mapping files;
* one Morph-KGC configuration file.

### Process

* Morph-KGC executes the mappings;
* built-in and custom functions transform the data;
* the launch script coordinates preprocessing, conversion, merging, and post-processing.

### Output

* a unified RDF graph aligned with CHAD-AP;
* optionally, performance and quality JSON reports.

Although the current repository snapshot may expose only a sample mapping/input pair for direct execution, the software architecture is intended for this broader multi-dataset workflow.

---

## Notes on interaction and user effort

Interaction with the software is intentionally limited. In ordinary use, the main required interventions are:

* preparing the input CSV file(s);
* adjusting paths in the cleaning script;
* adjusting paths in the main launch script;
* editing the `.ini` configuration file when needed;
* modifying or extending mappings only if the input template changes or new modelling needs arise.

For standard conversions based on already-supported templates, users are not expected to edit the mapping logic extensively.

---

## Extending Morph-KGChad

Morph-KGChad is extensible at several levels.

### Extending mappings

If a new dataset follows the same template logic, the existing mapping structure can usually be reused with only minor adaptations.

If a new dataset introduces additional semantic needs, new mapping rules can be added.

### Extending controlled values

If a value is not currently represented in the expected controlled vocabulary alignment, the relevant transformation logic may need to be updated so that the string used in the table is associated with the intended URI.

### Extending CHAD-AP alignment

Where new informational needs emerge that are not covered by the current modelling layer, additional ontological elements and mapping patterns may be introduced.

This is particularly relevant for data domains that expand beyond the initial Aldrovandi use case.

---

## Current limitations

As documented in the associated research output, Morph-KGChad is designed to be accessible for case studies similar to those on which it was developed and tested. However:

* more advanced customisation still requires technical intervention;
* extending mappings and controlled-term alignments may require Semantic Web knowledge and programming skills;
* some domain extensions may depend on corresponding updates in the target conceptual model.

In other words, the software is reproducible and reusable, but not yet fully low-code for all extension scenarios.

---

## Example execution summary

A minimal execution path in the current repository is:

```bash
python src/morph_kgc_changes_metadata_conversions/clean_csv.py
python main_aldrovandi.py
```

Before running:

* check the input/output paths in the scripts;
* confirm the mapping file path;
* confirm the configuration file values;
* ensure the CSV structure is compatible with the expected template.

---

## Case study context

Morph-KGChad was documented through the CHAD-KG case study, where it was used to materialise a knowledge graph for cultural heritage objects and digitisation paradata related to the Aldrovandi exhibition and its Digital Twin. In that documented release, the resulting graph contained tens of thousands of RDF triples and was used as the semantic source of truth for the project’s published data ecosystem.

The broader methodological contribution of the software lies not only in RDF production, but also in supporting:

* traceable digitisation workflows;
* semantically aligned metadata and paradata;
* FAIR-oriented publication practices;
* reuse across related cultural heritage case studies.

---

## Why Morph-KGChad

Morph-KGChad was developed to address a recurring need in cultural heritage digitisation projects: producing semantically rich, interoperable, and reusable RDF graphs from collaborative tabular data without relying entirely on bespoke hard-coded conversion pipelines.

Its main strengths are:

* explicit alignment with CHAD-AP;
* declarative mappings in YARRRML;
* extensibility through user-defined functions;
* reproducible and versioned working configuration;
* compatibility with heterogeneous cultural heritage metadata scenarios;
* integration of operational checks such as performance reporting and data quality inspection.

---

## Dependencies and conceptual background

Morph-KGChad is based on **Morph-KGC**, which in turn relies on **RML** and supports **RML-FNML** for function-based transformations.

Relevant references:

* Arenas-Guerrero, Julián, David Chaves-Fraga, Jhon Toledo, María S. Pérez, and Oscar Corcho. 2024.
  “Morph-KGC: Scalable knowledge graph materialization with mapping partitions.”
  *Semantic Web* 15 (1): 1–20.
  [https://doi.org/10.3233/SW-223135](https://doi.org/10.3233/SW-223135)

* Arenas-Guerrero, Julián, Paola Espinoza-Arias, José Antonio Bernabé-Diaz, Prashant Deshmukh, José Luis Sánchez-Fernández, and Oscar Corcho. 2024.
  “An RML-FNML module for Python user-defined functions in Morph-KGC.”
  *SoftwareX* 26: 101709.
  [https://doi.org/10.1016/j.softx.2024.101709](https://doi.org/10.1016/j.softx.2024.101709)

---

## BibTeX

```bibtex
@article{arenas2024rmlfnml,
  title = {{An RML-FNML module for Python user-defined functions in Morph-KGC}},
  author = {Julián Arenas-Guerrero and Paola Espinoza-Arias and José Antonio Bernabé-Diaz and Prashant Deshmukh and José Luis Sánchez-Fernández and Oscar Corcho},
  journal = {SoftwareX},
  year = {2024},
  volume = {26},
  pages = {101709},
  issn = {2352-7110},
  publisher = {Elsevier},
  doi = {10.1016/j.softx.2024.101709}
}

@article{arenas2024morph,
  title     = {{Morph-KGC: Scalable knowledge graph materialization with mapping partitions}},
  author    = {Arenas-Guerrero, Julián and Chaves-Fraga, David and Toledo, Jhon and Pérez, María S. and Corcho, Oscar},
  journal   = {Semantic Web},
  year      = {2024},
  volume    = {15},
  number    = {1},
  pages     = {1-20},
  issn      = {2210-4968},
  publisher = {IOS Press},
  doi       = {10.3233/SW-223135}
}
```

---

## Recommended citation for this repository

If you refer to the software itself, please cite:

**Moretti, Arianna, and Sebastian Barzaghi.**
*Morph-KGChad (Morph-KGC CHANGES Metadata).*
Software repository.
[https://github.com/dharc-org/morph-kgc-changes-metadata](https://github.com/dharc-org/morph-kgc-changes-metadata)

If you refer to the documented workflow and case study, please also cite:

**Barzaghi, Sebastian, Arianna Moretti, Ivan Heibi, and Silvio Peroni.**
“CHAD-KG: A Knowledge Graph for Representing Cultural Heritage Objects and Digitisation Paradata.”
arXiv preprint, 19 May 2025.
[https://doi.org/10.48550/arXiv.2505.13276](https://doi.org/10.48550/arXiv.2505.13276)

