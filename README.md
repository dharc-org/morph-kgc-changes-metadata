
# CHANGES Metadata Conversion Plugin
## To Do:
- produce mapping for acquisition process 
- Manage both VIAF and ULAN in case a different id is found 
- Issue with angular brackets, quotes, and datatype managed in pre or post processing. To be authomatized with mapping. See also: Issue with specifying types (IRI/string/etc) when using a function. Check if it's manageable also with YARRRML (it is possible with a RML mapping, try using the official converter). For now: default post-processing is executed on the first version of the RDF file just produced.
- Similarly, the ```clean_csv``` is necessary for extracting the language - Verify with the suggestion by the developer.
- Clarify licenses and IRI of licenses. 
- consider uniforming name-surname order.
- Keep for now a unique file for all the triples, consider separating for each entity. 
- make the code command-line-executable
- implementing the possibility of splitting the files production (one for each aton object, for example)
- flask webserver for easily exploitable UI 
- Fonte,Immagine digitale, Iconografia non sono attualmente modellate



## How to run the code

### preprocessing
```
python src/morph_kgc_changes_metadata_conversions/clean_csv.py
```
- change manually the paths of the input and the output file (where the input file is the raw one and the output file is the postprocessed input. In the current execution, the file is overwritten)

### triples production and postprocessing
```
python main_aldrovandi.py
```
- change manually input, output, mapping and configuration paths if needed
- the execution of this file, postprocess the produced data and fixes issues related to the dataypes of the subjects and objects of the produced triples, where needed. 

### current mapping file path 
```
src/morph_kgc_changes_metadata_conversions/sample_mapping_file.yaml
```

### structure of the input file
- the code currently accepts as input csv tables structured as the sample at path: 

```
src/morph_kgc_changes_metadata_conversions/sample_input_3_entries.csv
```

### Understanding and Running the scripts - Changes
Premises

To execute the conversion process from CSV to RDF serialization, the Morph-kgc code (https://morph-kgc.readthedocs.io/en/stable/) has been extended. Morph-kgc is software based on the use of RDF Mapping Language (RML, https://rml.io/specs/rml/) conversion technology. Below, the main components are described to understand their structure and usage.

Objectives

Interaction with the software extension is minimal and aimed at producing data in Turtle RDF serialization. This allows leveraging the semantic potential of the information internally connected and with external resources.

Operational Actions

A general overview of the software components and the possible user interactions is provided below. However, for more detailed information on using the software to produce data in Turtle RDF format, please refer to the README.md file, which will be updated alongside the code.

Mapping Files – Definition of Conversion Rules

These consist of two YARRRML mapping files (https://rml.io/yarrrml/), one for each of the input datasets and modules of the Application Profile (objects and acquisition). These files define the rules for converting data into RDF format based on the project’s Application Profile. Users do not interact with these files as they are precompiled to cover all scenarios presented in datasets formulated according to the previously described guidelines.

Configuration File – Configuration of Conversion Parameters

This is an .ini file where the values of parameters concerning general configurations and each input dataset are defined. For compiling the configuration file, refer to the aforementioned README.md file.

In the section concerning the configuration of general parameters ([CONFIGURATION]), both mandatory and optional, the user defines the name for the output file (output_file), the format (output_format), and the directory where the file will be saved (output_dir).

Regarding the configuration of parameters for managing input, it is useful to note that it is possible to handle multiple datasets simultaneously, each with its own mapping file. Therefore, for each data source, a dedicated section will be added (in the format [<source_name>], for example: [DataSource1]), within which the file_path parameter value will specify the path to the input CSV file. The user must specify in [DataSource1] the file_path parameter value as the path to the Objects dataset CSV, while in [DataSource2], the file_path parameter value to be specified will be the path to the acquisition process CSV dataset.

```
[CONFIGURATION]
na_values = ,#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,nan,None
output_file = knowledge-graph.ttl
output_dir = src/morph_kgc_changes_metadata_conversions/output_dir
output_format = N-TRIPLES
only_printable_characters = no
safe_percent_encoding =
mapping_partitioning = PARTIAL-AGGREGATIONS
infer_sql_datatypes = no
logging_level = INFO
logs_file =
oracle_client_lib_dir =
oracle_client_config_dir =

[DataSource1]
mappings = src/morph_kgc_changes_metadata_conversions/sample_mapping_file.yaml
mapping_format = YARRRML
file_path = src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv
delimiter = ,
quotechar = "
encoding = utf-8
```

- User-Defined Functions – Handling Specific Cases and Interpreting Complex Data. These are declarative transformation functions implemented through the RML-FNML (RML Function Mapping Language) language. It is important to note that RML already provides a set of built-in functions that allow handling the interpretation of common formalizations across various input datasets, such as extracting multiple values separated by delimiters within the same cell. Additionally, for the case study, further functions have been added to perform value conversion processes characteristic of the case study, such as converting the string of the technique used. Generally, no user intervention is required unless there is a need to insert a value currently not anticipated within one of the controlled value sets, such as in the case of techniques with AAT codes. Refer to the next section (Data Extension) for the extension process. 
- Launch Script – Executing the Conversion. In addition to orchestrating the execution of the conversion, the launch scripts allow for the pre-processing of datasets, cleaning and normalizing tabular metadata, preparing them for RDF serialization, and resolving any discrepancies that could prevent accurate data extraction. As a temporary solution, the script also performs post-processing of the data, correcting any formal errors in the produced RDF files. For more detailed information on the script launch commands, refer to the README.md file in the GitHub repository.

The command to execute the code is:

```python convert_to_rdf.py -obj <csv_input_file_objects> -acq <csv_input_file_acquisition>```

Summary of Sections:

	1.	Premises: Introduction to the software and its purpose.
	2.	Objectives: Goals of using the software extension.
	3.	Operational Actions: Overview of software components and user interactions.
	•	Mapping Files: Definition and purpose.
	•	Configuration File: Setting parameters for conversion.
	•	User-Defined Functions: Handling specific cases and complex data interpretations.
	•	Launch Script: Executing the conversion process.

Additional Notes:

	•	Mapping Files: Ensure that the YARRRML mapping files are correctly formatted and correspond to each input dataset.
	•	Configuration Parameters: Carefully set the output_file, output_format, and output_dir to match your requirements.
	•	User-Defined Functions: Extend or modify these functions as needed for handling specific data transformation cases.
	•	Launch Scripts: Verify that all dependencies and paths are correctly set before executing the conversion command.


The software is based on Morph-KGC
 **[SoftwareX](https://www.sciencedirect.com/science/article/pii/S2352711024000803)** and **[SWJ](https://www.doi.org/10.3233/SW-223135)** papers:

```bib
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
