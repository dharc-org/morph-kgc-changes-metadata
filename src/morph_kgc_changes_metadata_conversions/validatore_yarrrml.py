import ruamel.yaml

yaml = ruamel.yaml.YAML()
try:
    with open("src/morph_kgc_changes_metadata_conversions/mapping_file_acquisition.yaml", "r") as file:
        yaml.load(file)
    print("Il file YAML è valido.")
except ruamel.yaml.YAMLError as e:
    print(f"Errore nel file YAML: {e}")