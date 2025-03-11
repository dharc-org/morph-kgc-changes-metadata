import pandas as pd
import json
import re

# Input file paths
csv_file_obj = "src/morph_kgc_changes_metadata_conversions/dataset/dataset_oggetto_aldrovandi/dataset_oggetto_aldrovandi.csv"
csv_file_pro = "src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_aldrovandi_clean_restructured_final_merged.csv"

map_json_obj = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed/nr_dictionary_object.json"
map_json_pro = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed/nr_dictionary_process.json"

output_csv_obj = "src/morph_kgc_changes_metadata_conversions/output_dir/new_tabs_for_final_manual_update/aldrovandi_obj.csv"
output_csv_pro = "src/morph_kgc_changes_metadata_conversions/output_dir/new_tabs_for_final_manual_update/aldrovandi_pro.csv"

# Function to capitalize words properly
def capitalize_words(value):
    if pd.isna(value) or not isinstance(value, str):  # Handle NaN values and non-strings
        return value
    value = re.sub(r'\s+', ' ', value.strip())  # Remove multiple spaces
    return ' '.join(word.capitalize() for word in value.split(' '))  # Capitalize each word

# Function to process a CSV with NR mapping and capitalization
def process_csv(csv_file, json_map_file, output_csv):
    # Load the CSV file
    df = pd.read_csv(csv_file, dtype=str)

    # Load the JSON mapping
    with open(json_map_file, "r", encoding="utf-8") as f:
        mapping_dict = json.load(f)

    # Replace values in the "NR" column using the JSON mapping
    df["NR"] = df["NR"].map(mapping_dict).fillna(df["NR"])  # Keeps original value if no match found

    # Apply Capitalized Words conversion to all columns EXCEPT "NR"
    for col in df.columns:
        if col != "NR":
            df[col] = df[col].apply(capitalize_words)

    # Save the transformed CSV
    df.to_csv(output_csv, index=False)

    print(f"✅ CSV transformation complete! Saved as {output_csv}")

# Process both OBJECT and PROCESS CSVs
process_csv(csv_file_obj, map_json_obj, output_csv_obj)
process_csv(csv_file_pro, map_json_pro, output_csv_pro)