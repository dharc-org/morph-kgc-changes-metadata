import pandas as pd
import os
import json
import re

# Define input file and output folder
process_input = "src/morph_kgc_changes_metadata_conversions/output_dir/acquisizione_aldrovandi_clean_restructured_final_merged.csv"  # Change this to your file (can be .csv or .xlsx)
object_input = "src/morph_kgc_changes_metadata_conversions/dataset/dataset_oggetto_aldrovandi/dataset_oggetto_aldrovandi.csv"  # Change this to your file (can be .csv or .xlsx)
output_folder = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed"
output_object = os.path.join(output_folder, "nr_dictionary_object.json")
output_process = os.path.join(output_folder, "nr_dictionary_process.json")

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)


# PROCESS

# Read the table (CSV or Excel)
if process_input.endswith(".csv"):
    df = pd.read_csv(process_input)
elif process_input.endswith(".xlsx"):
    df = pd.read_excel(process_input)
else:
    raise ValueError("Unsupported file format. Use CSV or XLSX.")

# Check if "NR" column exists
if "NR" not in df.columns:
    raise KeyError("The column 'NR' is missing in the input file.")

# Function to clean NR values
def clean_value(value):
    if pd.isna(value):  # Handle NaN values
        return None
    value = str(value).strip()  # Remove leading/trailing spaces
    value = re.sub(r'\s+', ' ', value)  # Replace multiple spaces with single space
    value = re.sub(r'[\s\-_]+', '_', value)  # Replace spaces, "-" and "_" with "_"
    value = value.lower()
    return value

# Create the dictionary
nr_dict = {str(val): clean_value(val) for val in df["NR"].dropna()}

# Save the dictionary as JSON
with open(output_process, "w", encoding="utf-8") as f:
    json.dump(nr_dict, f, ensure_ascii=False, indent=4)

print(f"Dictionary saved to {output_process}")


# OBJECT

# Read the table (CSV or Excel)
if object_input.endswith(".csv"):
    df = pd.read_csv(object_input)
elif object_input.endswith(".xlsx"):
    df = pd.read_excel(object_input)
else:
    raise ValueError("Unsupported file format. Use CSV or XLSX.")

# Check if "NR" column exists
if "NR" not in df.columns:
    raise KeyError("The column 'NR' is missing in the input file.")






# Create a dictionary to track occurrences of each value
occurrences = {}

# Create the dictionary with unique keys
nr_dict = {}

for val in df["NR"].dropna():
    val_str = str(val).strip()  # Convert to string and remove leading/trailing spaces
    clean_val = clean_value(val_str)  # Apply the cleaning function

    # Track occurrences of this cleaned value
    if val in occurrences:
        occurrences[val] += 1
        unique_key = f"{val}_repeated_{occurrences[val]}"  # Rename duplicate
    else:
        occurrences[val] = 0  # First occurrence
        unique_key = val  # Keep original for first time

    nr_dict[unique_key] = clean_val  # Add to dictionary



# Save the dictionary as JSON
with open(output_object, "w", encoding="utf-8") as f:
    json.dump(nr_dict, f, ensure_ascii=False, indent=4)

print(f"Dictionary saved to {output_object}")




### CREATE REPORT OF THE ID CHANGINGS


import os
import json
from collections import defaultdict

# Directory containing JSON files
input_folder = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed"
output_file = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed_stat/report.json"

# Initialize storage structures
all_keys = set()  # Unique keys
all_values_list = []  # List of all values
all_values_set = set()  # Unique values
mapping_dict = defaultdict(list)  # Dictionary mapping values to their corresponding keys

# Process each JSON file in the directory
for filename in os.listdir(input_folder):
    if filename.endswith(".json"):  # Process only JSON files
        file_path = os.path.join(input_folder, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

                # Ensure data is a dictionary
                if isinstance(data, dict):
                    for key, value in data.items():
                        all_keys.add(key)  # Add key to unique key set
                        all_values_list.append(value)  # Add value to value list
                        all_values_set.add(value)  # Add value to unique value set
                        mapping_dict[value].append(key)  # Map value to its key(s)
                else:
                    print(f"Skipping {filename}: JSON is not a dictionary.")

        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Create output report
report = {
    "number_of_unique_keys": len(all_keys),
    "number_of_total_keys": sum(len(json.load(open(os.path.join(input_folder, file), "r", encoding="utf-8")).keys())
                                for file in os.listdir(input_folder) if file.endswith(".json")),
    "number_of_total_values": len(all_values_list),
    "number_of_unique_values": len(all_values_set),
    "mapping_dictionary": mapping_dict
}

# Save report as JSON
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=4)

print(f"Report saved to {output_file}")



### REPORT OF THE REPORT

import json

# Input JSON file
input_file = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed_stat/report.json"
output_file = "src/morph_kgc_changes_metadata_conversions/output_dir/nr_fixed_stat/report_of_report.json"

# Read the input JSON file
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Extract the mapping dictionary
mapping_dict = data.get("mapping_dictionary", {})

# Initialize counters and storage structures
size_categories = {1: [], 2: [], 3: [], "more_than_3": []}
equal_values_keys = []  # Keys with lists of equal strings
unequal_values_keys = []  # Keys with lists of unequal strings

# Analyze the mapping dictionary
for key, values in mapping_dict.items():
    if isinstance(values, list):  # Ensure values are lists
        list_length = len(values)

        # Categorize by list length
        if list_length == 1:
            size_categories[1].append(key)
        elif list_length == 2:
            size_categories[2].append(key)
        elif list_length == 3:
            size_categories[3].append(key)
        elif list_length > 3:
            size_categories["more_than_3"].append(key)

        # Check if all elements in the list are the same
        if all(v == values[0] for v in values):
            equal_values_keys.append(key)
        else:
            unequal_values_keys.append(key)

# Calculate total keys
total_keys = sum(len(v) for v in size_categories.values())

# Compute percentages
size_percentages = {
    "percentage_1_element": round(len(size_categories[1]) / total_keys * 100, 2) if total_keys > 0 else 0,
    "percentage_2_elements": round(len(size_categories[2]) / total_keys * 100, 2) if total_keys > 0 else 0,
    "percentage_3_elements": round(len(size_categories[3]) / total_keys * 100, 2) if total_keys > 0 else 0,
    "percentage_more_than_3_elements": round(len(size_categories["more_than_3"]) / total_keys * 100, 2) if total_keys > 0 else 0
}

# Prepare the final dictionary
analysis_report = {
    "num_keys_with_1_element": len(size_categories[1]),
    "num_keys_with_2_elements": len(size_categories[2]),
    "num_keys_with_3_elements": len(size_categories[3]),
    "num_keys_with_more_than_3_elements": len(size_categories["more_than_3"]),
    "percentage_1_element": size_percentages["percentage_1_element"],
    "percentage_2_elements": size_percentages["percentage_2_elements"],
    "percentage_3_elements": size_percentages["percentage_3_elements"],
    "percentage_more_than_3_elements": size_percentages["percentage_more_than_3_elements"],
    "keys_with_1_element": size_categories[1],
    "keys_with_2_elements": size_categories[2],
    "keys_with_3_elements": size_categories[3],
    "keys_with_more_than_3_elements": size_categories["more_than_3"],
    "keys_with_equal_values": equal_values_keys,
    "keys_with_unequal_values": unequal_values_keys
}

# Save report as JSON
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(analysis_report, f, ensure_ascii=False, indent=4)

print(f"Analysis report saved to {output_file}")
