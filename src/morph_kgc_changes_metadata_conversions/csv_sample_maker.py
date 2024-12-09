import csv
import pandas as pd
import os

def sample_csv(input_file, output_file, num_lines=3):
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)

        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)

            # Read and write the first `num_lines` lines
            for i, row in enumerate(reader):
                if i < num_lines:
                    writer.writerow(row)
                else:
                    break


def extract_minimum_sample(file_path, output_dir):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Initialize a set to keep track of filled fields
    filled_fields = set()
    # Initialize a list to collect selected rows
    selected_rows = []

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        # Add the current row to the selected rows
        selected_rows.append(row)

        # Update the filled fields with non-null values from the current row
        for col in df.columns:
            if pd.notnull(row[col]):
                filled_fields.add(col)

        # Check if all fields are filled
        if len(filled_fields) == len(df.columns):
            break

    # Create a new DataFrame with the selected rows
    sample_df = pd.DataFrame(selected_rows)

    # Save the sample DataFrame to a CSV file
    output_file_path = os.path.join(output_dir, 'minimum_sample.csv')
    sample_df.to_csv(output_file_path, index=False, encoding='utf-8')

    return sample_df


def clean_sample(file_path, output_file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Initialize a set to keep track of filled fields
    filled_fields = set()
    # Initialize a list to collect valid rows
    valid_rows = []

    # Iterate through each row in the DataFrame
    for index, row in df.iterrows():
        new_fields = set()

        # Check each column for filled values
        for col in df.columns:
            if pd.notnull(row[col]) and col not in filled_fields:
                new_fields.add(col)

        # If the row contributes new fields, add it to valid rows and update filled fields
        if new_fields:
            valid_rows.append(row)
            filled_fields.update(new_fields)

    # Create a new DataFrame with valid rows
    cleaned_df = pd.DataFrame(valid_rows)

    # Save the cleaned DataFrame to a CSV file
    cleaned_df.to_csv(output_file_path, index=False, encoding='utf-8')

    return cleaned_df


if __name__ == '__main__':
    #  usage
    input_csv = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
    output_csv = 'src/morph_kgc_changes_metadata_conversions/sample_input_file.csv'
    # sample_csv(input_csv, output_csv, num_lines=4)


    # Input file path
    csv_file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'
    output_dir = 'src/morph_kgc_changes_metadata_conversions/output_dir'

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Extract the minimum sample and save to CSV
    sample_df = extract_minimum_sample(csv_file_path, output_dir)

    if not sample_df.empty:
        print("Sample extracted and saved to:", os.path.join(output_dir, 'minimum_sample.csv'))
    else:
        print("No valid sample could be extracted.")

    # Input and output file paths
    input_file_path = 'src/morph_kgc_changes_metadata_conversions/output_dir/minimum_sample.csv'
    output_file_path = 'src/morph_kgc_changes_metadata_conversions/output_dir/cleaned_sample.csv'

    # Clean the sample and save to CSV
    cleaned_df = clean_sample(input_file_path, output_file_path)

    if not cleaned_df.empty:
        print("Cleaned sample saved to:", output_file_path)
    else:
        print("No valid sample could be extracted.")
