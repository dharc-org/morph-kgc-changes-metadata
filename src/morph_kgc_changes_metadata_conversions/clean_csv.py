import pandas as pd
import re

# Path to the CSV file
csv_file_path = 'src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv'

# Read the CSV file
df = pd.read_csv(csv_file_path, delimiter=',', quotechar='"', encoding='utf-8')

# Remove spaces from column names
df.columns = df.columns.str.strip()

# Remove leading and trailing spaces from each cell
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Write the data back to the same CSV file (overwriting)
df.to_csv(csv_file_path, index=False, sep=',', quotechar='"', encoding='utf-8')

print(f"File '{csv_file_path}' has been cleaned from unnecessary spaces.")


def process_language_code(title):
    """
    Extracts the language code from the string in the format "Title @lang".
    :param title: The string containing the title and language in the format "Title @lang".
    :return: The language code.
    """
    if not isinstance(title, str):
        return None

    # language pattern
    pattern = r'^(.*) @(\w+)$'
    match = re.match(pattern, title)

    if match:
        lang = match.group(2).strip()

        # mapping language codes
        lang_mapping = {
            'lat': 'la',
            'gre': 'grc',
            'ita': 'it',
        }

        lang_code = lang_mapping.get(lang, 'und')  # 'und' for undefined language
        return lang_code

    return None


def add_language_column(csv_file_path, output_file_path):
    """
    Reads the CSV file, extracts the language code from the "Original Title" column,
    and adds a new column "Original Title Language" to the CSV.
    :param csv_file_path: The path to the input CSV file.
    :param output_file_path: The path to the output CSV file.
    """
    # read CSV
    df = pd.read_csv(csv_file_path)

    # add new column with language code
    df['Lingua titolo originale'] = df['Titolo originale'].apply(process_language_code)

    # save csv with new column
    df.to_csv(output_file_path, index=False)
    print(f"File salvato con successo: {output_file_path}")


# usage
input_csv = csv_file_path
output_csv = csv_file_path
add_language_column(input_csv, output_csv)

# NAN TO EMPTY STRING

# csv file
csv_file_path = "src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv"
df = pd.read_csv(csv_file_path, delimiter=',', quotechar='"', encoding='utf-8')

# subs NaN with empty strings
df = df.fillna('')

# save modified dataframe
preprocessed_csv_file_path = "src/morph_kgc_changes_metadata_conversions/metadata_aldrovandi.csv"
df.to_csv(preprocessed_csv_file_path, index=False, quoting=1, encoding='utf-8')

print(f"Preprocessed CSV saved to: {preprocessed_csv_file_path}")