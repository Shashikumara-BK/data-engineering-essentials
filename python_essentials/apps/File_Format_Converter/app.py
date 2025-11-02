import glob
import os
import sys
import json
import re
import pandas as pd


def get_column_names(schemas, ds_name, sorting_key='column_position'):
    """
    Get the list of column names for a given dataset based on the schema.

    Args:
        schemas (dict): Loaded schema JSON.
        ds_name (str): Dataset name (e.g., 'orders', 'customers').
        sorting_key (str): Key used to sort columns (default is 'column_position').

    Returns:
        List[str]: Sorted list of column names.
    """
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]


def read_csv(file, schemas):
    """
    Read a CSV file using schema-based column names.

    Args:
        file (str): Full path to the CSV file.
        schemas (dict): Loaded schema JSON.

    Returns:
        pd.DataFrame: DataFrame with properly named columns.
    """
    file_path_list = re.split('[/\\\]', file)
    ds_name = file_path_list[-2]  # Dataset name from folder
    file_name = file_path_list[-1]
    columns = get_column_names(schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df


def to_json(df, tgt_base_dir, ds_name, file_name):
    """
    Convert a DataFrame to newline-delimited JSON and save it.

    Args:
        df (pd.DataFrame): DataFrame to convert.
        tgt_base_dir (str): Target base directory to store JSON files.
        ds_name (str): Dataset name.
        file_name (str): Name of the JSON file to be saved.
    """
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)

    df.to_json(
        json_file_path,
        orient='records',
        lines=True  # JSON lines format
    )


def file_converter(src_base_dir, tgt_base_dir, ds_name):
    """
    Convert all CSV files for a dataset into JSON format.

    Args:
        src_base_dir (str): Source directory containing CSV files and schema.
        tgt_base_dir (str): Destination directory to store JSON files.
        ds_name (str): Dataset name.
    """
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))

    # Match files like part-0000.csv inside each dataset folder
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f"file not found {ds_name}")

    for file in files:
        df = read_csv(file, schemas)
        file_name = re.split('[/\\\]', file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)


def process_files(ds_names=None):
    """
    Process one or multiple datasets and convert them to JSON.

    Args:
        ds_names (list, optional): List of dataset names. If not provided, all datasets in schema will be processed.
    """
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    tgt_base_dir = os.environ.get('TGT_BASE_DIR')

    schemas = json.load(open(f'{src_base_dir}/schemas.json'))

    # If no dataset names provided, process all in schema
    if not ds_names:
        ds_names = schemas.keys()

    for ds_name in ds_names:
        try:
            print(f'Processing {ds_name}')
            file_converter(src_base_dir, tgt_base_dir, ds_name)
        except NameError as ne:
            print(ne)
            print(f"Error processing {ds_name}")
            pass


if __name__ == '__main__':
    """
    Entry point of the script. Allows running via command line:
    python file_converter.py '["orders", "customers"]'
    """
    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1])  # Corrected `==` to `=`
        process_files(ds_names)
    else:
        process_files()
