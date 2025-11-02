import sys
import glob
import os
import json
import re
import pandas as pd
import multiprocessing


def get_column_names(schemas, ds_name, sorting_key='column_position'):
    """
    Retrieve ordered column names from the schema for a specific dataset.

    Args:
        schemas (dict): Loaded schema definitions.
        ds_name (str): Dataset name (e.g., 'orders', 'customers').
        sorting_key (str): Schema key used to order columns (default: 'column_position').

    Returns:
        List[str]: List of column names in correct order.
    """
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]


def read_csv(file, schemas):
    """
    Read a CSV file into a chunked DataFrame iterator using schema-based column names.

    Args:
        file (str): Full path to the CSV file.
        schemas (dict): Schema dictionary for column name mapping.

    Returns:
        Iterator[pd.DataFrame]: Chunked DataFrame reader (chunksize=10,000).
    """
    file_path_list = re.split(r'[/\\]', file)
    ds_name = file_path_list[-2]  # Dataset is the folder name
    columns = get_column_names(schemas, ds_name)
    df_reader = pd.read_csv(file, names=columns, chunksize=10000)
    return df_reader


def to_sql(df, db_conn_uri, ds_name):
    """
    Write a DataFrame to a PostgreSQL table using SQLAlchemy URI.

    Args:
        df (pd.DataFrame): The DataFrame to write.
        db_conn_uri (str): SQLAlchemy connection URI for PostgreSQL.
        ds_name (str): Target table name (same as dataset name).
    """
    df.to_sql(
        ds_name,
        db_conn_uri,
        if_exists='append',
        index=False
    )


def db_loader(src_base_dir, db_conn_uri, ds_name):
    """
    Read CSV files for a dataset and insert into PostgreSQL in chunks.

    Args:
        src_base_dir (str): Base directory containing datasets and schema.
        db_conn_uri (str): SQLAlchemy DB URI.
        ds_name (str): Dataset name to load.
    """
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')

    if not files:
        raise NameError(f'No files found for {ds_name}')

    for file in files:
        df_reader = read_csv(file, schemas)
        for idx, df in enumerate(df_reader):
            print(f'Populating chunk {idx} of {ds_name}')
            to_sql(df, db_conn_uri, ds_name)


def process_dataset(args):
    """
    Wrapper function for multiprocessing — processes one dataset at a time.

    Args:
        args (Tuple): (src_base_dir, db_conn_uri, ds_name)
    """
    src_base_dir, db_conn_uri, ds_name = args
    try:
        print(f'Processing {ds_name}')
        db_loader(src_base_dir, db_conn_uri, ds_name)
    except NameError as ne:
        print(ne)
    except Exception as e:
        print(e)
    finally:
        print(f'Data Processing of {ds_name} is completed successfully')


def process_files(ds_names=None):
    """
    Main processing function that sets up DB URI and runs multiprocessing pool.

    Args:
        ds_names (list, optional): List of dataset names to process. If None, all from schema will be processed.
    """
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')

    # Construct PostgreSQL connection URI
    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'

    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()

    # Use up to 8 parallel processes
    pprocess = min(len(ds_names), 8)
    pool = multiprocessing.Pool(pprocess)

    # Prepare arguments for each dataset
    pd_args = [(src_base_dir, db_conn_uri, ds_name) for ds_name in ds_names]

    # Run dataset loaders in parallel
    pool.map(process_dataset, pd_args)


if __name__ == '__main__':
    """
    Entry point for the script. Accepts optional dataset list via command-line JSON string.
    Example:
        python loader.py '["orders", "customers"]'
    """
    if len(sys.argv) == 2:
        ds_names = json.loads(sys.argv[1])
        process_files(ds_names)
    else:
        process_files()
