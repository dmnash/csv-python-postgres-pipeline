# file_loader.py

import pandas
from src.logger import log_event

def load_csv(runtime_config, schema_definitions, csv_path):
    """
    Load a CSV file and return a pandas DataFrame.
    """
    try:
        raw_data = pandas.read_csv(csv_path)
        """
        validate data for expected column types; reject if missing columns, accept extraneous columns but log exception
        """
        log_event(f"Successfully loaded data from {csv_path}","EVENT")
        return raw_data
    except Exception as e:
        log_event(f"ERROR loading CSV: {e}","ERROR")
        return None