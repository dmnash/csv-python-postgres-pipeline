# file_loader.py

import pandas
from src.logger import log_event

def load_csv(runtime_config, schema_keys, csv_path):
    """
    Load a CSV file and return a pandas DataFrame.
    """
    try:
        raw_data = pandas.read_csv(csv_path)
        csv_keys = set(raw_data.columns)
        missing_cols = schema_keys - csv_keys
        if missing_cols:
            log_event(runtime_config, f"Missing required columns: {missing_cols}", "ERROR")
            return None
        extraneous_cols = csv_keys - schema_keys
        if extraneous_cols:
            log_event(runtime_config, f"Extraneous columns: {sorted(list(extraneous_cols))}", "INGEST")
        raw_data = raw_data[schema_keys]
        log_event(runtime_config, f"Successfully loaded data from {csv_path}","INGEST")
        return raw_data
    except Exception as e:
        log_event(runtime_config, f"ERROR loading CSV: {e}","ERROR")
        return None