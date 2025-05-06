# file_loader.py

import pandas
import src.validation_library as vl
from src.logger import log_event

def load_csv(runtime_config, schema_keys):
    """
    Load a CSV file and return a pandas DataFrame.
    """
    reserved_cols = {"source_index"}
    try:
        csv_path = runtime_config["csv_path"]
        raw_data = pandas.read_csv(csv_path)
        csv_keys = set(raw_data.columns)

        missing_cols = schema_keys - csv_keys
        if missing_cols:
            log_event(runtime_config, f"Missing required columns: {missing_cols}", "ERROR")
            return None
        
        extraneous_cols = csv_keys - schema_keys
        if extraneous_cols:
            if runtime_config["drop_extra_cols"]:
                raw_data = raw_data[list(schema_keys)]
            log_event(runtime_config, f"Extraneous columns {'dropped' if runtime_config["drop_extra_cols"]==True else 'found'}: {sorted(list(extraneous_cols))}", "INGEST")

        rename_map = {col:f"input_{col}" for col in raw_data.columns if col in reserved_cols}
        if rename_map:
            log_event(runtime_config, f"Conflicting columns renamed: {rename_map}", "INGEST")
            raw_data.rename(columns=rename_map, inplace=True)
    
        raw_data.insert(0, "source_index", range(len(raw_data)))
    
        log_event(runtime_config, f"Successfully loaded data from {csv_path}","INGEST")
        return raw_data
    
    except Exception as e:
        log_event(runtime_config, f"ERROR loading CSV: {e}","ERROR")
        return None