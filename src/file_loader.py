import pandas
from src.logger import log_event

def load_csv(filepath):
    """
    Load a CSV file and return a pandas DataFrame.
    """
    try:
        raw_data = pandas.read_csv(filepath)
        log_event(f"Successfully loaded data from {filepath}","EVENT")
        return raw_data
    except Exception as e:
        log_event(f"ERROR loading CSV: {e}","ERROR")
        return None
