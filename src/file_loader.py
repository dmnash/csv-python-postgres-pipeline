import pandas as pd
from src.logger import log_event

def load_csv(filepath):
    """
    Load a CSV file and return a pandas DataFrame.
    """
    try:
        df = pd.read_csv(filepath)
        log_event(f"Successfully loaded data from {filepath}")
        return df
    except Exception as e:
        log_event(f"ERROR loading CSV: {e}")
        return None
