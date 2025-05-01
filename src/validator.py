#validator.py

import pandas as pd
from src.logger import log_event

def exist_notnull():
    pass # this will be the exists-and-not-null function that required fields all apply

def format_compliance():
    pass # this will be the regex format compliance test

def acceptable_value():
    pass # this will be the permitted values test

def forbidden_value():
    pass # this will be the forbidden values test

def valid_datatype():
    pass # this will be the data type validation test

def validate_data(runtime_config, schema, raw_data):
    primary_key = schema["primary_key"]
    group_reject = schema["group_reject"]
    schema_definitions = schema["schema_definitions"]
    sorted_data = raw_data.sort_values(by=primary_key, na_position="first")
    grouped_data = sorted_data.groupby(primary_key)
    valid_data = []
    rejected_orders = set()

    for primary_key_value, group in grouped_data:
        order_rejected = False
        for idx, row in group.iterrows():
            row_valid = True # ADD VALIDATION HERE
            if not row_valid:
                log_event(f"Row {idx} failed validation", "ERROR")
                if group_reject:
                    order_rejected = True
                break
        if not order_rejected:
            valid_data.append(group)
        else:
            rejected_orders.add(primary_key_value)
    
    log_event("Validation module called (no rules applied)","EVENT")
    return pd.concat(valid_data) if valid_data else pd.DataFrame()