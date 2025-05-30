# validator.py

import pandas as pd
import src.validation_library as vl
from src.logger import log_event

def validate_data(runtime_config, schema, raw_data):
    cascade_reject = runtime_config["cascade_reject"]
    primary_key = schema["primary_key"]
    group_reject = schema["group_reject"]
    schema_definitions = schema["schema_definitions"]
    dispatch_table = vl.build_dispatch_table

    valid_data = []
    rejected_data = {}

    null_key_rows = raw_data[raw_data[primary_key].isnull()]
    if not null_key_rows.empty:
        log_event(runtime_config, {"message": "Null key rows dropped:","log_type": "INGEST","log_class": "info_detailed","called_by": "validate_data"})
        for idx, row in null_key_rows.iterrows():
            log_event(runtime_config, {"message": row["source_index"],"log_type": "INGEST","log_class": "info_detailed","called_by": "validate_data"})
        log_event(runtime_config, {"message": "==end null key rows==","log_type": "INGEST","log_class": "info_detailed","called_by": "validate_data"})
    grouped_data = raw_data.groupby(primary_key)

    for primary_key_value, group in grouped_data:
        group_rejected = False
        group_reject_reason = f"group {primary_key_value} rejected:\n"

        for idx, row in group.iterrows():
            row_rejected = False
            
            for col_name, col_rules in schema_definitions.items():
                test_value = row[col_name]
                rules = col_rules.get("rules",{})

                for rule_name, schema_rule in rules.items():
                    result = validation_engine(runtime_config, dispatch_table, rule_name, schema_rule, test_value, col_name, row["source_index"])
                    if not result["valid"]:
                        row_rejected = True
                        if group_reject:
                            group_reject_reason += f"{result['message']}\n"
                        if cascade_reject:
                            break
                            
                if cascade_reject and row_rejected:
                    break

            if group_reject and cascade_reject and row_rejected:
                break

            if not row_rejected and not group_reject:
                valid_data.append(row.to_frame().T)

        if group_reject:
            if not group_rejected:
                valid_data.append(group)
    
    log_event(runtime_config, {"message": "Validation module called","log_type": "EVENT","log_class": "info_general","called_by": "validate_data"})
    return pd.concat(valid_data) if valid_data else pd.DataFrame()

def validation_engine(runtime_config, dispatch_table, rule_name, schema_rule, test_value, col_name=None, source_index=None):
    validation_func = dispatch_table.get(rule_name)
    try:
        result = validation_func(schema_rule, test_value)
        result.setdefault("log_type", "INGEST")
        result.setdefault("col_name", col_name)
        result.setdefault("source_index", source_index)
        result["message"] = f"row {result['source_index']} rejected at {result['col_name']}: {result['message']}"
        log_event(runtime_config, result)
        return result
    except Exception as e:
        result = {
            "valid": False, "log_type": "EXCEPTION", "log_class": "error_critical", 
            "message": f"{validation_func.__name__} failed: {e}", 
            "col_name": col_name, "source_index": source_index
            }
        log_event(runtime_config, result)
        return result
