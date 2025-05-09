# validator.py

import pandas as pd
import src.validation_library as vl
from src.logger import log_event

def validate_data(runtime_config, schema, raw_data):
    cascade_reject = runtime_config["cascade_reject"]
    primary_key = schema["primary_key"]
    group_reject = schema["group_reject"]
    schema_definitions = schema["schema_definitions"]
    valid_data = []
    rejected_data = {}
    rule_name = ""

    null_key_rows = raw_data[raw_data[primary_key].isnull()]
    if not null_key_rows.empty:
        log_event(runtime_config, "Null key rows dropped:", "INGEST")
        for idx, row in null_key_rows.iterrows():
            log_event(runtime_config, row["source_index"], "INGEST")
        log_event(runtime_config, "==end null key rows==", "INGEST")
    grouped_data = raw_data.groupby(primary_key)

    for primary_key_value, group in grouped_data:
        group_rejected = False
        group_reject_reason = ""

        for idx, row in group.iterrows():
            row_rejected = False
            rejection_reason = ""

            for col_name, col_rules in schema_definitions.items():
                test_value = row[col_name]
                rules = col_rules.get("rules",{})

                for rule_name, schema_rule in rules.items():
                    validate_func = vl.dispatch_table.get(rule_name)
                    if not validate_func:
                        log_event(runtime_config, f"'{rule_name}' validation function not found for column '{col_name}'", "ERROR")
                        continue
                    check_result, check_msg = validate_func(schema_rule, test_value)
                    
                    if check_result in ("RED", "YELLOW"):
                        rejection_reason = f"row {row['source_index']} {'rejected at' if check_result == 'RED' else 'information at'} {col_name}: {check_msg}"
                        log_event(runtime_config, rejection_reason, "INGEST")
                        if group_reject:
                            group_reject_reason += rejection_reason
                        if check_result == "RED":
                            row_rejected = True
                            if group_reject:
                                group_rejected = True
                            if cascade_reject:
                                break
                            
                if cascade_reject and row_rejected:
                    break

            if cascade_reject and group_reject and row_rejected:
                break

            if not row_rejected and not group_reject:
                valid_data.append(row.to_frame().T)

        if group_reject:
            if not group_rejected:
                valid_data.append(group)
    
    log_event(runtime_config, "Validation module called (no rules applied)","EVENT")
    return pd.concat(valid_data) if valid_data else pd.DataFrame()