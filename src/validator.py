# validator.py

import re
import pandas as pd
from src.logger import log_event

def error_handler(rule_func):
    def wrapper(schema_rule, test_value):
        try:
            return rule_func(schema_rule, test_value)
        except Exception as e:
            return False, f"{rule_func.__name__} error: {str(e)}"
    return wrapper

@error_handler
def exist_notnull(schema_rule, test_value):
    check_result = True
    check_msg = ""
    if not schema_rule:
        return check_result, check_msg
    if test_value is None or pd.isnull(test_value):
        check_result = False
        check_msg = "Value is missing or null"
    return check_result, check_msg # this will be the exists-and-not-null function that required fields all apply

@error_handler
def format_compliance(schema_rule, test_value):
    check_result = True
    check_msg = ""
    if not re.match(schema_rule, str(test_value)):
        check_result = False
        check_msg = f"{test_value} does not adhere to format {schema_rule}"
    return check_result, check_msg # this will be the regex format compliance test

@error_handler
def permitted_value(schema_rule, test_value):
    check_result = True
    check_msg = ""
    if schema_rule.get("accepted_value") is not None and test_value not in schema_rule["accepted_value"]:
        check_result = False
        check_msg = f"{test_value} is not an accepted value"
    if schema_rule.get("forbidden_value") is not None and test_value in schema_rule["forbidden_value"]:
        check_result = False
        check_msg = f"{test_value} is a forbidden value"
    return check_result, check_msg # this will be the accepted values and forbidden values tests

@error_handler
def valid_datatype(schema_rule, test_value):
    check_result = True
    check_msg = ""
    if type(test_value) != schema_rule:
        check_result = False
        check_msg = f"{test_value} is an invalid data type"
    return check_result, check_msg # this will be the data type validation test

@error_handler
def limit_value(schema_rule, test_value):
    check_result = True
    check_msg = ""
    if schema_rule.get("min") is not None and test_value < schema_rule["min"]:
        check_result = False
        check_msg = f"{test_value} below minimum tolerance {schema_rule['min']}"
    if schema_rule.get("max") is not None and test_value < schema_rule["max"]:
        check_result = False
        check_msg = f"{test_value} below minimum tolerance {schema_rule['max']}"
    return check_result, check_msg # this will be the min/max value test

dispatch_table = {
    "required": exist_notnull,
    "format": format_compliance,
    "permitted_value": permitted_value,
    "data_type": valid_datatype,
    "limit": limit_value
}

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
            rejection_reason = ""
            for col_name, col_rules in schema_definitions.items():
                test_value = row[col_name]
                rules = col_rules.get("rules",{})
                for rule_name, schema_rule in rules.items():
                    validate_func = dispatch_table.get(rule_name)
                    if not validate_func:
                        log_event(runtime_config, f"'{rule_name}' validation function not found for column '{col_name}'", "ERROR")
                        continue
                    check_result, check_msg = validate_func(schema_rule, test_value)
                    if not check_result:
                        log_event(runtime_config, f"row {row['source_index']} {col_name}: {check_msg}\n", "INGEST")
                        if group_reject:
                            group_rejected = True
                        if cascade_reject:
                            break
        if not group_rejected:
            valid_data.append(group)
        else:
            rejected_data.update({primary_key_value: rejection_reason})
    
    log_event(runtime_config, "Validation module called (no rules applied)","EVENT")
    return pd.concat(valid_data) if valid_data else pd.DataFrame()