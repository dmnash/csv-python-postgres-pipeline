# validation_library.py

import pandas as pd
import re

# validation functions return "GREEN", "YELLOW", or "RED", and a logging message
# "GREEN" results pass validation and will not generate a log entry
# "YELLOW" results pass validation but will generate a log entry
# "RED" results fail validation and will generate a log entry

def function_wrangler(rule_func):
    def wrapper(schema_rule, test_value):
        try:
            return rule_func(schema_rule, test_value)
        except Exception as e:
            return "RED", f"{rule_func.__name__} error: {str(e)}"
    return wrapper

@function_wrangler
def exist_notnull(schema_rule, test_value):
# this will be the exists-and-not-null function that required fields all apply
    if not schema_rule:
        return "GREEN", "value exists and is not null"
    if test_value is None or pd.isnull(test_value):
        return "RED", "value is missing or null"
    return "GREEN", "value exists and is not null"

@function_wrangler
def format_compliance(schema_rule, test_value):
# this will be the regex format compliance test
    if not re.match(schema_rule, str(test_value)):
        return "RED", f"{test_value} does not comply with format {schema_rule}"
    return "GREEN", f"{test_value} complies with format {schema_rule}"

@function_wrangler
def permitted_value(schema_rule, test_value):
# this will be the accepted values and forbidden values tests
    if schema_rule.get("accepted_value") is not None and test_value in schema_rule["accepted_value"]:
        return "GREEN", f"{test_value} is a permitted value"
    if schema_rule.get("forbidden_value") is not None and test_value in schema_rule["forbidden_value"]:
        return "RED", f"{test_value} is a forbidden value"
    if schema_rule.get("accepted_value") is not None:
        return "RED", f"{test_value} is not an accepted value"
    return "GREEN", f"{test_value} is a permitted value"

@function_wrangler
def valid_datatype(schema_rule, test_value):
# this will be the data type validation test
    expected_type = schema_rule.lower()
    type_map = {"integer": int, "float": float, "string": str}
    if expected_type not in type_map:
        return "RED", f"Unsupported data type: {expected_type}"
    if isinstance(test_value, type_map[expected_type]):
        return "GREEN", f"{test_value} is valid {expected_type}"
    try:
        cast_value = type_map[expected_type](test_value)
        return "YELLOW", f"{test_value} cast from {type(test_value).__name__.lower()} to {expected_type}"
    except Exception as e:
        return "RED", f"{test_value} is not castable to {expected_type} ({type(test_value).__name__.lower()})"

@function_wrangler
def limit_value(schema_rule, test_value):
# this will be the min/max value test
    if schema_rule.get("min") is not None:
        try:
            casted_value = type(schema_rule["min"])(test_value)
            if casted_value < schema_rule["min"]:
                return "RED", f"{test_value} below minimum tolerance {schema_rule['min']}"
        except Exception as e:
            return "RED", f"{test_value} cannot be evaluated against MIN rule"
    if schema_rule.get("max") is not None:
        try:
            casted_value = type(schema_rule["max"])(test_value)
            if casted_value > schema_rule["max"]:
                return "RED", f"{test_value} exceeds maximum tolerance {schema_rule['max']}"
        except Exception as e:
            return "RED", f"{test_value} cannot be evaluated against MAX rule"
    return "GREEN", f"{test_value} is within accepted tolerance"

dispatch_table = {
    "required": exist_notnull,
    "format": format_compliance,
    "permitted_value": permitted_value,
    "data_type": valid_datatype,
    "limit": limit_value
}

def schema_validator(schema: dict) -> tuple[bool, str]:
    schema_valid = True
    validation_msg = ""
    return schema_valid, validation_msg